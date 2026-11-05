//! Ingest-time leg a4: Rust, one multi-pattern pass per field (aho-corasick, memmem, linear regex),
//! then rule trees evaluated from leaf bits for candidate rules only.
//! Usage: rsmatch <data_dir> <rules.json> <N> [per_rule_out]
use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind, StartKind, Anchored};
use aho_corasick::automaton::Automaton;
use aho_corasick::dfa::DFA;
use arrow::array::{Array, StringArray};
use memchr::memmem;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::Regex;
use std::collections::HashMap;
use std::time::Instant;


// ---------------- Vectorscan FFI (feature "vs") ----------------
#[cfg(feature = "vs")]
mod vs {
    use std::ffi::{c_char, c_int, c_uint, c_ulonglong, c_void, CString};
    #[repr(C)] pub struct HsDb { _p: [u8; 0] }
    #[repr(C)] pub struct HsScratch { _p: [u8; 0] }
    #[repr(C)] pub struct HsCompileError { pub message: *mut c_char, pub expression: c_int }
    pub type Handler = Option<unsafe extern "C" fn(id: c_uint, from: c_ulonglong, to: c_ulonglong, flags: c_uint, ctx: *mut c_void) -> c_int>;
    extern "C" {
        pub fn hs_compile_multi(exprs: *const *const c_char, flags: *const c_uint, ids: *const c_uint, n: c_uint, mode: c_uint, platform: *const c_void, db: *mut *mut HsDb, err: *mut *mut HsCompileError) -> c_int;
        pub fn hs_alloc_scratch(db: *const HsDb, scratch: *mut *mut HsScratch) -> c_int;
        pub fn hs_scan(db: *const HsDb, data: *const c_char, len: c_uint, flags: c_uint, scratch: *mut HsScratch, on: Handler, ctx: *mut c_void) -> c_int;
        pub fn hs_free_compile_error(e: *mut HsCompileError) -> c_int;
        pub fn hs_version() -> *const c_char;
    }
    pub const HS_MODE_BLOCK: c_uint = 1; pub const HS_FLAG_CASELESS: c_uint = 1; pub const HS_FLAG_DOTALL: c_uint = 2; pub const HS_FLAG_SINGLEMATCH: c_uint = 8;
    pub struct Ctx { pub bits: *mut u32, pub gen: u32, pub hits: *mut Vec<usize> }
    pub unsafe extern "C" fn on_match(id: c_uint, _f: c_ulonglong, _t: c_ulonglong, _fl: c_uint, ctx: *mut c_void) -> c_int {
        let c = &mut *(ctx as *mut Ctx); let b = c.bits.add(id as usize);
        if *b != c.gen { *b = c.gen; (*c.hits).push(id as usize); }
        0
    }
    pub struct BatchCtx { pub offsets: *const i32, pub n_rows: usize, pub out: *mut Vec<(u32, u32)>, pub pat_len: *const u32 }
    /// batch-mode callback: map match end offset to a row via binary search on the Arrow offsets; drop matches that start in an earlier row
    pub unsafe extern "C" fn on_match_batch(id: c_uint, _f: c_ulonglong, to: c_ulonglong, _fl: c_uint, ctx: *mut c_void) -> c_int {
        let c = &*(ctx as *mut BatchCtx);
        let offs = std::slice::from_raw_parts(c.offsets, c.n_rows + 1);
        let to = to as i32;
        // row r such that offs[r] < to <= offs[r+1]
        let r = offs.partition_point(|&o| o < to) - 1;
        let from = to - *c.pat_len.add(id as usize) as i32;
        if from >= offs[r] { (*c.out).push((r as u32, id)); }
        0
    }
    pub fn escape(lit: &str) -> String { let mut o = String::with_capacity(lit.len() * 2); for ch in lit.chars() { if "\\^$.|?*+()[]{}/-#".contains(ch) { o.push('\\'); } o.push(ch); } o }
    pub fn glob_to_re(g: &str) -> String { let mut o = String::new(); if !g.starts_with('*') { o.push('^'); } for ch in g.chars() { match ch { '*' => o.push_str(".*"), '?' => o.push('.'), c => o.push_str(&escape(&c.to_string())) } } if !g.ends_with('*') { o.push('$'); } o }
    /// Compile one database; returns (db, scratch, ids that failed to compile)
    pub fn compile(exprs: &[(String, c_uint, u32)]) -> (*mut HsDb, *mut HsScratch, Vec<u32>) {
        let mut failed = Vec::new(); let mut cur: Vec<(String, c_uint, u32)> = exprs.to_vec();
        loop {
            let cs: Vec<CString> = cur.iter().map(|(e, _, _)| CString::new(e.as_str()).unwrap()).collect();
            let ptrs: Vec<*const c_char> = cs.iter().map(|c| c.as_ptr()).collect();
            let flags: Vec<c_uint> = cur.iter().map(|(_, f, _)| *f).collect(); let ids: Vec<c_uint> = cur.iter().map(|(_, _, i)| *i).collect();
            let mut db: *mut HsDb = std::ptr::null_mut(); let mut err: *mut HsCompileError = std::ptr::null_mut();
            let rc = unsafe { hs_compile_multi(ptrs.as_ptr(), flags.as_ptr(), ids.as_ptr(), cur.len() as c_uint, HS_MODE_BLOCK, std::ptr::null(), &mut db, &mut err) };
            if rc == 0 { let mut sc: *mut HsScratch = std::ptr::null_mut(); assert_eq!(unsafe { hs_alloc_scratch(db, &mut sc) }, 0); return (db, sc, failed); }
            let bad = unsafe { (*err).expression }; unsafe { hs_free_compile_error(err) };
            if bad < 0 || cur.is_empty() { panic!("hs_compile_multi failed rc={rc}"); }
            failed.push(cur.remove(bad as usize).2);
            if cur.is_empty() { return (std::ptr::null_mut(), std::ptr::null_mut(), failed); }
        }
    }
}

const DATASET_COLS: &[&str] = &[
    "Computer", "User", "Image", "CommandLine", "ParentImage", "ParentCommandLine", "OriginalFileName",
    "IntegrityLevel", "Hashes", "Imphash", "Description", "Product", "Company", "CurrentDirectory", "ParentUser", "LogonId",
];

#[derive(Clone, Copy, PartialEq)]
enum Op { Eq, Contains, Starts, Ends, Glob, Re, IsNull, False }

struct Leaf { field: usize, op: Op, value: String }

enum Node { And(Vec<Node>), Or(Vec<Node>), Not(Box<Node>), Leaf(usize) }

struct FieldMatcher {
    contains_ac: Option<AhoCorasick>, contains_ids: Vec<usize>,
    contains_mem: Vec<(memmem::Finder<'static>, usize)>,
    starts_ac: Option<DFA>, starts_ids: Vec<usize>,
    ends_ac: Option<DFA>, ends_ids: Vec<usize>,
    eq: ahash::AHashMap<String, Vec<usize>>,
    globs: Vec<(Vec<memmem::Finder<'static>>, bool, bool, Vec<u8>, bool, usize, usize)>, // segment finders, anchored_start, anchored_end, raw pattern, has_qmark, leaf, prefilter bit
    res: Vec<(Regex, usize)>,
    isnull: Vec<usize>, always: Vec<usize>,
    used: bool,
}

fn glob_match(pat: &[u8], s: &[u8]) -> bool {
    // classic iterative wildcard match with * and ?
    let (mut p, mut i, mut star, mut mark) = (0usize, 0usize, usize::MAX, 0usize);
    while i < s.len() {
        if p < pat.len() && (pat[p] == b'?' || pat[p] == s[i]) { p += 1; i += 1; }
        else if p < pat.len() && pat[p] == b'*' { star = p; mark = i; p += 1; }
        else if star != usize::MAX { p = star + 1; mark += 1; i = mark; }
        else { return false; }
    }
    while p < pat.len() && pat[p] == b'*' { p += 1; }
    p == pat.len()
}

fn seg_match(segs: &[memmem::Finder<'static>], a_start: bool, a_end: bool, s: &[u8]) -> bool {
    let mut pos = 0usize;
    for (i, f) in segs.iter().enumerate() {
        let n = f.needle();
        if i == 0 && a_start { if !s.starts_with(n) { return false; } pos = n.len(); continue; }
        if i == segs.len() - 1 && a_end { return s.len() >= pos + n.len() && s.ends_with(n); }
        match f.find(&s[pos..]) { Some(k) => pos += k + n.len(), None => return false }
    }
    true
}

fn build_tree(v: &serde_json::Value, leaves: &mut Vec<Leaf>, ids: &mut HashMap<String, usize>, fields: &HashMap<String, usize>) -> Node {
    let op = v["op"].as_str().unwrap();
    match op {
        "and" => Node::And(v["args"].as_array().unwrap().iter().map(|a| build_tree(a, leaves, ids, fields)).collect()),
        "or" => Node::Or(v["args"].as_array().unwrap().iter().map(|a| build_tree(a, leaves, ids, fields)).collect()),
        "not" => Node::Not(Box::new(build_tree(&v["arg"], leaves, ids, fields))),
        "in" => Node::Or(v["values"].as_array().unwrap().iter().map(|x| {
            let l = serde_json::json!({"op":"eq","field":v["field"],"value":x}); build_tree(&l, leaves, ids, fields)
        }).collect()),
        _ => {
            let field = v["field"].as_str().unwrap_or("");
            let value = v["value"].as_str().unwrap_or("").to_string();
            let key = format!("{op}|{field}|{value}");
            let fi = *fields.get(field).unwrap_or(&usize::MAX);
            let id = *ids.entry(key).or_insert_with(|| {
                leaves.push(Leaf { field: fi, value: value.clone(), op: match op { "eq" => Op::Eq, "contains" => Op::Contains, "startswith" => Op::Starts, "endswith" => Op::Ends, "glob" => Op::Glob, "re" => Op::Re, "isnull" => Op::IsNull, _ => Op::False } });
                leaves.len() - 1
            });
            Node::Leaf(id)
        }
    }
}

fn eval(n: &Node, bits: &[u32], gen: u32) -> bool {
    match n {
        Node::And(a) => a.iter().all(|x| eval(x, bits, gen)),
        Node::Or(a) => a.iter().any(|x| eval(x, bits, gen)),
        Node::Not(a) => !eval(a, bits, gen),
        Node::Leaf(i) => bits[*i] == gen,
    }
}
fn positive_leaves(n: &Node, out: &mut Vec<usize>) {
    match n { Node::And(a) | Node::Or(a) => a.iter().for_each(|x| positive_leaves(x, out)), Node::Not(_) => {}, Node::Leaf(i) => out.push(*i) }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (data, rules_json, n) = (&a[1], &a[2], a[3].parse::<usize>().unwrap());
    let rj: serde_json::Value = serde_json::from_reader(std::fs::File::open(rules_json).unwrap()).unwrap();
    let fields: HashMap<String, usize> = DATASET_COLS.iter().enumerate().map(|(i, c)| (c.to_string(), i)).collect();
    let mut leaves = Vec::new(); let mut ids = HashMap::new();
    let rules: Vec<Node> = rj["rules"].as_array().unwrap().iter().take(n).map(|r| build_tree(&r["expr"], &mut leaves, &mut ids, &fields)).collect();
    let n = rules.len();

    // leaf -> rules index for candidate generation; rules true with all leaves false always evaluated
    let mut leaf_rules: Vec<Vec<u32>> = vec![Vec::new(); leaves.len()];
    let mut always: Vec<u32> = Vec::new();
    let zero = vec![0u32; leaves.len()];
    for (ri, r) in rules.iter().enumerate() {
        if eval(r, &zero, 1) { always.push(ri as u32); continue; }
        let mut pl = Vec::new(); positive_leaves(r, &mut pl); pl.sort(); pl.dedup();
        for l in pl { leaf_rules[l].push(ri as u32); }
    }

    // per-field matchers
    let nf = DATASET_COLS.len() + 1; // last slot = absent fields
    let mut fm: Vec<FieldMatcher> = (0..nf).map(|_| FieldMatcher { contains_ac: None, contains_ids: vec![], contains_mem: vec![], starts_ac: None, starts_ids: vec![], ends_ac: None, ends_ids: vec![], eq: ahash::AHashMap::new(), globs: vec![], res: vec![], isnull: vec![], always: vec![], used: false }).collect();
    let mut n_bits = leaves.len();
    let mut contains_pats: Vec<Vec<String>> = vec![vec![]; nf]; let mut starts_pats = contains_pats.clone(); let mut ends_pats: Vec<Vec<Vec<u8>>> = vec![vec![]; nf];
    for (li, l) in leaves.iter().enumerate() {
        let f = if l.field == usize::MAX { nf - 1 } else { l.field };
        fm[f].used = true;
        match l.op {
            Op::Contains => { if l.value.is_empty() { fm[f].always.push(li) } else { contains_pats[f].push(l.value.clone()); fm[f].contains_ids.push(li) } }
            Op::Starts => { starts_pats[f].push(l.value.clone()); fm[f].starts_ids.push(li) }
            Op::Ends => { ends_pats[f].push(l.value.bytes().rev().collect()); fm[f].ends_ids.push(li) }
            Op::Eq => fm[f].eq.entry(l.value.clone()).or_default().push(li),
            Op::Glob => {
                let v = l.value.as_bytes(); let segs: Vec<memmem::Finder<'static>> = l.value.split('*').filter(|x| !x.is_empty()).map(|x| memmem::Finder::new(x.as_bytes()).into_owned()).collect();
                let longest = l.value.split('*').filter(|x| !x.contains('?')).max_by_key(|x| x.len()).unwrap_or("");
                let pre = if longest.len() >= 3 { let bit = n_bits; n_bits += 1; contains_pats[f].push(longest.to_string()); fm[f].contains_ids.push(bit); bit } else { usize::MAX };
                fm[f].globs.push((segs, !v.starts_with(b"*"), !v.ends_with(b"*"), v.to_vec(), l.value.contains('?'), li, pre)) }
            Op::Re => fm[f].res.push((Regex::new(&format!("(?i){}", l.value)).unwrap_or_else(|_| Regex::new("a^").unwrap()), li)),
            Op::IsNull => fm[f].isnull.push(li),
            Op::False => {}
        }
    }
    let mut ac_pats = 0; let mut mem_pats = 0;
    let kind = match std::env::var("AC_KIND").as_deref() { Ok("dfa") => Some(aho_corasick::AhoCorasickKind::DFA), Ok("nfa") => Some(aho_corasick::AhoCorasickKind::ContiguousNFA), _ => None };
    for f in 0..nf {
        if contains_pats[f].len() > 4 {
            fm[f].contains_ac = Some(AhoCorasickBuilder::new().kind(kind).ascii_case_insensitive(true).match_kind(MatchKind::Standard).build(&contains_pats[f]).unwrap()); ac_pats += contains_pats[f].len();
        } else {
            let ids_copy = fm[f].contains_ids.clone();
            for (k, p) in contains_pats[f].iter().enumerate() { fm[f].contains_mem.push((memmem::Finder::new(p.as_bytes()).into_owned(), ids_copy[k])); mem_pats += 1; }
        }
        if !starts_pats[f].is_empty() { fm[f].starts_ac = Some(DFA::builder().start_kind(StartKind::Anchored).ascii_case_insensitive(true).match_kind(MatchKind::Standard).build(&starts_pats[f]).unwrap()); }
        if !ends_pats[f].is_empty() { fm[f].ends_ac = Some(DFA::builder().start_kind(StartKind::Anchored).ascii_case_insensitive(true).match_kind(MatchKind::Standard).build(&ends_pats[f]).unwrap()); }
    }
    eprintln!("rules={} leaves={} ac_patterns={} memmem_patterns={} always_rules={}", n, leaves.len(), ac_pats, mem_pats, always.len());
    let use_vs = std::env::var("VS").is_ok();
    #[cfg(feature = "vs")]
    let mut vsdb: Vec<Option<(*mut vs::HsDb, *mut vs::HsScratch)>> = vec![None; nf];
    #[cfg(feature = "vs")]
    let mut vs_fallback_re: Vec<Vec<(Regex, usize)>> = vec![vec![]; nf];
    let vs_batch = std::env::var("VS").map(|v| v == "batch").unwrap_or(false);
    #[cfg(feature = "vs")]
    let mut vsbatch_db: Vec<Option<(*mut vs::HsDb, *mut vs::HsScratch)>> = vec![None; nf];
    #[cfg(feature = "vs")]
    let vs_pat_len: Vec<u32> = leaves.iter().map(|l| l.value.len() as u32).collect();
    #[cfg(feature = "vs")]
    if vs_batch {
        let t = Instant::now(); let mut total = 0;
        for f in 0..nf {
            let mut ex: Vec<(String, u32, u32)> = Vec::new();
            for (li, l) in leaves.iter().enumerate() {
                let lf = if l.field == usize::MAX { nf - 1 } else { l.field }; if lf != f || l.op != Op::Contains || l.value.is_empty() { continue; }
                ex.push((vs::escape(&l.value), vs::HS_FLAG_CASELESS, li as u32));
            }
            if ex.is_empty() { continue; }
            total += ex.len();
            let (db, sc, failed) = vs::compile(&ex); assert!(failed.is_empty());
            if !db.is_null() { vsbatch_db[f] = Some((db, sc)); }
        }
        eprintln!("vectorscan batch mode: {} contains literals in {} databases ({:.1}s); other leaf kinds stay on the Rust matchers", total, vsbatch_db.iter().filter(|d| d.is_some()).count(), t.elapsed().as_secs_f64());
    }
    #[cfg(feature = "vs")]
    if use_vs && !vs_batch {
        eprintln!("vectorscan {}", unsafe { std::ffi::CStr::from_ptr(vs::hs_version()) }.to_str().unwrap());
        let t = Instant::now(); let mut total = 0; let mut nfail = 0;
        for f in 0..nf {
            let mut ex: Vec<(String, u32, u32)> = Vec::new();
            for (li, l) in leaves.iter().enumerate() {
                let lf = if l.field == usize::MAX { nf - 1 } else { l.field }; if lf != f { continue; }
                let base = vs::HS_FLAG_CASELESS | vs::HS_FLAG_SINGLEMATCH;
                match l.op {
                    Op::Contains => if !l.value.is_empty() { ex.push((vs::escape(&l.value), base, li as u32)) },
                    Op::Starts => ex.push((format!("^{}", vs::escape(&l.value)), base, li as u32)),
                    Op::Ends => ex.push((format!("{}$", vs::escape(&l.value)), base, li as u32)),
                    Op::Eq => ex.push((format!("^{}$", vs::escape(&l.value)), base, li as u32)),
                    Op::Glob => ex.push((vs::glob_to_re(&l.value), base | vs::HS_FLAG_DOTALL, li as u32)),
                    Op::Re => ex.push((l.value.clone(), base | vs::HS_FLAG_DOTALL, li as u32)),
                    _ => {}
                }
            }
            if ex.is_empty() { continue; }
            total += ex.len();
            let (db, sc, failed) = vs::compile(&ex);
            nfail += failed.len();
            for id in failed { let l = &leaves[id as usize]; if l.op == Op::Re { vs_fallback_re[f].push((Regex::new(&format!("(?i){}", l.value)).unwrap_or_else(|_| Regex::new("a^").unwrap()), id as usize)); } else { eprintln!("vectorscan rejected non-regex leaf {} {:?}", id, l.value); } }
            if !db.is_null() { vsdb[f] = Some((db, sc)); }
        }
        eprintln!("vectorscan: {} expressions compiled into {} databases in {:.1}s, {} fell back to regex crate", total, vsdb.iter().filter(|d| d.is_some()).count(), t.elapsed().as_secs_f64(), nfail);
    }
    #[cfg(not(feature = "vs"))]
    if use_vs { panic!("built without VS_LIB_DIR"); }
    for f in 0..nf { let m = &fm[f]; if m.used { eprintln!("field {} contains={} mem={} starts={} ends={} eq={} globs={} re={} isnull={} always={}", if f == nf-1 { "ABSENT" } else { DATASET_COLS[f] }, m.contains_ids.len(), m.contains_mem.len(), m.starts_ids.len(), m.ends_ids.len(), m.eq.len(), m.globs.len(), m.res.len(), m.isnull.len(), m.always.len()); } }

    // scan
    let mut files: Vec<_> = std::fs::read_dir(data).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.extension().map(|x| x == "parquet").unwrap_or(false)).collect();
    files.sort();
    let mut bits = vec![0u32; n_bits]; let mut leaf_rules = leaf_rules; leaf_rules.resize(n_bits, Vec::new()); let mut gen: u32 = 0; let mut hits: Vec<usize> = Vec::with_capacity(256);
    let mut cand_mark = vec![0u32; n]; let mut cands: Vec<u32> = Vec::with_capacity(n);
    let mut per_rule = vec![0u64; n];
    let (mut rows, mut matched_rows, mut rule_hits) = (0u64, 0u64, 0u64);
    let mut eval_ns = 0u128; let mut lower = Vec::<u8>::with_capacity(4096); let mut rev = Vec::<u8>::with_capacity(4096);
    let skip_eval = std::env::var("SKIP_EVAL").is_ok(); let skip_ac = std::env::var("SKIP_AC").is_ok(); let skip_ends = std::env::var("SKIP_ENDS").is_ok(); let skip_starts = std::env::var("SKIP_STARTS").is_ok(); let skip_glob = std::env::var("SKIP_GLOB").is_ok(); let skip_eq = std::env::var("SKIP_EQ").is_ok(); let skip_fields = std::env::var("SKIP_FIELDS").is_ok(); let skip_lower = std::env::var("SKIP_LOWER").is_ok(); let skip_mem = std::env::var("SKIP_MEM").is_ok(); let skip_value = std::env::var("SKIP_VALUE").is_ok(); let cut: u32 = std::env::var("CUT").ok().and_then(|v| v.parse().ok()).unwrap_or(99);
    let t_all = Instant::now();
    for path in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap().with_batch_size(8192).build().unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let cols: Vec<Option<&StringArray>> = DATASET_COLS.iter().map(|c| batch.column_by_name(c).map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())).collect();
            let t0 = Instant::now();
            #[cfg(feature = "vs")]
            let mut batch_hits: Vec<(u32, u32)> = Vec::new();
            #[cfg(feature = "vs")]
            let mut batch_hits_pos = 0usize;
            #[cfg(feature = "vs")]
            if vs_batch {
                for f in 0..nf - 1 {
                    let Some((db, sc)) = vsbatch_db[f] else { continue };
                    let Some(c) = cols[f] else { continue };
                    let offs = c.value_offsets(); let values = c.value_data();
                    let (lo, hi) = (offs[0] as usize, offs[offs.len() - 1] as usize);
                    let mut ctx = vs::BatchCtx { offsets: offs.as_ptr(), n_rows: c.len(), out: &mut batch_hits as *mut _, pat_len: vs_pat_len.as_ptr() };
                    // offsets are relative to the array's value buffer start; pass the full buffer so offsets line up
                    let _ = (lo, hi);
                    let rc = unsafe { vs::hs_scan(db, values.as_ptr() as *const _, values.len() as u32, 0, sc, Some(vs::on_match_batch), &mut ctx as *mut _ as *mut _) };
                    debug_assert_eq!(rc, 0);
                }
                batch_hits.sort_unstable();
            }
            for i in 0..batch.num_rows() {
                gen = gen.wrapping_add(1); if gen == 0 { bits.iter_mut().for_each(|b| *b = 0); gen = 1; }
                cands.clear(); hits.clear();
                #[cfg(feature = "vs")]
                if vs_batch { while batch_hits_pos < batch_hits.len() && batch_hits[batch_hits_pos].0 as usize == i { let __l = batch_hits[batch_hits_pos].1 as usize; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } batch_hits_pos += 1; } }
                for f in 0..nf {
                    let m = &fm[f]; if !m.used || skip_fields { continue; }
                    let val: Option<&str> = if f == nf - 1 { None } else { cols[f].and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }) };
                    if skip_value { std::hint::black_box(val); continue; }
                    let Some(v) = val else { for &l in &m.isnull { { let __l = l; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } continue; };
                    for &l in &m.always { { let __l = l; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } }
                    let vb = v.as_bytes(); if cut == 1 { std::hint::black_box(vb); continue; }
                    #[cfg(feature = "vs")]
                    if use_vs && !vs_batch {
                        if let Some((db, sc)) = vsdb[f] {
                            let mut ctx = vs::Ctx { bits: bits.as_mut_ptr(), gen, hits: &mut hits as *mut Vec<usize> };
                            let rc = unsafe { vs::hs_scan(db, vb.as_ptr() as *const _, vb.len() as u32, 0, sc, Some(vs::on_match), &mut ctx as *mut _ as *mut _) };
                            debug_assert_eq!(rc, 0);
                        }
                        for (re, l) in &vs_fallback_re[f] { if re.is_match(v) { let __l = *l; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } }
                        continue;
                    }
                    if let (false, false, Some(ac)) = (skip_ac, vs_batch && f != nf - 1 && cols[f].is_some(), &m.contains_ac) { for mt in ac.find_overlapping_iter(vb) { { let __l = m.contains_ids[mt.pattern().as_usize()]; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } }
                    if cut == 2 { continue; }
                    let need_lower = !m.contains_mem.is_empty() || !m.eq.is_empty() || !m.globs.is_empty();
                    if need_lower && !skip_lower { lower.clear(); lower.extend(vb.iter().map(|b| b.to_ascii_lowercase())); }
                    for (fd, l) in &m.contains_mem { if !skip_mem && fd.find(&lower).is_some() { { let __l = *l; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } }
                    if cut == 3 { continue; }
                    if let (false, Some(ac)) = (skip_starts, &m.starts_ac) { let mut sid = ac.start_state(Anchored::Yes).unwrap(); for (pos, &b) in vb.iter().enumerate() { sid = ac.next_state(Anchored::Yes, sid, b); if ac.is_dead(sid) { break; } if ac.is_match(sid) { for k in 0..ac.match_len(sid) { let pid = ac.match_pattern(sid, k); if ac.pattern_len(pid) != pos + 1 { continue; } let __l = m.starts_ids[pid.as_usize()]; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } } }
                    if let (false, Some(ac)) = (skip_ends, &m.ends_ac) { rev.clear(); rev.extend(vb.iter().rev()); let mut sid = ac.start_state(Anchored::Yes).unwrap(); for (pos, &b) in rev.iter().enumerate() { sid = ac.next_state(Anchored::Yes, sid, b); if ac.is_dead(sid) { break; } if ac.is_match(sid) { for k in 0..ac.match_len(sid) { let pid = ac.match_pattern(sid, k); if ac.pattern_len(pid) != pos + 1 { continue; } let __l = m.ends_ids[pid.as_usize()]; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } } }
                    if cut == 4 { continue; }
                    if !skip_eq && !m.eq.is_empty() { if let Some(ls) = m.eq.get(unsafe { std::str::from_utf8_unchecked(&lower) }) { for &l in ls { { let __l = l; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } } }
                    if !skip_glob { for (segs, a_start, a_end, raw, qm, l, pre) in &m.globs { if *pre != usize::MAX && bits[*pre] != gen { continue; } if if *qm { glob_match(raw, &lower) } else { seg_match(segs, *a_start, *a_end, &lower) } { { let __l = *l; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } } }
                    if cut == 5 { continue; }
                    for (re, l) in &m.res { if re.is_match(v) { { let __l = *l; if bits[__l] != gen { bits[__l] = gen; hits.push(__l); } } } }
                }
                // candidates = rules with any positive leaf set, plus always-rules
                for &li in &hits { for &r in &leaf_rules[li] { if cand_mark[r as usize] != gen { cand_mark[r as usize] = gen; cands.push(r); } } }
                let mut any = false;
                if !skip_eval { for &r in cands.iter().chain(always.iter()) { if eval(&rules[r as usize], &bits, gen) { any = true; rule_hits += 1; per_rule[r as usize] += 1; } } }
                if any { matched_rows += 1; }
            }
            let dt = t0.elapsed(); if std::env::var("DBG").is_ok() { eprintln!("batch rows={} eval={:?}", batch.num_rows(), dt); }
            eval_ns += dt.as_nanos(); rows += batch.num_rows() as u64;
        }
    }
    let total = t_all.elapsed();
    if let Some(out) = a.get(4) { use std::io::Write; let mut f = std::fs::File::create(out).unwrap(); for (i, c) in per_rule.iter().enumerate() { writeln!(f, "{i} {c}").unwrap(); } }
    let leg = if vs_batch { "a4vsbatch" } else if use_vs { "a4vs" } else { "a4" };
    println!("leg={leg} rules={n} leaves={} rows={rows} matched_rows={matched_rows} rule_hits={rule_hits} eval_ms={:.1} total_ms={:.1} ns_per_row={:.1} ns_per_row_rule={:.3} rows_per_s={:.0}",
        leaves.len(), eval_ns as f64 / 1e6, total.as_secs_f64() * 1e3, eval_ns as f64 / rows as f64, eval_ns as f64 / rows as f64 / n as f64, rows as f64 / (eval_ns as f64 / 1e9));
}
