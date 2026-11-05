//! Synthetic Sysmon-style process_creation dataset.
//! Writes N files of ROWS_PER_FILE rows as Parquet (engine defaults from opensearch-3.7.0
//! ParquetSettings: LZ4_RAW, 1 MiB / 20k-row pages, 2 MiB dictionaries, 1M-row row groups)
//! and the same rows as TSV for the Java legs. Positives are injected by splicing values
//! taken from the compiled rule set (rules.json) so matches spread across many rules.
use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

const COLS: &[&str] = &[
    "_id", "ts", "Computer", "User", "Image", "CommandLine", "ParentImage", "ParentCommandLine",
    "OriginalFileName", "IntegrityLevel", "Hashes", "Imphash", "Description", "Product", "Company",
    "CurrentDirectory", "ParentUser", "LogonId", "EventID",
];

const IMAGES: &[&str] = &[
    "C:\\Windows\\System32\\svchost.exe", "C:\\Windows\\System32\\conhost.exe", "C:\\Windows\\explorer.exe",
    "C:\\Windows\\System32\\cmd.exe", "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe", "C:\\Program Files\\Microsoft Office\\root\\Office16\\OUTLOOK.EXE",
    "C:\\Program Files\\Microsoft Office\\root\\Office16\\WINWORD.EXE", "C:\\Windows\\System32\\taskhostw.exe",
    "C:\\Windows\\System32\\RuntimeBroker.exe", "C:\\Windows\\System32\\dllhost.exe", "C:\\Windows\\System32\\wbem\\WmiPrvSE.exe",
    "C:\\Windows\\System32\\SearchProtocolHost.exe", "C:\\Windows\\System32\\backgroundTaskHost.exe",
    "C:\\Program Files\\Microsoft VS Code\\Code.exe", "C:\\Program Files\\Git\\cmd\\git.exe",
    "C:\\Windows\\System32\\msiexec.exe", "C:\\Windows\\System32\\sc.exe", "C:\\Windows\\System32\\net.exe",
    "C:\\Windows\\System32\\reg.exe", "C:\\Windows\\System32\\schtasks.exe", "C:\\Windows\\System32\\whoami.exe",
    "C:\\Windows\\System32\\ipconfig.exe", "C:\\Windows\\System32\\tasklist.exe", "C:\\Windows\\System32\\findstr.exe",
    "C:\\Windows\\System32\\wscript.exe", "C:\\Windows\\System32\\cscript.exe", "C:\\Windows\\System32\\rundll32.exe",
    "C:\\Windows\\System32\\regsvr32.exe", "C:\\Windows\\System32\\mshta.exe", "C:\\Windows\\System32\\certutil.exe",
    "C:\\Windows\\System32\\bitsadmin.exe", "C:\\Windows\\System32\\wmic.exe", "C:\\Windows\\System32\\notepad.exe",
    "C:\\Program Files\\7-Zip\\7zFM.exe", "C:\\Program Files\\7-Zip\\7z.exe", "C:\\Windows\\System32\\SecurityHealthSystray.exe",
    "C:\\Users\\jdoe\\AppData\\Local\\Microsoft\\Teams\\current\\Teams.exe", "C:\\Users\\jdoe\\AppData\\Local\\slack\\slack.exe",
    "C:\\Program Files\\Mozilla Firefox\\firefox.exe",
];
const ARGS: &[&str] = &[
    "", "-k netsvcs -p", "-k LocalServiceNetworkRestricted", "/c dir", "/c echo hello", "-NoProfile -Command Get-Date",
    "--type=renderer --field-trial-handle=1234", "/Embedding", "-Embedding", "/s", "/q /i package.msi",
    "query state=all", "user", "view \\\\fileserver\\share", "query HKLM\\Software\\Microsoft", "/query /fo LIST",
    "/all", "/svc", "/i \"error\" logfile.txt", "status", "pull origin main", "--log-level=1", "-s -t 30",
    "export HKCU\\Environment env.reg", "/c copy a.txt b.txt", "-ExecutionPolicy Bypass -File deploy.ps1",
    "commit -m \"fix build\"", "add .", "stop spooler", "start wuauserv", "/c ping -n 1 10.0.0.1",
];
const USERS: &[&str] = &["CORP\\jdoe", "CORP\\asmith", "CORP\\svc_backup", "NT AUTHORITY\\SYSTEM", "NT AUTHORITY\\LOCAL SERVICE", "NT AUTHORITY\\NETWORK SERVICE", "CORP\\mkhan", "CORP\\build"];
const LEVELS: &[&str] = &["Medium", "High", "System", "Low"];
const COMPANIES: &[&str] = &["Microsoft Corporation", "Google LLC", "Igor Pavlov", "Microsoft Corporation", "Mozilla Corporation", "Slack Technologies, Inc."];

#[derive(serde::Deserialize)]
struct Rules { rules: Vec<Rule> }
#[derive(serde::Deserialize)]
struct Rule { expr: serde_json::Value }

/// Collect (field, op, value) leaves usable as injection material.
fn leaves(v: &serde_json::Value, out: &mut Vec<(String, String, String)>) {
    if let Some(op) = v.get("op").and_then(|o| o.as_str()) {
        match op {
            "and" | "or" => { for a in v["args"].as_array().unwrap() { leaves(a, out); } }
            "not" => {}
            "contains" | "endswith" | "startswith" | "eq" => {
                out.push((v["field"].as_str().unwrap().to_string(), op.to_string(), v["value"].as_str().unwrap().to_string()));
            }
            _ => {}
        }
    }
}

fn mixed_case(rng: &mut impl Rng, s: &str) -> String {
    // Real logs are mixed case; vary the case of injected tokens so matching must be case-insensitive.
    match rng.gen_range(0..3) { 0 => s.to_string(), 1 => s.to_uppercase(), _ => { let mut c = s.chars(); match c.next() { Some(f) => f.to_uppercase().collect::<String>() + c.as_str(), None => String::new() } } }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rules_path = &args[1];
    let out_dir = &args[2];
    let n_files: usize = args[3].parse().unwrap();
    let rows_per_file: usize = args[4].parse().unwrap();
    let inject_rate: f64 = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(0.03);
    let write_tsv = args.get(6).map(|s| s == "tsv").unwrap_or(true);

    let rules: Rules = serde_json::from_reader(File::open(rules_path).unwrap()).unwrap();
    let mut lv = Vec::new();
    for r in &rules.rules { leaves(&r.expr, &mut lv); }
    let cmd_tokens: Vec<&str> = lv.iter().filter(|(f, op, _)| f == "CommandLine" && op == "contains").map(|(_, _, v)| v.as_str()).collect();
    let img_ends: Vec<&str> = lv.iter().filter(|(f, op, _)| f == "Image" && op == "endswith").map(|(_, _, v)| v.as_str()).collect();
    let pimg_ends: Vec<&str> = lv.iter().filter(|(f, op, _)| f == "ParentImage" && op == "endswith").map(|(_, _, v)| v.as_str()).collect();
    let ofn: Vec<&str> = lv.iter().filter(|(f, op, _)| f == "OriginalFileName" && op == "eq").map(|(_, _, v)| v.as_str()).collect();
    eprintln!("injection pools: cmd={} img={} pimg={} ofn={}", cmd_tokens.len(), img_ends.len(), pimg_ends.len(), ofn.len());

    std::fs::create_dir_all(out_dir).unwrap();
    let schema = Arc::new(Schema::new(COLS.iter().map(|c| match *c {
        "ts" => Field::new(*c, DataType::Int64, false),
        "EventID" => Field::new(*c, DataType::Int32, false),
        "_id" => Field::new(*c, DataType::Utf8, false),
        _ => Field::new(*c, DataType::Utf8, true),
    }).collect::<Vec<_>>()));
    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(2 * 1024 * 1024)
        .set_data_page_size_limit(1024 * 1024)
        .set_data_page_row_count_limit(20_000)
        .set_max_row_group_size(1_000_000)
        .build();

    let mut rng = rand_pcg::Pcg64::seed_from_u64(42);
    let mut ts: i64 = 1_762_000_000_000;
    let mut injected = 0usize;
    for f in 0..n_files {
        let pq = File::create(format!("{out_dir}/part-{f:04}.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(pq, schema.clone(), Some(props.clone())).unwrap();
        let mut tsv = if write_tsv { Some(BufWriter::with_capacity(1 << 20, File::create(format!("{out_dir}/part-{f:04}.tsv")).unwrap())) } else { None };
        let batch_rows = 100_000;
        let mut done = 0;
        while done < rows_per_file {
            let n = batch_rows.min(rows_per_file - done);
            let mut cols: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(n); COLS.len()];
            let mut tsv_buf = String::with_capacity(n * 400);
            for i in 0..n {
                ts += rng.gen_range(1..40);
                let row_id = format!("{:08x}{:08x}", f * rows_per_file + done + i, rng.gen::<u32>());
                let img_i = rng.gen_range(0..IMAGES.len());
                let mut image = IMAGES[img_i].to_string();
                let pimg_i = rng.gen_range(0..IMAGES.len());
                let mut pimage = IMAGES[pimg_i].to_string();
                let mut cmd = format!("\"{}\" {}", image, ARGS[rng.gen_range(0..ARGS.len())]);
                let mut orig = image.rsplit('\\').next().unwrap().to_string();
                if rng.gen_bool(inject_rate) {
                    injected += 1;
                    match rng.gen_range(0..4) {
                        0 => { let t = cmd_tokens[rng.gen_range(0..cmd_tokens.len())]; let mc = mixed_case(&mut rng, t); cmd = format!("{} {} {}", cmd, mc, ARGS[rng.gen_range(0..ARGS.len())]); }
                        1 => { let e = img_ends[rng.gen_range(0..img_ends.len())]; image = format!("C:\\Windows\\System32{}", mixed_case(&mut rng, e)); cmd = format!("\"{}\" {}", image, ARGS[rng.gen_range(0..ARGS.len())]); orig = image.rsplit('\\').next().unwrap().to_string(); }
                        2 => { let e = pimg_ends[rng.gen_range(0..pimg_ends.len())]; pimage = format!("C:\\Users\\jdoe\\AppData\\Local\\Temp{}", mixed_case(&mut rng, e)); let t = cmd_tokens[rng.gen_range(0..cmd_tokens.len())]; cmd = format!("{} {}", cmd, mixed_case(&mut rng, t)); }
                        _ => { if !ofn.is_empty() { let o = ofn[rng.gen_range(0..ofn.len())]; orig = mixed_case(&mut rng, o); } let t = cmd_tokens[rng.gen_range(0..cmd_tokens.len())]; cmd = format!("{} {}", cmd, mixed_case(&mut rng, t)); }
                    }
                }
                let user = USERS[rng.gen_range(0..USERS.len())];
                let vals: Vec<Option<String>> = vec![
                    Some(row_id), Some(ts.to_string()), Some(format!("WS-{:04}.corp.local", rng.gen_range(0..3000))), Some(user.to_string()),
                    Some(image), Some(cmd), Some(pimage), Some(format!("\"{}\" {}", IMAGES[pimg_i], ARGS[rng.gen_range(0..ARGS.len())])),
                    Some(orig), Some(LEVELS[rng.gen_range(0..LEVELS.len())].to_string()),
                    Some(format!("SHA256={:064x}", rng.gen::<u128>() as u128 * 7 + rng.gen::<u128>())), Some(format!("{:032x}", rng.gen::<u128>())),
                    if rng.gen_bool(0.1) { None } else { Some("Host Process for Windows Services".to_string()) },
                    Some("Microsoft Windows Operating System".to_string()), Some(COMPANIES[rng.gen_range(0..COMPANIES.len())].to_string()),
                    Some(if rng.gen_bool(0.7) { "C:\\Windows\\system32\\".to_string() } else { format!("C:\\Users\\{}\\", user.rsplit('\\').next().unwrap()) }),
                    Some(user.to_string()), Some(format!("0x{:x}", rng.gen_range(0x3e7..0x9ffff))), Some("1".to_string()),
                ];
                if let Some(_) = tsv {
                    for (k, v) in vals.iter().enumerate() {
                        if k > 0 { tsv_buf.push('\t'); }
                        if let Some(s) = v { tsv_buf.push_str(&s.replace(['\t', '\n'], " ")); } else { tsv_buf.push_str("\\N"); }
                    }
                    tsv_buf.push('\n');
                }
                for (k, v) in vals.into_iter().enumerate() { cols[k].push(v); }
            }
            let arrays: Vec<ArrayRef> = cols.iter().enumerate().map(|(k, c)| -> ArrayRef {
                match COLS[k] {
                    "ts" => Arc::new(Int64Array::from(c.iter().map(|v| v.as_ref().unwrap().parse::<i64>().unwrap()).collect::<Vec<_>>())),
                    "EventID" => Arc::new(Int32Array::from(c.iter().map(|v| v.as_ref().unwrap().parse::<i32>().unwrap()).collect::<Vec<_>>())),
                    _ => Arc::new(StringArray::from(c.iter().map(|v| v.as_deref()).collect::<Vec<_>>())),
                }
            }).collect();
            w.write(&RecordBatch::try_new(schema.clone(), arrays).unwrap()).unwrap();
            if let Some(t) = tsv.as_mut() { t.write_all(tsv_buf.as_bytes()).unwrap(); }
            done += n;
        }
        w.close().unwrap();
        if let Some(mut t) = tsv.take() { t.flush().unwrap(); }
        eprintln!("file {f} done ({rows_per_file} rows)");
    }
    eprintln!("rows={} injected={} ({:.2}%)", n_files * rows_per_file, injected, 100.0 * injected as f64 / (n_files * rows_per_file) as f64);
}
