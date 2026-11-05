import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Ingest-time rule evaluation legs over the TSV dataset (same rows as the Parquet files).
 *   a1     per-row tree walk, every rule independently (OpenObserve Condition.evaluate style)
 *   a1cse  per-row, identical leaves across rules deduplicated and memoised per row (predicate DAG)
 *   a2     per-batch columnar: each leaf evaluated over the whole batch into a boolean[], and/or combined
 * Rows are parsed into memory per chunk first; only evaluation is timed.
 * Usage: java Harness <data_dir> <rules.json> <N> <mode> [chunkRows]
 */
public class Harness {
    static final String[] COLS = {"_id","ts","Computer","User","Image","CommandLine","ParentImage","ParentCommandLine",
        "OriginalFileName","IntegrityLevel","Hashes","Imphash","Description","Product","Company","CurrentDirectory","ParentUser","LogonId","EventID"};
    static final Map<String,Integer> COL_IDX = new HashMap<>();
    static { for (int i = 0; i < COLS.length; i++) COL_IDX.put(COLS[i], i); }

    // ---------- predicate IR ----------
    abstract static class Node { abstract boolean eval(Row r); abstract void evalBatch(Batch b, boolean[] out); }
    static final class And extends Node {
        final Node[] a; And(Node[] a) { this.a = a; }
        boolean eval(Row r) { for (Node n : a) if (!n.eval(r)) return false; return true; }
        void evalBatch(Batch b, boolean[] out) { boolean[] t = new boolean[out.length]; a[0].evalBatch(b, out); for (int i = 1; i < a.length; i++) { a[i].evalBatch(b, t); for (int k = 0; k < out.length; k++) out[k] &= t[k]; } }
    }
    static final class Or extends Node {
        final Node[] a; Or(Node[] a) { this.a = a; }
        boolean eval(Row r) { for (Node n : a) if (n.eval(r)) return true; return false; }
        void evalBatch(Batch b, boolean[] out) { boolean[] t = new boolean[out.length]; a[0].evalBatch(b, out); for (int i = 1; i < a.length; i++) { a[i].evalBatch(b, t); for (int k = 0; k < out.length; k++) out[k] |= t[k]; } }
    }
    static final class Not extends Node {
        final Node a; Not(Node a) { this.a = a; }
        boolean eval(Row r) { return !a.eval(r); }
        void evalBatch(Batch b, boolean[] out) { a.evalBatch(b, out); for (int k = 0; k < out.length; k++) out[k] = !out[k]; }
    }
    static class Leaf extends Node {
        final int field; final int op; final String value; final Set<String> set; final Pattern re; final int id;
        static final int ISNULL=0, EQ=1, IN=2, CONTAINS=3, STARTS=4, ENDS=5, RE=6;
        static int opcode(String op) { switch (op) { case "isnull": return ISNULL; case "eq": return EQ; case "in": return IN; case "contains": return CONTAINS; case "startswith": return STARTS; case "endswith": return ENDS; default: return RE; } }
        Leaf(int id, int field, String op, String value, Set<String> set, Pattern re) { this.id = id; this.field = field; this.op = opcode(op); this.value = value; this.set = set; this.re = re; }
        boolean test(String v) {
            if (v == null) return op == ISNULL;
            switch (op) {
                case ISNULL: return false;
                case EQ: return v.equals(value);
                case IN: return set.contains(v);
                case CONTAINS: return v.contains(value);
                case STARTS: return v.startsWith(value);
                case ENDS: return v.endsWith(value);
                default: return re.matcher(v).find();
            }
        }
        boolean eval(Row r) {
            if (indexed) return r.memo[id] == 1;
            if (r.memo != null) { byte m = r.memo[id]; if (m != 0) return m == 1; boolean b = test(r.lower(field)); r.memo[id] = (byte) (b ? 1 : 2); return b; }
            return test(r.lower(field));
        }
        void evalBatch(Batch b, boolean[] out) { String[] c = b.lower(field); for (int k = 0; k < out.length; k++) out[k] = test(c[k]); }
    }

    static final class Row {
        final String[] raw; final String[] lc; byte[] memo;
        Row(String[] raw) { this.raw = raw; this.lc = new String[raw.length]; }
        String lower(int f) { if (f < 0) return null; String v = lc[f]; if (v == null) { String r = raw[f]; if (r == null) return null; v = r.toLowerCase(Locale.ROOT); lc[f] = v; } return v; }
        void reset() { Arrays.fill(lc, null); if (memo != null) Arrays.fill(memo, (byte) 0); }
    }
    static final class Batch {
        final String[][] cols; final String[][] lcCols; final int n;
        Batch(List<String[]> rows) { n = rows.size(); cols = new String[COLS.length][n]; lcCols = new String[COLS.length][]; for (int i = 0; i < n; i++) { String[] r = rows.get(i); for (int c = 0; c < COLS.length; c++) cols[c][i] = r[c]; } }
        String[] lower(int f) { if (f < 0) return new String[n]; String[] v = lcCols[f]; if (v == null) { v = new String[n]; String[] src = cols[f]; for (int i = 0; i < n; i++) v[i] = src[i] == null ? null : src[i].toLowerCase(Locale.ROOT); lcCols[f] = v; } return v; }
    }

    // ---------- IR loading ----------
    static int leafCount = 0; static final Map<String,Integer> leafIds = new HashMap<>(); static boolean cse = false; static boolean indexed = false; static final Map<Integer,Leaf> leafById = new HashMap<>();
    static Node build(Map<String,Object> n) {
        String op = (String) n.get("op");
        switch (op) {
            case "and": case "or": {
                List<?> args = (List<?>) n.get("args"); Node[] a = new Node[args.size()];
                for (int i = 0; i < a.length; i++) a[i] = build((Map<String,Object>) args.get(i));
                return op.equals("and") ? new And(a) : new Or(a);
            }
            case "not": return new Not(build((Map<String,Object>) n.get("arg")));
            case "false": return new Leaf(cse ? leafIds.computeIfAbsent("false", k -> leafCount++) : leafCount++, -1, "isnull", null, null, null) { boolean eval(Row r) { return false; } void evalBatch(Batch b, boolean[] out) { Arrays.fill(out, false); } };
            default: {
                String field = (String) n.get("field"); int fi = COL_IDX.getOrDefault(field, -1);
                String value = (String) n.get("value"); Set<String> set = null; Pattern re = null;
                String key = op + "|" + field + "|" + (op.equals("in") ? String.valueOf(n.get("values")) : value);
                if (op.equals("in")) set = new HashSet<>((List<String>) n.get("values"));
                else if (op.equals("glob")) { StringBuilder sb = new StringBuilder(); for (char ch : value.toCharArray()) { if (ch == '*') sb.append(".*"); else if (ch == '?') sb.append('.'); else sb.append(Pattern.quote(String.valueOf(ch))); } re = Pattern.compile("^" + sb + "$", Pattern.DOTALL); }
                else if (op.equals("re")) re = Pattern.compile(value, Pattern.CASE_INSENSITIVE);
                int id = cse ? leafIds.computeIfAbsent(key, k -> leafCount++) : leafCount++;
                Leaf lf = new Leaf(id, fi, op, value, set, re); leafById.putIfAbsent(id, lf); return lf;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path data = Paths.get(args[0]); String rulesPath = args[1]; int N = Integer.parseInt(args[2]); String mode = args[3];
        int chunkRows = args.length > 4 ? Integer.parseInt(args[4]) : 500_000;
        cse = mode.equals("a1cse") || mode.equals("a3"); indexed = mode.equals("a3");
        Map<String,Object> rj = (Map<String,Object>) Json.parse(new String(Files.readAllBytes(Paths.get(rulesPath)), StandardCharsets.UTF_8));
        List<Map<String,Object>> rules = (List<Map<String,Object>>) rj.get("rules");
        int n = Math.min(N, rules.size()); Node[] preds = new Node[n];
        for (int i = 0; i < n; i++) preds[i] = build((Map<String,Object>) rules.get(i).get("expr"));
        int leaves = leafCount;
        if (indexed) index = new LeafIndex(COLS.length, leafById.values());

        List<Path> files = new ArrayList<>(); try (DirectoryStream<Path> ds = Files.newDirectoryStream(data, "*.tsv")) { for (Path p : ds) files.add(p); } Collections.sort(files);
        long rows = 0, matchedRows = 0, ruleHits = 0, evalNanos = 0; perRule = new long[n];
        List<String[]> chunk = new ArrayList<>(chunkRows);
        for (Path f : files) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f.toFile()), StandardCharsets.UTF_8), 1 << 20)) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] r = line.split("\t", -1); for (int c = 0; c < r.length; c++) if (r[c].equals("\\N")) r[c] = null;
                    chunk.add(r);
                    if (chunk.size() == chunkRows) { long[] res = run(mode, preds, chunk); evalNanos += res[0]; matchedRows += res[1]; ruleHits += res[2]; rows += chunk.size(); chunk.clear(); }
                }
            }
        }
        if (!chunk.isEmpty()) { long[] res = run(mode, preds, chunk); evalNanos += res[0]; matchedRows += res[1]; ruleHits += res[2]; rows += chunk.size(); }
        if (System.getenv("PER_RULE") != null) try (PrintWriter pw = new PrintWriter(data.resolve("per_rule_java_" + n + ".txt").toFile())) { for (int i = 0; i < n; i++) pw.println(i + " " + perRule[i]); }
        System.out.printf(Locale.ROOT, "leg=%s rules=%d leaves=%d rows=%d matched_rows=%d rule_hits=%d eval_ms=%.1f ns_per_row=%.1f ns_per_row_rule=%.3f rows_per_s=%.0f%n",
            mode, n, leaves, rows, matchedRows, ruleHits, evalNanos / 1e6, (double) evalNanos / rows, (double) evalNanos / rows / n, rows / (evalNanos / 1e9));
    }

    static LeafIndex index; static long[] perRule;
    static long[] run(String mode, Node[] preds, List<String[]> chunk) {
        long matched = 0, hits = 0; long t0 = System.nanoTime();
        if (mode.equals("a2")) {
            Batch b = new Batch(chunk); boolean[] any = new boolean[b.n]; boolean[] out = new boolean[b.n];
            for (int pi = 0; pi < preds.length; pi++) { preds[pi].evalBatch(b, out); for (int k = 0; k < b.n; k++) if (out[k]) { hits++; any[k] = true; perRule[pi]++; } }
            for (boolean x : any) if (x) matched++;
        } else {
            Row row = new Row(new String[COLS.length]); if (cse) row.memo = new byte[leafCount];
            for (String[] r : chunk) {
                System.arraycopy(r, 0, row.raw, 0, COLS.length); row.reset();
                if (indexed) index.scan(row, row.memo);
                boolean any = false;
                for (int pi = 0; pi < preds.length; pi++) if (preds[pi].eval(row)) { hits++; any = true; perRule[pi]++; }
                if (any) matched++;
            }
        }
        return new long[] { System.nanoTime() - t0, matched, hits };
    }

    // ---------- minimal JSON ----------
    static final class Json {
        final String s; int i = 0; Json(String s) { this.s = s; }
        static Object parse(String s) { Json j = new Json(s); j.ws(); return j.value(); }
        void ws() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }
        Object value() {
            char c = s.charAt(i);
            if (c == '{') { i++; Map<String,Object> m = new LinkedHashMap<>(); ws(); if (s.charAt(i) == '}') { i++; return m; } while (true) { ws(); String k = str(); ws(); i++; ws(); m.put(k, value()); ws(); if (s.charAt(i) == ',') { i++; continue; } i++; return m; } }
            if (c == '[') { i++; List<Object> l = new ArrayList<>(); ws(); if (s.charAt(i) == ']') { i++; return l; } while (true) { ws(); l.add(value()); ws(); if (s.charAt(i) == ',') { i++; continue; } i++; return l; } }
            if (c == '"') return str();
            if (s.startsWith("true", i)) { i += 4; return Boolean.TRUE; } if (s.startsWith("false", i)) { i += 5; return Boolean.FALSE; } if (s.startsWith("null", i)) { i += 4; return null; }
            int st = i; while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) i++; return Double.parseDouble(s.substring(st, i));
        }
        String str() { i++; StringBuilder sb = new StringBuilder(); while (true) { char c = s.charAt(i++); if (c == '"') return sb.toString(); if (c == '\\') { char e = s.charAt(i++); switch (e) { case 'n': sb.append('\n'); break; case 't': sb.append('\t'); break; case 'r': sb.append('\r'); break; case 'b': sb.append('\b'); break; case 'f': sb.append('\f'); break; case 'u': sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; break; default: sb.append(e); } } else sb.append(c); } }
    }
}
