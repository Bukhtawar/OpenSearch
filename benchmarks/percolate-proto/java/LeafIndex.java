import java.util.*;

/**
 * Leaf-indexed evaluation (leg a3): all string leaves of one field are matched in one pass over the
 * field value, setting leaf bits; rule trees are then evaluated from the bits.
 *   contains   -> Aho-Corasick automaton over all tokens of the field
 *   endswith   -> trie over reversed values, walked over the reversed string
 *   startswith -> trie over values, walked forward
 *   eq / in    -> hash lookup
 *   glob / re / isnull -> evaluated individually (few)
 */
final class LeafIndex {
    static final class Trie {
        int[][] next = new int[64][]; int[][] out = new int[64][]; int size = 1;
        Trie() { next[0] = new int[0]; }
        int node() { if (size == next.length) { next = Arrays.copyOf(next, size * 2); out = Arrays.copyOf(out, size * 2); } next[size] = new int[0]; return size++; }
        int child(int n, char c) { int[] e = next[n]; for (int i = 0; i < e.length; i += 2) if (e[i] == c) return e[i + 1]; return -1; }
        int add(int n, char c) { int ch = child(n, c); if (ch >= 0) return ch; ch = node(); int[] e = next[n]; e = Arrays.copyOf(e, e.length + 2); e[e.length - 2] = c; e[e.length - 1] = ch; next[n] = e; return ch; }
        void insert(String s, int leaf) { int n = 0; for (int i = 0; i < s.length(); i++) n = add(n, s.charAt(i)); int[] o = out[n] == null ? new int[0] : out[n]; o = Arrays.copyOf(o, o.length + 1); o[o.length - 1] = leaf; out[n] = o; }
        /** walk s from the start, set bits for every terminal reached (prefix matches) */
        void prefixes(String s, boolean rev, byte[] bits) {
            int n = 0, len = s.length();
            for (int i = 0; i < len; i++) { n = child(n, s.charAt(rev ? len - 1 - i : i)); if (n < 0) return; int[] o = out[n]; if (o != null) for (int l : o) bits[l] = 1; }
        }
    }
    /** Aho-Corasick with HashMap transitions on the (sparse) alphabet. */
    static final class AC {
        final List<Map<Character,Integer>> go = new ArrayList<>(); final List<int[]> out = new ArrayList<>(); int[] fail;
        AC() { go.add(new HashMap<>()); out.add(new int[0]); }
        void add(String s, int leaf) { int n = 0; for (int i = 0; i < s.length(); i++) { Integer ch = go.get(n).get(s.charAt(i)); if (ch == null) { ch = go.size(); go.add(new HashMap<>()); out.add(new int[0]); go.get(n).put(s.charAt(i), ch); } n = ch; } int[] o = out.get(n); o = Arrays.copyOf(o, o.length + 1); o[o.length - 1] = leaf; out.set(n, o); }
        void build() {
            fail = new int[go.size()]; ArrayDeque<Integer> q = new ArrayDeque<>();
            for (int c : go.get(0).values()) { fail[c] = 0; q.add(c); }
            while (!q.isEmpty()) { int u = q.poll(); for (Map.Entry<Character,Integer> e : go.get(u).entrySet()) { char c = e.getKey(); int v = e.getValue(); int f = fail[u]; while (f != 0 && !go.get(f).containsKey(c)) f = fail[f]; Integer fv = go.get(f).get(c); fail[v] = (fv != null && fv != v) ? fv : 0; int[] a = out.get(v), b = out.get(fail[v]); if (b.length > 0) { int[] m = Arrays.copyOf(a, a.length + b.length); System.arraycopy(b, 0, m, a.length, b.length); out.set(v, m); } q.add(v); } }
        }
        void scan(String s, byte[] bits) {
            int n = 0;
            for (int i = 0; i < s.length(); i++) { char c = s.charAt(i); Integer nx; while ((nx = go.get(n).get(c)) == null && n != 0) n = fail[n]; n = nx == null ? 0 : nx; int[] o = out.get(n); for (int l : o) bits[l] = 1; }
        }
    }

    final int nFields; final AC[] contains; final Trie[] ends; final Trie[] starts; final Map<String,int[]>[] eq; final List<Harness.Leaf>[] slow; final boolean[] used;
    @SuppressWarnings("unchecked")
    LeafIndex(int nFields, Collection<Harness.Leaf> leaves) {
        this.nFields = nFields; contains = new AC[nFields]; ends = new Trie[nFields]; starts = new Trie[nFields]; eq = new Map[nFields]; slow = new List[nFields]; used = new boolean[nFields];
        for (int f = 0; f < nFields; f++) { contains[f] = new AC(); ends[f] = new Trie(); starts[f] = new Trie(); eq[f] = new HashMap<>(); slow[f] = new ArrayList<>(); }
        for (Harness.Leaf l : leaves) {
            if (l.field < 0) continue; used[l.field] = true;
            switch (l.op) {
                case Harness.Leaf.CONTAINS: contains[l.field].add(l.value, l.id); break;
                case Harness.Leaf.ENDS: ends[l.field].insert(new StringBuilder(l.value).reverse().toString(), l.id); break;
                case Harness.Leaf.STARTS: starts[l.field].insert(l.value, l.id); break;
                case Harness.Leaf.EQ: eq[l.field].merge(l.value, new int[]{l.id}, (a, b) -> { int[] m = Arrays.copyOf(a, a.length + 1); m[a.length] = b[0]; return m; }); break;
                case Harness.Leaf.IN: for (String v : l.set) eq[l.field].merge(v, new int[]{l.id}, (a, b) -> { int[] m = Arrays.copyOf(a, a.length + 1); m[a.length] = b[0]; return m; }); break;
                default: slow[l.field].add(l);
            }
        }
        for (AC a : contains) a.build();
    }
    /** Fill bits (1 = true) for every indexed leaf of this row; leaves not set are false. */
    void scan(Harness.Row r, byte[] bits) {
        for (int f = 0; f < nFields; f++) {
            if (!used[f]) continue;
            String v = r.lower(f);
            if (v == null) { for (Harness.Leaf l : slow[f]) if (l.op == Harness.Leaf.ISNULL) bits[l.id] = 1; continue; }
            contains[f].scan(v, bits); ends[f].prefixes(v, true, bits); starts[f].prefixes(v, false, bits);
            int[] e = eq[f].get(v); if (e != null) for (int l : e) bits[l] = 1;
            for (Harness.Leaf l : slow[f]) if (l.op != Harness.Leaf.ISNULL && l.test(v)) bits[l.id] = 1;
        }
    }
}
