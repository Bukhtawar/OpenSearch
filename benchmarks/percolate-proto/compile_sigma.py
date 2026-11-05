#!/usr/bin/env python3
"""Compile Sigma process_creation rules into a predicate IR (JSON) plus DataFusion SQL.

Output IR node shapes:
  {"op":"and"|"or", "args":[...]}
  {"op":"not", "arg": node}
  {"op":"eq"|"contains"|"startswith"|"endswith"|"glob"|"re", "field": F, "value": V}
  {"op":"in", "field": F, "values":[...]}        # OR of eq
Values are lower-cased: Sigma matching is case-insensitive, both evaluators lower-case
field values once per row.
"""
import json, os, re, sys, glob
import yaml

RULES_DIR = sys.argv[1]
OUT = sys.argv[2]
FIELD_ALIASES = {"Commandline": "CommandLine", "OriginalName": "OriginalFileName"}
UNSUPPORTED_MODS = {"base64", "base64offset", "windash", "cidr", "utf16", "utf16le", "utf16be", "wide", "expand", "lt", "lte", "gt", "gte", "fieldref", "exists"}

stats = {"files": 0, "compiled": 0, "skip_keyword": 0, "skip_agg": 0, "skip_modifier": 0, "skip_parse": 0, "skip_other": 0}
field_use = {}

def lc(v):
    return str(v).lower()

def leaf(field, mods, value):
    field = FIELD_ALIASES.get(field, field)
    field_use[field] = field_use.get(field, 0) + 1
    v = lc(value)
    if "re" in mods:
        if os.environ.get("NO_RE"):
            return {"op": "false"}
        return {"op": "re", "field": field, "value": str(value)}
    has_wild = ("*" in v) or ("?" in v)
    if "contains" in mods:
        return {"op": "glob" if has_wild else "contains", "field": field, "value": ("*" + v + "*") if has_wild else v}
    if "startswith" in mods:
        return {"op": "glob" if has_wild else "startswith", "field": field, "value": (v + "*") if has_wild else v}
    if "endswith" in mods:
        return {"op": "glob" if has_wild else "endswith", "field": field, "value": ("*" + v) if has_wild else v}
    if has_wild:
        return {"op": "glob", "field": field, "value": v}
    return {"op": "eq", "field": field, "value": v}

def compile_selection(sel):
    """A selection is a map (AND of fields; list value = OR) or a list of maps (OR)."""
    if isinstance(sel, list):
        if all(isinstance(x, dict) for x in sel):
            return {"op": "or", "args": [compile_selection(x) for x in sel]}
        raise KeyError("keyword")
    if not isinstance(sel, dict):
        raise KeyError("keyword")
    parts = []
    for k, v in sel.items():
        bits = k.split("|")
        field, mods = bits[0], bits[1:]
        bad = [m for m in mods if m not in ("contains", "startswith", "endswith", "all", "re")]
        if bad:
            raise ValueError("modifier:" + ",".join(bad))
        if field == "" or field is None:
            raise KeyError("keyword")
        vals = v if isinstance(v, list) else [v]
        if any(x is None for x in vals):
            # Sigma: `field: null` means the field is absent
            field_use[FIELD_ALIASES.get(field, field)] = field_use.get(FIELD_ALIASES.get(field, field), 0) + 1
            parts.append({"op": "isnull", "field": FIELD_ALIASES.get(field, field)})
            vals = [x for x in vals if x is not None]
            if not vals:
                continue
        if "all" in mods:
            parts.append({"op": "and", "args": [leaf(field, mods, x) for x in vals]})
        elif len(vals) == 1:
            parts.append(leaf(field, mods, vals[0]))
        else:
            leaves = [leaf(field, mods, x) for x in vals]
            if all(l["op"] == "eq" for l in leaves):
                parts.append({"op": "in", "field": leaves[0]["field"], "values": [l["value"] for l in leaves]})
            else:
                parts.append({"op": "or", "args": leaves})
    if len(parts) == 1:
        return parts[0]
    return {"op": "and", "args": parts}

TOK = re.compile(r"\s*(\(|\)|\band\b|\bor\b|\bnot\b|1 of them|all of them|1 of [A-Za-z0-9_*]+|all of [A-Za-z0-9_*]+|[A-Za-z0-9_]+)")

def tokenize(cond):
    pos, out = 0, []
    while pos < len(cond):
        m = TOK.match(cond, pos)
        if not m:
            raise SyntaxError(cond)
        out.append(m.group(1)); pos = m.end()
    return out

def parse_condition(cond, sels):
    toks = tokenize(cond)
    i = [0]
    def peek(): return toks[i[0]] if i[0] < len(toks) else None
    def take(): t = toks[i[0]]; i[0] += 1; return t
    def expand(prefix, how):
        names = [n for n in sels if (n == prefix if not prefix.endswith("*") else n.startswith(prefix[:-1]))] if prefix != "them" else list(sels)
        if not names: raise SyntaxError("no selections for " + prefix)
        return {"op": how, "args": [compile_selection(sels[n]) for n in names]}
    def atom():
        t = take()
        if t == "(":
            e = expr(); assert take() == ")"; return e
        if t == "not":
            return {"op": "not", "arg": atom()}
        if t.startswith("1 of "):
            return expand(t[5:], "or")
        if t.startswith("all of "):
            return expand(t[7:], "and")
        if t not in sels: raise SyntaxError("unknown selection " + t)
        return compile_selection(sels[t])
    def term():
        e = atom()
        while peek() == "and":
            take(); e = {"op": "and", "args": [e, atom()]}
        return e
    def expr():
        e = term()
        while peek() == "or":
            take(); e = {"op": "or", "args": [e, term()]}
        return e
    e = expr()
    if peek() is not None: raise SyntaxError("trailing " + str(peek()))
    return e

def flatten(n):
    if n["op"] in ("and", "or"):
        args = []
        for a in n["args"]:
            a = flatten(a)
            if a["op"] == n["op"]: args.extend(a["args"])
            else: args.append(a)
        return {"op": n["op"], "args": args}
    if n["op"] == "not":
        return {"op": "not", "arg": flatten(n["arg"])}
    return n

rules = []
for path in sorted(glob.glob(os.path.join(RULES_DIR, "**", "*.yml"), recursive=True)):
    try:
        docs = list(yaml.safe_load_all(open(path)))
    except Exception:
        stats["skip_parse"] += 1; continue
    d = docs[0]
    if not isinstance(d, dict) or d.get("logsource", {}).get("category") != "process_creation":
        continue
    stats["files"] += 1
    det = d.get("detection") or {}
    cond = det.get("condition")
    if isinstance(cond, list):
        cond = " or ".join("(%s)" % c for c in cond)
    if not cond: stats["skip_other"] += 1; continue
    if "|" in cond or " near " in cond:
        stats["skip_agg"] += 1; continue
    sels = {k: v for k, v in det.items() if k != "condition"}
    try:
        expr = flatten(parse_condition(cond, sels))
    except KeyError:
        stats["skip_keyword"] += 1; continue
    except ValueError as e:
        stats["skip_modifier"] += 1; continue
    except (SyntaxError, AssertionError, IndexError) as e:
        stats["skip_parse"] += 1; continue
    rules.append({"id": d.get("id"), "title": d.get("title"), "level": d.get("level"), "expr": expr})
    stats["compiled"] += 1

# ---- SQL emission (predicates reference lower-cased projected columns l_<field>) ----
def sql_lit(v):
    return "'" + v.replace("'", "''") + "'"
def like_escape(v):
    return v.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
def glob_to_like(v):
    out = []
    for ch in v:
        if ch == "*": out.append("%")
        elif ch == "?": out.append("_")
        else: out.append(like_escape(ch))
    return "".join(out)
def col(f): return "l_" + re.sub(r"[^A-Za-z0-9_]", "_", f)
def to_sql(n):
    op = n["op"]
    if op == "and": return "(" + " AND ".join(to_sql(a) for a in n["args"]) + ")"
    if op == "or": return "(" + " OR ".join(to_sql(a) for a in n["args"]) + ")"
    if op == "not": return "(NOT " + to_sql(n["arg"]) + ")"
    if op == "false": return "FALSE"
    c = col(n["field"])
    if op == "isnull": return c + " IS NULL"
    if op == "eq": return c + " = " + sql_lit(n["value"])
    if op == "in": return c + " IN (" + ", ".join(sql_lit(v) for v in n["values"]) + ")"
    if op == "contains": return c + " LIKE " + sql_lit("%" + like_escape(n["value"]) + "%")
    if op == "startswith": return c + " LIKE " + sql_lit(like_escape(n["value"]) + "%")
    if op == "endswith": return c + " LIKE " + sql_lit("%" + like_escape(n["value"]))
    if op == "glob": return c + " LIKE " + sql_lit(glob_to_like(n["value"]))
    if op == "re": return "regexp_like(" + c + ", " + sql_lit("(?i)" + n["value"]) + ")"
    raise ValueError(op)

fields = sorted(field_use, key=lambda f: -field_use[f])
os.makedirs(OUT, exist_ok=True)
json.dump({"fields": fields, "field_use": field_use, "rules": rules}, open(os.path.join(OUT, "rules.json"), "w"), indent=1)
with open(os.path.join(OUT, "rules.sql"), "w") as f:
    for i, r in enumerate(rules):
        f.write("-- %d %s\n%s\n" % (i, r["title"], to_sql(r["expr"])))
json.dump(stats, open(os.path.join(OUT, "compile_stats.json"), "w"), indent=1)
print(json.dumps(stats))
print("fields:", fields[:20])
