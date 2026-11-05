//! Query-time leg: evaluate N compiled Sigma predicates over the Parquet dataset with DataFusion.
//! Usage: dfscan <data_dir> <rules.json> <rules.sql> <N> <mode: where|project> <partitions> [repeats]
use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::prelude::*;
use std::time::Instant;

const DATASET_COLS: &[&str] = &[
    "Computer", "User", "Image", "CommandLine", "ParentImage", "ParentCommandLine", "OriginalFileName",
    "IntegrityLevel", "Hashes", "Imphash", "Description", "Product", "Company", "CurrentDirectory", "ParentUser", "LogonId",
];

fn col(f: &str) -> String {
    "l_".to_string() + &f.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect::<String>()
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let (data, rules_json, rules_sql, n, mode, parts) = (&a[1], &a[2], &a[3], a[4].parse::<usize>().unwrap(), a[5].as_str(), a[6].parse::<usize>().unwrap());
    let repeats: usize = a.get(7).map(|s| s.parse().unwrap()).unwrap_or(3);

    let rj: serde_json::Value = serde_json::from_reader(std::fs::File::open(rules_json).unwrap()).unwrap();
    let fields: Vec<String> = rj["fields"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_string()).collect();
    let preds: Vec<String> = std::fs::read_to_string(rules_sql).unwrap().lines().filter(|l| !l.starts_with("--") && !l.trim().is_empty()).map(|s| s.to_string()).collect();
    let preds = &preds[..n.min(preds.len())];

    // lower-cased projection shared by all predicates (common sub-expression, computed once per row)
    let mut proj = vec!["_id".to_string(), "ts".to_string()];
    for f in &fields {
        if DATASET_COLS.contains(&f.as_str()) { proj.push(format!("lower(\"{}\") AS {}", f, col(f))); }
        else { proj.push(format!("CAST(NULL AS VARCHAR) AS {}", col(f))); }
    }
    let any = preds.iter().map(|p| format!("({p})")).collect::<Vec<_>>().join(" OR ");
    let sql = match mode {
        "where" => format!("SELECT _id FROM (SELECT {} FROM t) l WHERE {}", proj.join(", "), any),
        "project" => {
            let bits = preds.iter().enumerate().map(|(i, p)| format!("({p}) AS r{i}")).collect::<Vec<_>>().join(", ");
            format!("SELECT _id, {} FROM (SELECT {} FROM t) l WHERE {}", bits, proj.join(", "), any)
        }
        _ => panic!("mode"),
    };
    std::fs::write(format!("{data}/last_query_{mode}_{n}.sql"), &sql).ok();

    let cfg = SessionConfig::new().with_target_partitions(parts).with_batch_size(8192);
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_parquet("t", &format!("{data}/"), ParquetReadOptions::default().file_extension(".parquet")).await?;
    let total_rows = ctx.sql("SELECT count(*) FROM t").await?.collect().await?;
    let total_rows = datafusion::arrow::util::pretty::pretty_format_batches(&total_rows)?.to_string();
    let total_rows: u64 = total_rows.lines().nth(3).unwrap().trim_matches(|c| c == '|' || c == ' ').parse().unwrap();

    for rep in 0..repeats {
        let t0 = Instant::now();
        let df = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        let t_plan = t0.elapsed();
        let t1 = Instant::now();
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
        let t_exec = t1.elapsed();
        let matched_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut true_bits = 0usize;
        if mode == "project" {
            let mut per_rule = vec![0usize; preds.len()];
            for b in &batches {
                for ci in 1..b.num_columns() {
                    let c = b.column(ci).as_any().downcast_ref::<BooleanArray>().unwrap();
                    true_bits += c.true_count(); per_rule[ci - 1] += c.true_count();
                }
            }
            if std::env::var("PER_RULE").is_ok() && rep == 0 {
                let mut f = std::fs::File::create(format!("{data}/per_rule_df_{n}.txt")).unwrap();
                for (i, c) in per_rule.iter().enumerate() { use std::io::Write; writeln!(f, "{i} {c}").unwrap(); }
            }
        }
        println!(
            "leg=df mode={mode} rules={} partitions={parts} rep={rep} rows={total_rows} matched_rows={matched_rows} rule_hits={true_bits} plan_ms={:.1} exec_ms={:.1} ns_per_row={:.1} ns_per_row_rule={:.3}",
            preds.len(), t_plan.as_secs_f64() * 1e3, t_exec.as_secs_f64() * 1e3,
            t_exec.as_nanos() as f64 / total_rows as f64, t_exec.as_nanos() as f64 / total_rows as f64 / preds.len() as f64
        );
    }
    Ok(())
}
