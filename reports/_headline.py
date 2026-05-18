"""Quick headline across completed bench runs."""
import json
import statistics
from collections import defaultdict
from pathlib import Path

ENGS = ["df", "duckdb", "spark-default", "spark-tuned"]


def warm_med_per_engine(run_dir):
    out = {}
    for eng in ENGS:
        jsonl = Path(run_dir) / "raw" / f"{eng}.jsonl"
        if not jsonl.exists():
            continue
        warm = defaultdict(list)
        n_fail = 0
        for line in jsonl.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            sid = r.get("step_id", "")
            if not sid.startswith("q") and not sid.startswith("write_"):
                continue
            # A run is a failure if it has an `error` field set, regardless
            # of whether wall_ms is non-None (parse failures still cost a
            # round-trip, so wall_ms is set but no actual work happened).
            if r.get("error"):
                n_fail += 1
                continue
            ms = r.get("engine_reported_ms") or r.get("wall_ms")
            if ms is None or r.get("cold"):
                continue
            warm[sid].append(ms)
        per_q = [statistics.median(v) for v in warm.values() if v]
        if per_q:
            n_ok = sum(1 for v in warm.values() if v)
            out[eng] = (statistics.median(per_q), n_ok, len(warm), n_fail)
    return out


def main():
    results = Path("/workspace/results")
    order = ["publish-writes", "publish-ssb", "publish-tpch",
             "publish-tpcds", "publish-job"]
    rows = []
    for tag in order:
        dirs = sorted(results.glob(f"*-{tag}"))
        if not dirs:
            continue
        d = dirs[-1]
        bench = tag.replace("publish-", "")
        eng_data = warm_med_per_engine(d)
        for eng in ENGS:
            if eng in eng_data:
                med, ok, total, fail = eng_data[eng]
                rows.append((bench, eng, ok, total, fail, med))
    print(f"{'bench':<10}{'engine':<18}{'completed':<14}{'fail':<8}{'warm-median ms':<18}")
    print("-" * 68)
    last_bench = None
    for bench, eng, ok, total, fail, med in rows:
        if bench != last_bench and last_bench is not None:
            print()
        med_str = f"{med:.2f}" if ok else "(no successful runs)"
        print(f"{bench:<10}{eng:<18}{ok}/{total:<11}{fail:<8}{med_str:<18}")
        last_bench = bench


if __name__ == "__main__":
    main()
