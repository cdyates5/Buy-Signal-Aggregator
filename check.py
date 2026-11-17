#!/usr/bin/env python3
"""Sanity gate run after rebuild.py and before publishing. Exits non-zero (blocking the deploy, so the last good page
stays live) if the build looks broken; prints GitHub warnings for inputs that are merely stale.

    python3 check.py site/index.html
"""
import datetime as dt, json, re, sys
import numpy as np

path = sys.argv[1] if len(sys.argv) > 1 else "site/index.html"
html = open(path, encoding="utf-8").read()
m = re.search(r"/\*@DATA\*/(.*?)/\*@END\*/", html, re.S)
if not m:
    print(f"::error::no data payload in {path}"); sys.exit(1)
P = json.loads(m.group(1).replace("<\\/", "</"))
M, N = P["meta"], P["now"]
today = dt.datetime.now(dt.timezone.utc).date()
errors, warnings = [], []

lag = int(np.busday_count(M["asof"], today.isoformat()))
if lag > 3:
    errors.append(f"S&P 500 data ends {M['asof']}, {lag} business days ago")
if M["members_last"] < 450:
    errors.append(f"only {M['members_last']} index members priced on the last day (expect ~500)")
if (M.get("cov_last") or 0) < 90:
    errors.append(f"price coverage {M.get('cov_last')}% of the index (expect ~100%)")
if M["n_legs"] != 14 or len(P["legs"]) != 14:
    errors.append(f"expected 14 signals, found {len(P['legs'])}")
if N.get("cnt") is None or N.get("st") not in (-1, 0, 1):
    errors.append("aggregator count/state missing")
if len(P["d"]) < 5000 or any(v is None for v in P["spx"][-60:]):
    errors.append("S&P 500 series short or has gaps in the last 60 sessions")

for label, month, limit in (("ISM New Orders/Prices Paid", M["ism_month"], 70), ("PPI", M["ppi_month"], 75), ("CPI", M["cpi_month"], 75)):
    age = (today - dt.date.fromisoformat(month + "-01")).days
    if age > limit:
        warnings.append(f"{label} last month is {month}; add newer months to data/ism_overrides.csv" if label.startswith("ISM")
                        else f"{label} last month is {month}; FRED may be late")

for w in warnings:
    print(f"::warning::{w}")
for e in errors:
    print(f"::error::{e}")
status = {"asof": M["asof"], "built": M["built"], "active": N["cnt"], "state": N["st"], "screened_active": N["cntc"],
          "screened_state": N["stc"], "ism_month": M["ism_month"], "ok": not errors, "warnings": warnings, "errors": errors}
open(path.rsplit("/", 1)[0] + "/status.json" if "/" in path else "status.json", "w").write(json.dumps(status, indent=2))
print(json.dumps(status, indent=2))
sys.exit(1 if errors else 0)
