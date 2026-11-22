#!/usr/bin/env python3
"""
rebuild.py - S&P 500 Buy Signal Aggregator

Regenerates sp500_buy_signal_aggregator.html from an empty directory.

    python3 rebuild.py                         # -> /mnt/user-data/outputs/sp500_buy_signal_aggregator.html (or ./ if absent)
    python3 rebuild.py --out my.html --refresh # ignore cached downloads
    python3 rebuild.py --ism ism_history.csv   # replace the ISM series with your own (date,new_orders,prices_paid)

Pipeline
  1. Point-in-time S&P 500 membership (fja05680/sp500 history rolled forward with Wikipedia's change table).
  2. Daily OHLCV and splits for every member since 1998 (Yahoo), plus ^GSPC from 1927, ^VIX and ^VIX3M.
  3. Bottom-up breadth: advance/decline, %>50/150/200DMA, McClellan Summation, TRIN proxy, P&F Bullish Percent Index.
  4. Macro: PPI and CPI (FRED), ISM New Orders minus Prices Paid (digitised 1948-2020, actual prints from 2021,
     latest months from Trading Economics' public summary page, OECD orders series via FRED as a cross-check).
  5. Fourteen buy-signal triggers, the aggregator, forward-return evidence.
  6. Data injected into the embedded HTML template (re.subn with a lambda replacement).

Requires: pandas, numpy, requests, yfinance, lxml. A first run downloads about 1,170 tickers and takes roughly 8 minutes.
Set FRED_API_KEY to use your own FRED key.
"""
import argparse, html, io, json, os, pickle, re, sys, time, warnings
import datetime as dt
import numpy as np, pandas as pd, requests

warnings.filterwarnings("ignore")
CACHE = os.environ.get("AGG_CACHE", "cache")


# ============================================================ FETCH: membership and prices
# Stage 1: membership, constituent prices (OHLCV), index and volatility series. Cached in ./cache.
START = "1998-01-01"
UA = {"User-Agent": "Mozilla/5.0 (research; buy-signal-aggregator)"}
MEMBERS_URL = ("https://raw.githubusercontent.com/fja05680/sp500/master/"
               "S%26P%20500%20Historical%20Components%20%26%20Changes%20(Updated).csv")
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_HIST_URL = "https://en.wikipedia.org/wiki/Historical_components_of_the_S%26P_500"


def log(*a):
    print(dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S"), *a, flush=True)


def get_membership():
    """Point-in-time membership: fja05680 history (1996->) rolled forward with Wikipedia changes."""
    txt = requests.get(MEMBERS_URL, headers=UA, timeout=60).text
    hist = pd.read_csv(io.StringIO(txt), parse_dates=["date"])
    hist["set"] = hist["tickers"].str.split(",").apply(lambda x: frozenset(t.strip() for t in x if t.strip()))
    hist = hist[["date", "set"]].sort_values("date").reset_index(drop=True)
    last_date, cur = hist["date"].iloc[-1], set(hist["set"].iloc[-1])
    notes = {"hist_last": str(last_date.date()), "wiki_applied": 0, "wiki_ok": False}
    try:
        html = requests.get(WIKI_URL, headers=UA, timeout=60).text
        tabs = pd.read_html(io.StringIO(html))
        current = set(tabs[0]["Symbol"].astype(str).str.strip())
        ch = next((t for t in tabs[1:] if any("Removed" in str(c) for c in t.columns)), None)
        if ch is None:   # change table moved to its own page in Aug 2026
            html2 = requests.get(WIKI_HIST_URL, headers=UA, timeout=60).text
            ch = next(t for t in pd.read_html(io.StringIO(html2)) if any("Removed" in str(c) for c in t.columns))
        ch = ch.copy()
        ch.columns = ["_".join([str(c) for c in col if "Unnamed" not in str(c)]).strip("_") if isinstance(col, tuple) else str(col)
                      for col in ch.columns]
        dcol = [c for c in ch.columns if c.lower().startswith("date") or "effective" in c.lower()][0]
        acol = [c for c in ch.columns if "Added" in c and "Ticker" in c][0]
        rcol = [c for c in ch.columns if "Removed" in c and "Ticker" in c][0]
        ch["d"] = pd.to_datetime(ch[dcol], errors="coerce")
        ch = ch.dropna(subset=["d"]).sort_values("d")
        rows = []
        for d, g in ch[ch["d"] > last_date].groupby("d"):
            for _, r in g.iterrows():
                a, rm = str(r[acol]).strip(), str(r[rcol]).strip()
                if rm and rm != "nan":
                    cur.discard(rm)
                if a and a != "nan":
                    cur.add(a)
                notes["wiki_applied"] += 1
            rows.append({"date": d, "set": frozenset(cur)})
        notes["recon_missing"] = len(current - cur)
        notes["recon_extra"] = len(cur - current)
        today = pd.Timestamp(dt.datetime.now(dt.timezone.utc).date())
        rows.append({"date": today, "set": frozenset(current)})
        hist = pd.concat([hist, pd.DataFrame(rows)], ignore_index=True)
        notes["wiki_ok"] = True
    except Exception as e:  # history file alone is still usable
        notes["wiki_err"] = repr(e)[:120]
    return hist, notes


def ysym(t):
    return t.replace(".", "-")


def _flat(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def _download(batch, start, actions=True):
    import yfinance as yf
    for attempt in range(4):
        try:
            df = yf.download([ysym(t) for t in batch], start=start, auto_adjust=False, actions=actions, progress=False,
                             threads=True, group_by="column", timeout=30)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            log("retry", attempt + 1, repr(e)[:80])
        time.sleep(5 * (attempt + 1))
    return None


def _index_series(out):
    import yfinance as yf
    for sym, key in (("^GSPC", "spx"), ("^VIX", "vix"), ("^VIX3M", "vix3m")):
        for attempt in range(4):
            try:
                d = _flat(yf.download(sym, start="1927-01-01", auto_adjust=False, progress=False))
                if len(d) > 100:
                    out[key] = d; break
            except Exception as e:
                log("retry", sym, repr(e)[:80])
            time.sleep(5 * (attempt + 1))
    return out


def _full_download(tickers):
    F = {"Close": {}, "High": {}, "Low": {}, "Adj Close": {}, "Volume": {}}; SPL = {}
    B = 60
    for i in range(0, len(tickers), B):
        batch = tickers[i:i + B]
        df = _download(batch, START)
        if df is None:
            continue
        for t in batch:
            y = ysym(t)
            try:
                c = df["Close"][y].dropna()
                if len(c) < 30:
                    continue
                for k in F:
                    F[k][t] = df[k][y].dropna().astype("float32" if k != "Volume" else "float64")
                if "Stock Splits" in df.columns.get_level_values(0):
                    sp = df["Stock Splits"][y].dropna(); sp = sp[sp > 0]
                    if len(sp):
                        SPL[t] = sp
            except Exception:
                pass
        log(f"prices {min(i + B, len(tickers))}/{len(tickers)} ok={len(F['Close'])}")
        time.sleep(0.5)
    return {"close": pd.DataFrame(F["Close"]), "high": pd.DataFrame(F["High"]), "low": pd.DataFrame(F["Low"]),
            "adj": pd.DataFrame(F["Adj Close"]), "vol": pd.DataFrame(F["Volume"]), "splits": SPL}


def _top_up(px, tickers, overlap_days=21):
    """Append the latest sessions for `tickers` to a cached panel. Yahoo restates the whole history after a split or
    dividend, so each ticker's cached history is rescaled to match the fresh download on the overlapping sessions."""
    last = px["adj"].index.max()
    start = (last - pd.Timedelta(days=overlap_days)).strftime("%Y-%m-%d")
    cols = {k: {c: px[k][c].dropna() for c in px[k].columns} for k in ("close", "high", "low", "adj", "vol")}
    SPL = dict(px.get("splits", {}))
    fld = {"close": "Close", "high": "High", "low": "Low", "adj": "Adj Close", "vol": "Volume"}
    tickers = sorted(tickers); B = 80; n_ok = n_rescaled = 0
    for i in range(0, len(tickers), B):
        batch = tickers[i:i + B]
        df = _download(batch, start)
        if df is None:
            log("top-up batch failed; keeping cached history for", len(batch), "tickers"); continue
        for t in batch:
            y = ysym(t)
            try:
                new = {k: df[f][y].dropna() for k, f in fld.items()}
            except Exception:
                continue
            if len(new["adj"]) == 0:
                continue
            old_c, old_a = cols["close"].get(t), cols["adj"].get(t)
            r_c = r_a = 1.0
            if old_c is not None and len(old_c):
                ov = old_c.index.intersection(new["close"].index)[:-1]
                if len(ov) >= 3:
                    r_c = float(np.median(new["close"][ov].values / old_c[ov].values))
                    r_a = float(np.median(new["adj"][ov].values / old_a.reindex(ov).values))
                elif new["close"].index[0] > old_c.index[-1] + pd.Timedelta(days=10):
                    continue                     # no overlap: leave for the full download
            if abs(r_c - 1) > 1e-4 or abs(r_a - 1) > 1e-4:
                n_rescaled += 1
            first = new["adj"].index[0]
            for k in fld:
                old = cols[k].get(t)
                r = {"adj": r_a, "vol": 1 / r_c}.get(k, r_c)
                keep = old[old.index < first] * r if old is not None else None
                ser = pd.concat([keep, new[k]]) if keep is not None else new[k]
                cols[k][t] = ser.astype("float64" if k == "vol" else "float32")
            if "Stock Splits" in df.columns.get_level_values(0):
                sp = df["Stock Splits"][y].dropna(); sp = sp[sp > 0]
                if len(sp):
                    SPL[t] = pd.concat([SPL.get(t, pd.Series(dtype=float)), sp]).groupby(level=0).last()
            n_ok += 1
        log(f"top-up {min(i + B, len(tickers))}/{len(tickers)} updated={n_ok} rescaled={n_rescaled}")
        time.sleep(0.5)
    out = {k: pd.DataFrame(v) for k, v in cols.items()}
    out["splits"] = SPL
    return out


def get_prices(tickers, recent=None):
    """Daily OHLCV for every ticker. With a cache from an earlier run, only `recent` tickers (index members within
    roughly the last year) are topped up and tickers missing from the cache are downloaded in full."""
    os.makedirs(CACHE, exist_ok=True)
    fp = os.path.join(CACHE, "prices.pkl")
    if os.path.exists(fp) and time.time() - os.path.getmtime(fp) < 18 * 3600:
        log("using cached prices")
        return pickle.load(open(fp, "rb"))
    tickers = sorted(tickers)
    if os.path.exists(fp) and recent is not None:
        px = pickle.load(open(fp, "rb"))
        if "splits" not in px:
            sfp = os.path.join(CACHE, "splits.pkl")
            px["splits"] = pickle.load(open(sfp, "rb")) if os.path.exists(sfp) else {}
        log("cached prices to", px["adj"].index.max().date(), "- topping up", len(recent), "recent tickers")
        dead = set(px.get("dead", ()))
        missing = sorted(set(recent) - set(px["adj"].columns) - dead)
        out = _top_up(px, sorted(set(recent) & set(px["adj"].columns)))
        if missing:
            log("full history for", len(missing), "new tickers:", ", ".join(missing[:12]))
            add = _full_download(missing)
            for k in ("close", "high", "low", "adj", "vol"):
                if len(add[k].columns):
                    out[k] = out[k].join(add[k], how="outer")
            out["splits"].update(add["splits"])
            dead |= set(missing) - set(add["adj"].columns)        # Yahoo has no history: skip until --refresh
        out["dead"] = sorted(dead)
        out["mode"] = "incremental"
    else:
        out = _full_download(tickers); out["mode"] = "full"
    out = _index_series(out)
    pickle.dump(out, open(fp, "wb"))
    return out


# ============================================================ BREADTH: constituent breadth, TRIN proxy, P&F Bullish Percent
# Stage 2: bottom-up breadth series from point-in-time S&P 500 constituents (incl. P&F Bullish Percent, TRIN).
# StockCharts "traditional" point-and-figure box scale: (range start, box size)
_PF = [(0.0, 0.0625), (0.25, 0.125), (1.0, 0.25), (5.0, 0.5), (20.0, 1.0), (100.0, 2.0), (200.0, 4.0), (500.0, 5.0), (1000.0, 50.0), (25000.0, 500.0)]
_LO = np.array([a for a, _ in _PF]); _ST = np.array([b for _, b in _PF])
_OFF = np.concatenate([[0.0], np.cumsum((_LO[1:] - _LO[:-1]) / _ST[:-1])])


def pf_idx(p):
    """Price -> continuous box index on the traditional scale."""
    p = np.asarray(p, dtype=float)
    k = np.clip(np.searchsorted(_LO, p, side="right") - 1, 0, len(_LO) - 1)
    return _OFF[k] + (p - _LO[k]) / _ST[k]


def pf_price(i):
    k = int(np.clip(np.searchsorted(_OFF, i, side="right") - 1, 0, len(_OFF) - 1))
    return _LO[k] + (i - _OFF[k]) * _ST[k]


def pf_signal(hi, lo, ratio):
    """3-box-reversal P&F on high/low; returns +1 (double-top buy) / -1 (double-bottom sell) / 0 per day.
    hi/lo are as-traded prices; ratio[t] > 0 marks a split on day t (chart levels are rescaled)."""
    n = len(hi); out = np.zeros(n, dtype=np.int8)
    H = np.floor(pf_idx(hi) + 1e-9); L = np.ceil(pf_idx(lo) - 1e-9)
    col = 0; top = bot = 0.0; pxt = pob = None; sig = 0
    for t in range(n):
        h = H[t]; l = L[t]
        if h != h or l != l:
            out[t] = sig; continue
        if col == 0:
            col, top, bot = 1, h, l
        else:
            r = ratio[t]
            if r > 0:
                f = lambda v: None if v is None else float(np.round(pf_idx(pf_price(v) / r)))
                top, bot, pxt, pob = f(top), f(bot), f(pxt), f(pob)
            if col == 1:
                if h > top:
                    top = h
                    if pxt is not None and top > pxt: sig = 1
                elif l <= top - 3:
                    pxt = top; col = -1; bot = l
                    if pob is not None and bot < pob: sig = -1
            else:
                if l < bot:
                    bot = l
                    if pob is not None and bot < pob: sig = -1
                elif h >= bot + 3:
                    pob = bot; col = 1; top = h
                    if pxt is not None and top > pxt: sig = 1
        out[t] = sig
    return out


def build_breadth(px, hist, splits=None, start="1998-01-01", with_bpi=True):
    adj, close, vol, spx = px["adj"], px["close"], px["vol"], px["spx"]
    idx = spx.index[spx.index >= start]
    adj = adj.reindex(idx).astype("float64"); close = close.reindex(idx).astype("float64"); vol = vol.reindex(idx)
    adj = adj.where(adj > 0)
    cols = adj.columns
    h = hist.sort_values("date")
    mrows = np.zeros((len(h), len(cols)), dtype=bool)
    cpos = {c: i for i, c in enumerate(cols)}
    for r, s in enumerate(h["set"]):
        for t in s:
            j = cpos.get(t)
            if j is not None:
                mrows[r, j] = True
    pos = np.clip(np.searchsorted(h["date"].values, idx.values, side="right") - 1, 0, len(h) - 1)
    member = pd.DataFrame(mrows[pos], index=idx, columns=cols)
    n_index = pd.Series(np.array([len(s) for s in h["set"]])[pos], index=idx)

    ret = adj.pct_change(fill_method=None)
    ret = ret.where(ret.abs() < 0.9)
    valid = member & ret.notna()
    up = valid & (ret > 0); dn = valid & (ret < 0)
    adv = up.sum(axis=1); dec = dn.sum(axis=1); n = valid.sum(axis=1)

    out = pd.DataFrame(index=idx)
    out["spx"] = spx["Close"].reindex(idx)
    out["n"] = n; out["coverage"] = n / n_index
    out["adv"] = adv; out["dec"] = dec
    out["adv_share"] = adv / (adv + dec)
    # TRIN proxy: each stock's volume is scaled by its own trailing 50-day median so mega-cap share volume does not dominate
    rv = (vol / vol.rolling(50, min_periods=20).median().shift(1)).clip(upper=10)
    upv = rv.where(up).sum(axis=1); dnv = rv.where(dn).sum(axis=1)
    out["trin"] = (adv / dec) / (upv / dnv)
    for w in (50, 150, 200):
        ma = adj.rolling(w, min_periods=w).mean()
        ok = member & ma.notna() & adj.notna()
        out[f"p{w}"] = 100 * (ok & (adj > ma)).sum(axis=1) / ok.sum(axis=1)
    rana = 1000 * (adv - dec) / (adv + dec)
    e19 = rana.ewm(span=19, adjust=False).mean(); e39 = rana.ewm(span=39, adjust=False).mean()
    out["osc"] = e19 - e39
    out["msi"] = 19 * e39 - 9 * e19        # Miekka closed form: level is start-date independent

    if with_bpi:
        hi = px["high"].reindex(idx).astype("float64"); lo = px["low"].reindex(idx).astype("float64")
        sig = np.zeros((len(idx), len(cols)), dtype=np.int8)
        for j, c in enumerate(cols):
            ratio = np.zeros(len(idx)); fac = np.ones(len(idx))
            if splits is not None and c in splits:
                s = splits[c]; s = s[(s.index > idx[0]) & (s.index <= idx[-1])]
                for d, r in s.items():
                    k = idx.searchsorted(d)
                    if k < len(idx):
                        ratio[k] = r; fac[:k] *= r      # undo split adjustment before the split date
            sig[:, j] = pf_signal(hi[c].values * fac, lo[c].values * fac, ratio)
        sg = pd.DataFrame(sig, index=idx, columns=cols)
        buy = (member & (sg == 1)).sum(axis=1); sell = (member & (sg == -1)).sum(axis=1)
        out["bpi"] = 100 * buy / (buy + sell)
        out.loc[(buy + sell) < 0.6 * n, "bpi"] = np.nan    # warm-up: most charts still without a signal
    return out.dropna(subset=["spx"]), member


# ============================================================ MACRO: PPI, CPI, ISM New Orders and Prices Paid
# Stage 1b: macro inputs for the Inflation and Stagflation models (PPI, CPI, ISM New Orders / Prices Paid).
FRED_KEY = os.environ.get("FRED_API_KEY", "")   # set as a repository secret; see README
UA_WEB = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}

# ISM New Orders minus Prices Paid, 1948-01..2020-12, digitised from SentimenTrader's chart (±3 pts, ±1-2 months).
SPREAD_DIGITISED = [-13.3,-26.6,-25.5,-21.7,-21.7,-26.9,-30.7,-4.9,-4.9,-8.5,-11.8,-11.1,-6.5,8.5,27.3,23.4,27.3,21.7,10.8,13.3,14.1,7.7,3.1,3.1,-5,-13.2,-9.5,-20.4,-17.5,-13.3,-8.3,-19.5,-35.4,-29.6,-17.6,-17.1,-16.6,-16,-16,-26.1,-16.6,-8.8,-5,0.2,-0.8,-6.2,-11.2,-5.5,-10.3,-3.4,-6.6,5.4,5.4,7.1,8.8,4.5,2.4,9,12.2,19,-9.3,-11.8,-20.3,-23,-21.1,-17.8,-4.6,-4.6,-6.2,-6.8,-8,-10,8.1,7.9,9.1,14,11.8,12.2,13,9.4,8.8,9,9,13.7,-8.1,-1.1,1.3,-4.6,-15,-12.7,-9.5,-18.4,-15.5,-14.9,-13.9,-21.7,-27.2,-27.2,-30,-31.4,-17.2,-29.8,-25.7,-21.4,-19.4,-23.7,-26.3,-29.2,-24.1,-14.2,-11.5,-13.2,-15,-16.7,-30.7,-9.5,-12.2,-8.3,-11.6,-12.2,-7.6,-0.9,4.5,9.9,11.3,13.4,2.2,2.2,-2.3,-5,1.1,9.4,4.3,1.5,2.6,-0.5,-0.5,-3.6,-6,-17.7,-14.4,-14.4,-7.3,1,-2.9,-6.8,-6.3,-10.9,-4.8,-3.1,4.4,2.9,3.3,4.7,0.3,-2,-2,-0.8,15.3,11.5,12.5,13,12.5,12.8,13,13.5,16.5,2.4,4.5,3.4,3.1,3.1,1.4,1.1,3.4,7.1,9.1,10.2,11,11.9,9.1,9.1,5.3,1.4,-0.7,1.1,1.1,1.1,-0.3,-12.8,-4.5,-0.3,1.3,1.1,-0.3,-0.6,5.7,8.5,5.3,-1.5,-9.7,-9.7,-6.5,-6.5,-8.1,-2,3.5,-10.8,-15.9,-15.5,-8.8,-8.8,-7,-8.8,-4.6,-12.2,-12.2,-20.3,-20.7,-28.9,-27.9,-25.5,-24.8,-18.8,-20.1,-24.9,-21.8,-22.8,-20.3,-9.2,-9.2,-9.6,-8.7,-7,-12.1,-13.6,-14.2,-22.6,-26.7,-18.5,-19.7,-20.2,-7.6,-15,-16.2,-16.2,-12.9,-20.2,-16.9,-8.6,-17.6,-22.7,-22.1,-21.5,-22.4,-23.1,-27.7,-31,-32.7,-33,-33,-32.4,-31.9,-33.9,-33.7,-22.8,-28.5,-32.9,-22.3,-20,-18.7,-26.4,-24,-23.1,-12.7,-12.7,-2.4,-7.7,-11,-11.9,-16.1,-19.9,-10.5,4.3,9.4,-0.9,1.3,-14.3,-9.3,-9.6,-9.6,-10.3,-4.9,-6.5,-5.6,-4.8,1,-2,-6,-10.2,-17.2,-22.7,-24,-25.4,-26.4,-26.4,-21,-31.7,-29.3,-31.9,-35,-37.5,-38.4,-36.4,-37.6,-40.7,-37.6,-36.4,-38.4,-38.4,-39.2,-39,-37.9,-34.4,-17.5,-5.6,-0.2,2.4,5.1,10.3,-7.6,-12.8,-11.3,-8.4,-8.4,-1.8,-5.7,-6,-12.8,-11.9,-18.1,-26.1,-17.2,-15.5,-13,-12.5,-17.3,-20.8,-19.8,-19.8,-17.6,-12.5,-12.7,-13.2,-10.7,-11.3,-7.9,-3.3,-6.2,-10.7,-16.9,-17.3,-18.3,-18.3,-18.3,-18.7,-19.3,-24.1,-27.6,-22.6,-24,-25.2,-28.5,-30.8,-31.7,-34.7,-41,-38,-37.7,-37.5,-37.2,-38.9,-42.5,-38.2,-33.3,-40,-41.8,-43.7,-39,-23.7,-14.6,-13.6,-12,-21.4,-21.4,-24.8,-26,-20,-21.4,-11.1,-19.6,-19.3,-19.2,-20.4,-17.9,-25.5,-16.1,-9.8,-9.8,-1.5,4.7,2,-0.8,2,0.2,-0.7,0.8,3.6,6,6.7,9.4,12.5,4.3,4.3,1.7,6.6,1.2,-1.1,-0.6,0,1.4,2.9,-6.5,-12.8,-9.6,-5,-5.4,-6.2,-6.2,-5.6,-5,-0.8,-2.9,3.6,5.3,5.4,4.2,1.1,7.8,7.1,7.6,8.4,9,9,9.8,6.1,7.6,7,5.5,9.3,4.3,3.6,3.3,6.3,-2.3,-0.8,-1.5,-6.9,-6.9,-3.6,-3.9,-5.3,-5.9,-10.5,-12.7,-14.7,-13.2,-18.6,-19.3,-20.4,-23.9,-16.9,-22.1,-22.1,-20.6,-19.7,-19.3,-18.6,-14.7,-14.1,-11.4,-12.4,-11.1,-15.9,-13,-9.8,-4.6,-4.6,-1.9,1.2,3.7,7.6,9.1,3.7,-1.6,7.2,2.6,1.2,-1.2,-4.5,-16.3,-18.9,-18.9,-30.1,-24,-19.8,-9.8,0.5,6,10.2,11.6,18,17.3,14.2,8.8,5.4,3.6,3.6,7.9,12.2,14.6,8.4,5.9,4.3,4,-0.4,1.5,10.5,16.2,11.8,6.7,3.6,3.6,-1.6,1.9,1.5,2.2,2.8,5.4,7.7,8.8,4.8,0.5,-0.6,-1.9,-5.6,-5.6,-5.6,-7.9,-14.6,-20.7,-22.9,-24.1,-28,-26.6,-26.2,-22.5,-26.4,-20.6,-6.2,-4.3,0.3,0.3,1.2,3.6,4.8,3.9,8.8,9.1,7,6.3,8,7.4,4.6,6.5,8.4,8.4,9.3,3.4,1.5,6.3,13.4,10.8,9.1,8,5.6,2.4,5.7,5.7,9.7,9.3,9.3,12.2,12.2,10.8,11.1,13,13.2,14.1,14.2,17,16.6,19.3,12.5,7.4,6.3,6.3,4.3,-0.9,-2.8,-7.4,-6.3,-5.9,-7.9,-13.2,-22.4,-18,-14.6,-14.1,-11.6,-8.6,-8.6,-9.4,-9.6,-9.5,-26.1,-22.1,-14.9,-8.2,-3.1,2.6,9.6,18.7,15,11.1,11.5,11.5,17.4,11.4,18.7,7,-1.5,-4.6,-10.8,-13.6,-11.3,-8.2,-5,1.3,-1.2,-22.7,-22.7,-8.6,-14.7,1.3,-1.1,7.5,5.3,5.3,5.1,5,3.1,-10.1,-20.6,-21.2,-21.2,-21.2,-17.6,-17,-18.9,-18.9,-19.9,-13.5,-6.1,-10.7,-14.9,-13.5,-16.9,-0.5,8.1,8.1,-4.6,-15.6,-22.4,-4.3,-6.3,-4.8,-2.1,-12.8,-21.2,-21.8,-22.7,-18.6,-16.4,6.6,6.6,0,3.2,1.1,-6.5,-15.2,-13.5,-11.6,-9.6,-9.6,-1.5,-8.2,-16.9,-17.5,-25.9,-25.9,-26.6,-33.7,-37.6,-40.1,-41.6,-28.8,-10.1,-7.8,-5.4,5.1,8.1,3,9.3,14,14,4.2,3.6,2.6,1.7,-0.3,-6.3,8.1,-7.6,-7.9,-10.6,-13.3,-15.6,3.2,-1.4,-1.4,-8,-17.7,-13.2,-12.5,-13.9,-15.2,-18.6,-21.7,-23.3,-14.1,-9,-3.9,-2.2,-2.2,10,7.7,9.7,1.1,-5.3,-4.8,-1.4,9.1,10.9,1.5,-6.3,-4.8,-3.6,-3.3,-3.3,-4.8,-5.4,-1.2,1.7,2,5.6,7.4,8.4,4,4.2,12.8,-6.9,-4,-2.2,-2.2,-0.5,-0.5,-1.4,-2.6,4.5,-0.7,15.9,18.7,19,19,17,11.1,5.6,3.3,3.3,7.6,12.4,13.4,12.7,14.4,15.5,16.2,3.7,-2.3,-5.4,-5.3,-2.2,1.3,0,0,-1.6,1.3,-7.8,-5,-4.8,-7.7,-10.9,7.8,1.2,-0.6,-5.9,-5.3,-3.4,-1.5,-1.5,-7.1,-10.5,-15.2,-17.4,-14.2,-13.9,-12.1,-8,-7.6,-13.4,-1.4,5.3,5.7,5.7,3.3,2,-0.7,2.6,5,-2,2.3,0.9,-2.6,-4.1,1.4,2.2,-6,-8.5,-8.5,3.7,1.5,6.3,-2.9,0.3,-1.9,-12.8]

# Actual ISM prints (New Orders, Prices Paid). 2021-25 from DBnomics' ISM mirror; 2026 from ISM releases as reported
# by ISM / Haver / TheStreet / Trading Economics. Add new months here, or pass --ism CSV, when the live sources lapse.
ISM_ACTUAL = {
    "2021-01": (61.1, 82.1), "2021-02": (64.8, 86), "2021-03": (68, 85.6), "2021-04": (64.3, 89.6),
    "2021-05": (67, 88), "2021-06": (66, 92.1), "2021-07": (64.9, 85.7), "2021-08": (66.7, 79.4),
    "2021-09": (66.7, 81.2), "2021-10": (60.6, 85.7), "2021-11": (61.4, 82.4), "2021-12": (61, 68.2),
    "2022-01": (57.9, 76.1), "2022-02": (61.7, 75.6), "2022-03": (53.8, 87.1), "2022-04": (53.5, 84.6),
    "2022-05": (55.1, 82.2), "2022-06": (49.2, 78.5), "2022-07": (48, 60), "2022-08": (51.3, 52.5),
    "2022-09": (47.1, 51.7), "2022-10": (48.2, 46.6), "2022-11": (46.8, 43), "2022-12": (45.1, 39.4),
    "2023-01": (42.5, 44.5), "2023-02": (47, 51.3), "2023-03": (44.3, 49.2), "2023-04": (45.7, 53.2),
    "2023-05": (42.6, 44.2), "2023-06": (45.6, 41.8), "2023-07": (47.3, 42.6), "2023-08": (46.8, 48.4),
    "2023-09": (49.2, 43.8), "2023-10": (46.2, 45.1), "2023-11": (47.8, 49.9), "2023-12": (47, 45.2),
    "2024-01": (52.5, 52.9), "2024-02": (49.2, 52.5), "2024-03": (51.4, 55.8), "2024-04": (49.1, 60.9),
    "2024-05": (45.4, 57), "2024-06": (49.3, 52.1), "2024-07": (47.4, 52.9), "2024-08": (44.6, 54),
    "2024-09": (46.1, 48.3), "2024-10": (47.9, 54.8), "2024-11": (50.3, 50.3), "2024-12": (52.1, 52.5),
    "2025-01": (55.1, 54.9), "2025-02": (48.6, 62.4), "2025-03": (45.2, 69.4), "2025-04": (47.2, 69.8),
    "2025-05": (47.6, 69.4), "2025-06": (46.4, 69.7), "2025-07": (47.1, 64.8), "2025-08": (51.4, 63.7),
    "2025-09": (48.9, 61.9), "2025-10": (49.4, 58), "2025-11": (47.4, 58.5), "2025-12": (47.7, 58.5),
    "2026-01": (None, 59.0), "2026-02": (55.8, 70.5), "2026-03": (53.5, 78.3), "2026-04": (54.1, 84.6),
    "2026-05": (56.8, 82.1), "2026-06": (56.0, 73.0), "2026-07": (56.7, 71.1), "2026-08": (53.7, 71.1),
    "2026-09": (55.3, 77.9), "2026-10": (55.5, 75.2),
}


def fred(sid):
    if not FRED_KEY:
        raise RuntimeError("FRED_API_KEY is not set. Add it as a repository secret (or export it locally); see README.")
    for attempt in range(3):
        try:
            j = requests.get("https://api.stlouisfed.org/fred/series/observations",
                             params=dict(series_id=sid, api_key=FRED_KEY, file_type="json"), timeout=60).json()
            return pd.Series({pd.Timestamp(o["date"]): float(o["value"]) for o in j["observations"] if o["value"] not in (".", "")}, name=sid)
        except Exception as e:
            err = e; time.sleep(3)
    raise RuntimeError(f"FRED {sid}: {err!r}")


def dbnomics_ism():
    out = {}
    for ds in ("neword", "prices"):
        r = requests.get(f"https://api.db.nomics.world/v22/series/ISM/{ds}/in?observations=1", timeout=40).json()["series"]["docs"][0]
        out[ds] = pd.Series([np.nan if v in (None, "NA") else float(v) for v in r["value"]], index=pd.to_datetime([p + "-01" for p in r["period"]])).dropna()
    return out


MONTHS = {m: i + 1 for i, m in enumerate("January February March April May June July August September October November December".split())}


def te_latest():
    """Latest and previous ISM New Orders / Prices Paid prints from Trading Economics' public page summary."""
    out = {}
    for key, slug in (("no", "ism-manufacturing-new-orders"), ("pr", "ism-manufacturing-prices")):
        t = requests.get("https://tradingeconomics.com/united-states/" + slug, headers=UA_WEB, timeout=30).text
        m = re.search(r'name="description" content="([^"]+)"', t)
        d = html.unescape(m.group(1))
        m = re.search(r"to ([\d.]+) points in (\w+) from ([\d.]+) points in (\w+) of (\d{4})", d)
        if not m:
            continue
        v1, m1, v0, m0, y0 = float(m.group(1)), MONTHS[m.group(2)], float(m.group(3)), MONTHS[m.group(4)], int(m.group(5))
        y1 = y0 + 1 if m1 < m0 else y0
        out[key] = {f"{y1}-{m1:02d}": v1, f"{y0}-{m0:02d}": v0}
    return out


def get_macro(ism_csv=None):
    os.makedirs(CACHE, exist_ok=True)
    fp = os.path.join(CACHE, "macro.pkl")
    if os.path.exists(fp) and time.time() - os.path.getmtime(fp) < 18 * 3600 and ism_csv is None:
        return pickle.load(open(fp, "rb"))
    notes = {}
    ppi = fred("PPIACO"); cpi = fred("CPIAUCNS"); oecd = fred("BSOITE02USM460S")
    no_oecd = (50 + oecd / 2).rename("no_oecd")                  # OECD orders-inflow balance -> ISM-style index
    idx = pd.date_range("1948-01-01", pd.Timestamp(dt.datetime.now(dt.timezone.utc).date()).replace(day=1), freq="MS")
    m = pd.DataFrame(index=idx, columns=["no", "pr", "spread", "src"], dtype=object)
    dig = pd.Series(SPREAD_DIGITISED, index=pd.date_range("1948-01-01", periods=len(SPREAD_DIGITISED), freq="MS"))
    m.loc[dig.index, "spread"] = dig.values; m.loc[dig.index, "src"] = "digitised"
    m.loc[dig.index, "no"] = no_oecd.reindex(dig.index).values
    m.loc[dig.index, "pr"] = (no_oecd.reindex(dig.index) - dig).clip(0, 100).values
    for k, (no, pr) in ISM_ACTUAL.items():
        d = pd.Timestamp(k + "-01")
        if no is None:
            no = float(no_oecd.get(d, np.nan)); src = "actual prices, OECD-implied orders"
        else:
            src = "actual"
        m.loc[d, ["no", "pr", "spread", "src"]] = [no, pr, no - pr, src]
    live = {}
    try:
        db = dbnomics_ism()
        for d in db["neword"].index.intersection(db["prices"].index):
            live[d] = (float(db["neword"][d]), float(db["prices"][d]), "actual (DBnomics)")
        notes["dbnomics_last"] = str(max(db["neword"].index).date())[:7]
    except Exception as e:
        notes["dbnomics_err"] = repr(e)[:100]
    try:
        te = te_latest()
        for k in sorted(set(te.get("no", {})) & set(te.get("pr", {}))):
            live[pd.Timestamp(k + "-01")] = (te["no"][k], te["pr"][k], "actual (Trading Economics)")
        notes["te_last"] = max(te.get("pr", {"": 0}))
    except Exception as e:
        notes["te_err"] = repr(e)[:100]
    last_embedded = pd.Timestamp(max(ISM_ACTUAL) + "-01")
    for d, (no, pr, src) in live.items():
        if d > last_embedded and d in m.index:
            m.loc[d, ["no", "pr", "spread", "src"]] = [no, pr, no - pr, src]
    if ism_csv:   # user-supplied history: columns date,new_orders,prices_paid
        u = pd.read_csv(ism_csv, parse_dates=[0]); u.columns = ["date", "no", "pr"][:len(u.columns)]
        for _, r in u.dropna().iterrows():
            d = pd.Timestamp(r["date"]).replace(day=1)
            if d in m.index:
                m.loc[d, ["no", "pr", "spread", "src"]] = [float(r["no"]), float(r["pr"]), float(r["no"]) - float(r["pr"]), "user file"]
        notes["ism_csv"] = os.path.basename(ism_csv)
    m = m[m["spread"].notna()]
    for c in ("no", "pr", "spread"):
        m[c] = pd.to_numeric(m[c], errors="coerce")
    m["ppi_yoy"] = (ppi / ppi.shift(12) - 1).reindex(m.index) * 100
    m["cpi_yoy"] = (cpi / cpi.shift(12) - 1).reindex(m.index) * 100
    long = pd.DataFrame({"ppi_yoy": (ppi / ppi.shift(12) - 1) * 100, "cpi_yoy": (cpi / cpi.shift(12) - 1) * 100}).dropna(how="all")
    notes.update(ppi_last=str(ppi.index[-1].date())[:7], cpi_last=str(cpi.index[-1].date())[:7], ism_last=str(m.index[-1].date())[:7],
                 oecd_last=str(oecd.index[-1].date())[:7])
    out = {"ism": m, "long": long, "notes": notes}
    pickle.dump(out, open(fp, "wb"))
    return out


# ============================================================ SIGNALS: the fourteen buy-signal triggers
# Stage 3a: the buy-signal triggers. Every function returns a boolean daily Series (True on the trigger day).
# ---------- helpers ----------
def cross_up(s, lvl):
    return ((s > lvl) & (s.shift(1) <= lvl)).fillna(False)


def armed_cycle(s, arm_below, fire_above):
    """Trigger the first time s >= fire_above after s has been <= arm_below (no time limit); then disarm."""
    v = s.values; out = np.zeros(len(v), bool); armed = False
    for i, x in enumerate(v):
        if x != x:
            continue
        if x <= arm_below:
            armed = True
        elif armed and x >= fire_above:
            out[i] = True; armed = False
    return pd.Series(out, index=s.index)


def weekly_bars(spx):
    """Completed Mon-Fri weeks only, stamped on the week's last trading day."""
    d = spx[["High", "Low", "Close"]].dropna()
    wk = d.index.to_period("W-FRI")
    g = d.groupby(wk)
    w = pd.DataFrame({"High": g["High"].max(), "Low": g["Low"].min(), "Close": g["Close"].last(), "date": g["Close"].apply(lambda x: x.index[-1])})
    last = d.index[-1]
    if last.dayofweek != 4 and (pd.Timestamp.now().normalize() - last).days < 4:   # current week still open
        w = w.iloc[:-1]
    return w.set_index("date")


def to_daily(trig_w, daily_index):
    out = pd.Series(False, index=daily_index)
    d = trig_w.index[trig_w.values]
    out.loc[out.index.intersection(d)] = True
    return out


def rsi_wilder(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + up / dn)


def macd(c, f=12, s=26, sig=9):
    m = c.ewm(span=f, adjust=False).mean() - c.ewm(span=s, adjust=False).mean()
    return m, m.ewm(span=sig, adjust=False).mean()


def macd_buy(c):
    m, sg = macd(c)
    return ((m > sg) & (m.shift(1) <= sg.shift(1)) & (m < 0)).fillna(False), m, sg


def td_sequential_buy(h, l, c):
    """DeMark TD Sequential, buy side. Returns (setup9, countdown13, setup_count, countdown_count).
    Setup: bearish price flip, then nine straight closes below the close four bars earlier.
    Countdown: from bar 9 of the setup, closes <= the low two bars earlier (need not be consecutive); bar 13 must also
    have a low <= the close of countdown bar 8. Cancelled by a completed sell setup or a close above the setup's
    highest high (TDST); a fresh buy setup restarts it (recycle)."""
    H, L, C = h.values, l.values, c.values; n = len(C)
    s9 = np.zeros(n, bool); c13 = np.zeros(n, bool); sc = np.zeros(n, int); cc = np.zeros(n, int)
    bs = ss = 0; active = False; cnt = 0; bar8 = np.nan; tdst = np.nan; setup_hi = -np.inf
    for i in range(4, n):
        if C[i] < C[i - 4]:
            if bs == 0 and not (C[i - 1] >= C[i - 5] if i >= 5 else True):
                bs = 0                                   # no price flip -> not a new setup
            else:
                bs += 1
            if bs == 1: setup_hi = H[i]
            else: setup_hi = max(setup_hi, H[i])
            ss = 0
        elif C[i] > C[i - 4]:
            ss = ss + 1 if (ss > 0 or (i >= 5 and C[i - 1] <= C[i - 5])) else 0
            bs = 0
        else:
            bs = ss = 0
        sc[i] = bs if bs <= 9 else 0
        if ss == 9 and active:
            active = False; cnt = 0
        if active and C[i] > tdst:
            active = False; cnt = 0
        if bs == 9:
            s9[i] = True; active = True; cnt = 0; bar8 = np.nan; tdst = setup_hi
        if active and i >= 2 and C[i] <= L[i - 2]:
            if cnt < 12:
                cnt += 1
                if cnt == 8: bar8 = C[i]
            elif L[i] <= bar8:
                cnt = 13; c13[i] = True; active = False
        cc[i] = cnt if active or c13[i] else 0
        if c13[i]: cnt = 0
    f = lambda a: pd.Series(a, index=c.index)
    return f(s9), f(c13), f(sc), f(cc)


def monthly_to_daily(m, daily_index, lag_days):
    """Monthly series (stamped first of month) -> daily, available `lag_days` after month end."""
    avail = (m.index + pd.offsets.MonthEnd(0)) + pd.Timedelta(days=lag_days)
    s = pd.Series(m.values, index=avail)
    return s.reindex(daily_index.union(s.index)).ffill().reindex(daily_index)


def adv_macd_buy(c, ma_n=50, slope_n=10, div_days=90):
    """Advanced MACD buy: MACD(12,26,9) crosses above its signal while below zero, AND either
    (a) a positive divergence: this dip's price low undercuts the previous dip's low while the MACD low is higher
        (previous dip had MACD below zero and ended no more than div_days earlier), or
    (b) the 50-day moving average is rising (above its level slope_n sessions earlier).
    A 'dip' is a run of sessions with MACD below its signal line. Returns (trigger, div_leg, ma_leg, macd, signal, ma)."""
    m, s = macd(c, 12, 26, 9)
    x = (m > s) & (m.shift(1) <= s.shift(1)) & (m < 0)
    ma = c.rolling(ma_n).mean(); rising = (ma > ma.shift(slope_n)).fillna(False)
    seg = (m < s).astype(int); sid = (seg.diff() == 1).cumsum().where(seg == 1)
    g = pd.DataFrame({"m": m, "p": c, "sid": sid}).dropna().groupby("sid")
    L = pd.DataFrame({"m": g["m"].min(), "p": g["p"].min(), "end": g["m"].apply(lambda z: z.index[-1])}).reset_index(drop=True)
    div = pd.Series(False, index=c.index)
    for k in range(1, len(L)):
        cur, prev = L.iloc[k], L.iloc[k - 1]
        if (cur.end - prev.end).days <= div_days and prev.m < 0 and cur.p < prev.p and cur.m > prev.m:
            j = c.index.searchsorted(cur.end) + 1
            if j < len(c):
                div.iloc[j] = True
    return (x & (div | rising)).fillna(False), (x & div).fillna(False), (x & rising).fillna(False), m, s, ma


def spring_days(b, look=13):
    """Spring Days on weekly closes: %>50DMA at 80%+ and %>200DMA at 40%+ hold for two consecutive weekly closes,
    with each line having been below its threshold within the prior `look` weeks (it 'moves back above')."""
    bw = b[["p50", "p200"]].dropna(); wk = bw.index.to_period("W-FRI")
    W = bw.groupby(wk).last(); W.index = bw.index.to_series().groupby(wk).last().values
    both = (W.p50 >= 80) & (W.p200 >= 40); hold = both & both.shift(1, fill_value=False)
    was = lambda col, lv: (W[col] < lv).astype(float).rolling(look, min_periods=1).max().shift(2).fillna(0).astype(bool)
    t = hold & ~hold.shift(1, fill_value=False) & was("p50", 80) & was("p200", 40)
    return t, W


# ---------- the signal book ----------
def build_signals(spx, b, vix, vix3m, macro):
    """spx: daily OHLC from 1927; b: constituent breadth frame (1998+). Returns dict key -> trigger Series on spx.index,
    plus a dict of indicator series for charts and the current read."""
    D = spx.index; c = spx["Close"]
    T, I = {}, {}
    w = weekly_bars(spx); wc = w["Close"]
    # 1. 4/40 momentum
    r440 = wc.rolling(4).mean() / wc.rolling(40).mean()
    T["m440"] = to_daily(armed_cycle(r440, 0.95, 1.03), D); I["r440"] = r440
    # 2. weekly RSI thrust
    rsi = rsi_wilder(wc, 14)
    T["rsi"] = to_daily(armed_cycle(rsi, 42.9999, 59.99), D); I["rsi_w"] = rsi
    # 9. MACD buys
    mb_w, m_w, s_w = macd_buy(wc); mb_d, m_d, s_d = macd_buy(c)
    T["macd_w"] = to_daily(mb_w, D); T["macd_d"] = mb_d
    I.update(macd_w=m_w, macdsig_w=s_w, macd_d=m_d, macdsig_d=s_d)
    am, am_div, am_ma, _, _, ma50 = adv_macd_buy(c)
    T["amacd"] = am; I.update(amacd_div=am_div, amacd_ma=am_ma, ma50=ma50, ma50_up=(ma50 > ma50.shift(10)))
    # 10. DeMark TD Sequential 9 / 13 buys (true highs/lows exist from 1962)
    ohl = spx[spx.index >= "1962-01-02"]
    s9d, c13d, scd, ccd = td_sequential_buy(ohl["High"], ohl["Low"], ohl["Close"])
    w62 = w[w.index >= "1962-01-02"]
    s9w, c13w, scw, ccw = td_sequential_buy(w62["High"], w62["Low"], w62["Close"])
    T["td_d"] = (s9d | c13d).reindex(D, fill_value=False); T["td_w"] = to_daily(s9w | c13w, D)
    I.update(td_s9_d=s9d, td_c13_d=c13d, td_sc_d=scd, td_cc_d=ccd, td_s9_w=s9w, td_c13_w=c13w, td_sc_w=scw, td_cc_w=ccw)
    # breadth legs (constituents)
    bb = b.reindex(D)
    msi = bb["msi"]
    T["msi"] = armed_cycle(msi, 100, 1000)                       # 3. Summation Index <100 -> >1000
    os_ = ((bb["p50"] <= 10) & (bb["p200"] <= 20)).fillna(False)
    T["oversold"] = os_ & ~os_.shift(1, fill_value=False)        # 4. long-term oversold
    ts = (vix["Close"] / vix3m["Close"]).reindex(D)
    obos = bb["p50"] / bb["p150"]
    c1 = (ts > 1).rolling(5, min_periods=1).max().fillna(0).astype(bool)
    c3 = (obos < 0.5).rolling(5, min_periods=1).max().fillna(0).astype(bool)
    tri = c1 & c3 & ts.notna()
    T["trifecta"] = tri & ~tri.shift(1, fill_value=False)        # 5. Humble Student Trifecta (two replicable legs; TRIN proxy shown, not required)
    trin_ok = (bb["trin"] > 2).rolling(5, min_periods=1).max().fillna(0).astype(bool)
    I.update(ts=ts, obos=obos, trin=bb["trin"], trin_ok=trin_ok)
    bpi = bb["bpi"]
    T["bpi"] = ((bpi < 25) & (bpi.shift(1) >= 25)).fillna(False)  # 6. Bullish Percent below 25
    ema10 = bb["adv_share"].ewm(span=10, adjust=False).mean()
    T["zbt"] = ((ema10 > 0.615) & (ema10.shift(1) <= 0.615) & (ema10.rolling(10).min() < 0.40)).fillna(False)   # 7. Zweig
    a10 = bb["adv"].rolling(10).sum() / bb["dec"].rolling(10).sum()
    T["ad10"] = cross_up(a10, 1.97)                               # 8. 10-day A/D thrust (stand-in for "Investec")
    I.update(ema10=ema10, a10=a10)
    sp_t, spW = spring_days(b)
    T["spring"] = to_daily(sp_t, D); I.update(spring_p50=spW["p50"], spring_p200=spW["p200"])
    # 11/12. macro models
    ism = macro["ism"]; lg = macro["long"]
    ppi = monthly_to_daily(lg["ppi_yoy"].dropna(), D, 16); cpi = monthly_to_daily(lg["cpi_yoy"].dropna(), D, 16)
    pr = monthly_to_daily(ism["pr"].dropna(), D, 3); sp = monthly_to_daily(ism["spread"].dropna(), D, 3)
    bad_ppi = (ppi >= 9) | (ppi <= -12); bad_cpi = (cpi >= 4) | (cpi <= -3); bad_pr = (pr > 85).fillna(False)
    infl_ok = ~(bad_ppi | bad_cpi | bad_pr) & ppi.notna()
    T["infl"] = infl_ok & ~infl_ok.shift(1, fill_value=True)     # model turns favourable
    T["stag"] = armed_cycle(sp, -25, 0)                           # spread recovers from below -25 to above 0
    I.update(ppi=ppi, cpi=cpi, pr=pr, spread=sp, infl_ok=infl_ok, bad_ppi=bad_ppi, bad_cpi=bad_cpi, bad_pr=bad_pr,
             stag_zone=pd.Series(np.where(sp > 0, 1, np.where(sp < -25, -1, 0)), index=D).where(sp.notna()))
    return T, I


BOOK = [  # key, name, short, family, rule, source, history-start, default hold (sessions)
    ("m440", "4/40 Momentum", "4/40", "Price momentum", "4-week ÷ 40-week average of weekly closes falls to 0.95 or lower, then recovers to 1.03 or higher", "SentimenTrader (TCTM)", "1950", 63),
    ("rsi", "RSI Thrust", "RSI", "Price momentum", "14-week Wilder RSI drops below 43, then crosses back above 59.99", "SentimenTrader (Kaeppel)", "1950", 63),
    ("amacd", "Advanced MACD Buy", "Adv MACD", "Price momentum", "Daily MACD(12,26,9) crosses above its signal while below zero, after a positive divergence (lower price low, higher MACD low inside 90 days) or with the 50-day average rising over 10 sessions", "CMT curriculum", "1950", 63),
    ("msi", "McClellan Summation Thrust", "Summation", "Breadth thrust", "Ratio-adjusted Summation Index cycles from below +100 to above +1,000", "SentimenTrader", "2000", 63),
    ("zbt", "Zweig Breadth Thrust", "Zweig", "Breadth thrust", "10-day EMA of advancers ÷ (advancers + decliners) from under 0.40 to over 0.615 inside ten sessions", "Martin Zweig", "2000", 63),
    ("ad10", "Investec Breadth Thrust*", "10d A/D", "Breadth thrust", "10-day advances ÷ 10-day declines crosses above 1.97 (stand-in rule, see notes)", "Stand-in: Deemer breakaway momentum", "2000", 63),
    ("spring", "Spring Days", "Spring", "Breadth thrust", "Weekly %>50DMA back above 80% and %>200DMA back above 40% (each below its line within the prior 13 weeks), both holding for two weekly closes", "CMT curriculum", "2000", 63),
    ("oversold", "Long-Term Oversold", "LT oversold", "Washout", "%>50DMA at 10% or lower while %>200DMA is at 20% or lower", "StockCharts oversold indicator", "2000", 63),
    ("trifecta", "Humble Student Trifecta", "Trifecta", "Washout", "VIX ÷ 3-month VIX above 1 and %>50DMA ÷ %>150DMA below 0.5 inside five sessions; the TRIN-above-2 leg is shown but not required (no free NYSE TRIN)", "Cam Hui", "2006", 63),
    ("bpi", "Bullish % Index", "Bullish %", "Washout", "P&F Bullish Percent Index closes below 25", "StockCharts $BPSPX", "2000", 63),
    ("td_w", "DeMark 9-13 · weekly", "TD wk", "Exhaustion", "TD Sequential buy setup 9 or buy countdown 13 completes on weekly bars", "Tom DeMark", "1962", 63),
    ("infl", "Inflation Model", "Inflation", "Macro regime", "Model turns favourable: PPI year-on-year back inside −12% to +9%, CPI inside −3% to +4% and ISM Prices Paid at 85 or lower", "SentimenTrader (Kaeppel) + workbook rule", "1950", 63),
    ("stag", "Stagflation Model", "Stagflation", "Macro regime", "ISM New Orders minus Prices Paid recovers from below −25 to above 0", "SentimenTrader", "1950", 63),
]


# ============================================================ MODEL: aggregator, evidence, payload
# Stage 3b: aggregator, forward-return evidence and the JSON payload.
EVID_START = "2000-01-03"
HZ = {"1m": 21, "3m": 63, "6m": 126, "12m": 252}
SPLIT = "2013-01-01"
HOLD = 63
BULL_AT = 5
WIN = {"1950": "1950-01-03", "1962": "1962-07-02", "2000": "2000-01-03", "2006": "2006-08-01"}


def hold(trig, n):
    return trig.astype(float).rolling(n, min_periods=1).max().fillna(0).astype(int)


def first_of_cluster(sig, gap=21):
    d = np.flatnonzero(sig.values); keep, last = [], -10 ** 9
    for i in d:
        if i - last > gap:
            keep.append(i)
        last = i
    return np.array(keep, dtype=int)


def r1(x, nd=1):
    return None if x is None or (isinstance(x, float) and np.isnan(x)) else round(float(x), nd)


def build_model(spx, b, vix, vix3m, macro, notes, n_tickers):
    T, I = build_signals(spx, b, vix, vix3m, macro)
    c = spx["Close"]; D = c.index; n = len(c); cv = c.values
    fwd = pd.DataFrame({k: c.shift(-h) / c - 1 for k, h in HZ.items()})
    dd6 = np.array([cv[i:i + 127].min() / cv[i] - 1 if i + 126 < n else np.nan for i in range(n)])
    rng = np.random.default_rng(7)
    pos = lambda d: int(D.searchsorted(pd.Timestamp(d)))
    ev0 = pos(EVID_START); in_ev = np.arange(n) >= ev0
    h1 = np.asarray(D < SPLIT); h2 = ~h1

    def base_stats(i0, mask=None):
        m = np.arange(n) >= i0 if mask is None else mask
        r = {}
        for k in HZ:
            v = fwd[k].values[m]; v = v[~np.isnan(v)]
            r[k] = r1(v.mean() * 100); r["w" + k] = r1((v > 0).mean() * 100, 0)
        return r

    def placebo(idx, i0, k):
        arr = fwd[k].values[i0:]; ok = ~np.isnan(arr); m_ = int(ok.sum())
        ii = idx - i0; ii = ii[(ii >= 0) & (ii < m_)]
        if len(ii) < 3:
            return None
        obs = arr[ii].mean()
        sims = np.array([arr[(ii + s) % m_].mean() for s in rng.integers(0, m_, 3000)])
        return round(float((sims >= obs).mean()), 3)

    def event_stats(idx, i0):
        idx = idx[idx >= i0]
        r = {"n": int(len(idx))}
        for k in HZ:
            v = fwd[k].values[idx]; v = v[~np.isnan(v)]
            r[k] = r1(v.mean() * 100) if len(v) else None
            r["w" + k] = r1((v > 0).mean() * 100, 0) if len(v) else None
            r["p" + k] = placebo(idx, i0, k)
        d = dd6[idx]; d = d[~np.isnan(d)]
        r["dd_worst"] = r1(d.min() * 100) if len(d) else None
        mid = D[i0] + (D[-1] - D[i0]) / 2
        r["mid"] = int(mid.year)
        for lab, m in (("a", D[idx] < mid), ("b", D[idx] >= mid)):
            for k in ("6m", "12m"):
                v = fwd[k].values[idx[m]]; v = v[~np.isnan(v)]
                r[k + "_" + lab] = r1(v.mean() * 100) if len(v) else None
            bm = (np.arange(n) >= i0) & ((D < mid) if lab == "a" else (D >= mid))
            bs = base_stats(None, bm); r["b6m_" + lab], r["b12m_" + lab] = bs["6m"], bs["12m"]
            r["n_" + lab] = int(m.sum())
        r["last"] = str(D[idx[-1]].date()) if len(idx) else None
        return r

    def state_stats(mask, cond=None):
        m = mask & in_ev if cond is None else mask & in_ev & cond
        den = in_ev.sum() if cond is None else (in_ev & cond).sum()
        r = {"pct": r1(m.sum() / den * 100), "days": int(m.sum())}
        for k in HZ:
            v = fwd[k].values[m]; v = v[~np.isnan(v)]
            r[k] = r1(v.mean() * 100) if len(v) else None
            r["w" + k] = r1((v > 0).mean() * 100, 0) if len(v) else None
        return r

    bases = {w: base_stats(pos(d)) for w, d in WIN.items()}

    # ---------------- per-leg evidence ----------------
    legs = []; act = {}
    for key, name, short, fam, rule, src, st, hd in BOOK:
        t = T[key]; i0 = pos(WIN[st]); idx = first_of_cluster(t)
        e = event_stats(idx, i0); bs = bases[st]
        beats = all(e[k] is not None and e[k] > bs[k] for k in ("6m", "12m"))
        halves = all(e[f"{k}_{h}"] is not None and e[f"{k}_{h}"] > e[f"b{k}_{h}"] for k in ("12m",) for h in ("a", "b"))
        sig = any(e["p" + k] is not None and e["p" + k] < 0.10 for k in ("6m", "12m"))
        grade = "pass" if (beats and halves and sig and e["n"] >= 10) else ("weak" if beats and (halves or sig) else "none")
        a = hold(t, HOLD)
        if key == "infl":   # a macro leg switches off early if its model turns unfavourable again
            a = a.where(I["infl_ok"], 0).astype(int)
        if key == "stag":
            a = a.where(~(I["stag_zone"] == -1), 0).astype(int)
        act[key] = a
        raw = np.flatnonzero(t.values); last_i = int(raw[-1]) if len(raw) else None
        legs.append({"key": key, "name": name, "short": short, "family": fam, "rule": rule, "src": src, "since": st, "ev": e, "base": bs,
                     "beats": beats, "halves": halves, "sig": sig, "grade": grade,
                     "active": int(a.iloc[-1]), "left": (HOLD - (n - 1 - last_i)) if (last_i is not None and n - 1 - last_i < HOLD and int(a.iloc[-1])) else 0,
                     "last": str(D[last_i].date()) if last_i is not None else None,
                     "n_raw": int(((np.arange(n) >= i0) & t.values).sum())})
    core = [l["key"] for l in legs if l["grade"] == "pass"]
    keys = [l["key"] for l in legs]

    # ---------------- aggregator ----------------
    def agg(ks, thr):
        cnt = sum(act[k] for k in ks)
        mx = cnt.rolling(21, min_periods=1).max()
        st = pd.Series(np.where(mx >= thr, 1, np.where(mx == 0, -1, 0)), index=D)
        out = {"thr": thr, "n": len(ks)}
        for v, lab in ((1, "bull"), (0, "neutral"), (-1, "none")):
            m = (st == v).values
            out[lab] = {"all": state_stats(m), "h1": state_stats(m, h1), "h2": state_stats(m, h2)}
        out["bycount"] = [dict(state_stats((cnt == k).values), count=int(k)) for k in sorted(cnt[in_ev].unique())]
        grp = []
        for lab, lo, hi in (("0", 0, 0), (f"1–{thr - 1}" if thr > 2 else "1", 1, thr - 1), (f"{thr}–{thr + 2}", thr, thr + 2), (f"{thr + 3}+", thr + 3, 99)):
            m = ((cnt >= lo) & (cnt <= hi)).values
            grp.append({"label": lab, "all": state_stats(m), "h1": state_stats(m, h1), "h2": state_stats(m, h2)})
        out["groups"] = grp
        return cnt, st, out

    cnt_all, st_all, ev_all = agg(keys, BULL_AT)
    thr_core = 2
    cnt_core, st_core, ev_core = agg(core, thr_core)

    # ---------------- macro regimes ----------------
    io = I["infl_ok"].values; sz = I["stag_zone"]; dret = c.pct_change().shift(-1).values
    i50 = pos(WIN["1950"]); m50 = np.arange(n) >= i50

    def regime(mask, era):
        m = mask & era & ~np.isnan(dret)
        r = {"pct": r1(m.sum() / (era & ~np.isnan(dret)).sum() * 100), "ann": r1(((1 + dret[m]).prod() ** (252 / max(1, m.sum())) - 1) * 100)}
        for k in ("3m", "12m"):
            v = fwd[k].values[mask & era]; v = v[~np.isnan(v)]
            r[k] = r1(v.mean() * 100) if len(v) else None
        return r

    szv = sz.values
    regimes = []
    for lab, mask in (("Inflation model favourable", io), ("Inflation model unfavourable", ~io),
                      ("Stagflation spread above 0", szv == 1), ("Stagflation spread −25 to 0", szv == 0), ("Stagflation spread below −25", szv == -1)):
        regimes.append({"label": lab, "long": regime(mask, m50), "recent": regime(mask, in_ev)})
    both_bad = (~io) | (szv == -1)
    macro_gate = {"bull_ok": state_stats((st_all == 1).values & ~both_bad), "bull_bad": state_stats((st_all == 1).values & both_bad),
                  "rest_ok": state_stats((st_all != 1).values & ~both_bad), "rest_bad": state_stats((st_all != 1).values & both_bad)}

    # ---------------- event log ----------------
    log = []
    nm = {l["key"]: l["name"] for l in legs}
    for key in keys:
        for i in first_of_cluster(T[key]):
            if i < ev0:
                continue
            log.append({"d": str(D[i].date()), "k": key, "s": nm[key], "px": r1(cv[i], 0),
                        "f1": r1(fwd["1m"].values[i] * 100), "f3": r1(fwd["3m"].values[i] * 100), "f6": r1(fwd["6m"].values[i] * 100), "f12": r1(fwd["12m"].values[i] * 100)})
    log.sort(key=lambda r: r["d"], reverse=True)

    # ---------------- current status per leg ----------------
    def last(s, nd=2):
        s = s.dropna(); return r1(s.iloc[-1], nd) if len(s) else None

    def armed(s, lo, hi):
        a = False
        for x in s.dropna().values:
            if x <= lo: a = True
            elif a and x >= hi: a = False
        return a

    bb = b.reindex(D)
    now_ism = macro["ism"].iloc[-1]; lg = macro["long"].dropna(how="all")
    wk_last = str(I["r440"].dropna().index[-1].date())
    sc_d, cc_d, sc_w, cc_w = int(I["td_sc_d"].iloc[-1]), int(I["td_cc_d"].iloc[-1]), int(I["td_sc_w"].iloc[-1]), int(I["td_cc_w"].iloc[-1])
    a440 = armed(I["r440"], 0.95, 1.03); arsi = armed(I["rsi_w"], 42.9999, 59.99); amsi = armed(bb["msi"], 100, 1000); astag = armed(I["spread"], -25, 0)
    ppi_n, cpi_n, pr_n = last(I["ppi"], 1), last(I["cpi"], 1), last(I["pr"], 1)
    bad = [t for t, f in ((f"PPI {ppi_n:+.1f}%", bool(I["bad_ppi"].iloc[-1])), (f"CPI {cpi_n:+.1f}%", bool(I["bad_cpi"].iloc[-1])), (f"Prices Paid {pr_n:.1f}", bool(I["bad_pr"].iloc[-1]))) if f]
    status = {
        "m440": f"Ratio {last(I['r440'], 3)}; " + ("armed, fires at 1.03" if a440 else "not armed, needs 0.95 or lower first"),
        "rsi": f"RSI {last(I['rsi_w'], 1)}; " + ("armed, fires above 59.99" if arsi else "not armed, needs a drop below 43 first"),
        "msi": f"Summation {last(bb['msi'], 0):.0f}; " + ("armed, fires above +1,000" if amsi else "not armed, needs a drop below +100 first"),
        "oversold": f"%>50DMA {last(bb['p50'], 0):.0f}%, %>200DMA {last(bb['p200'], 0):.0f}%; needs 10% and 20% or lower",
        "trifecta": f"VIX ÷ VIX3M {last(I['ts'], 2)}, %>50 ÷ %>150 {last(I['obos'], 2)}, TRIN proxy {last(I['trin'], 2)}",
        "bpi": f"Bullish % {last(bb['bpi'], 1)}; fires below 25",
        "zbt": f"10-day EMA {last(I['ema10'], 3)}; needs under 0.40 then over 0.615",
        "ad10": f"10-day A/D ratio {last(I['a10'], 2)}; fires above 1.97",
        "macd_w": f"MACD {last(I['macd_w'], 1)} vs signal {last(I['macdsig_w'], 1)}; " + ("below zero, a cross up would fire" if last(I['macd_w'], 1) < 0 else "above zero, no buy possible until it drops below"),
        "macd_d": f"MACD {last(I['macd_d'], 1)} vs signal {last(I['macdsig_d'], 1)}; " + ("below zero, a cross up would fire" if last(I['macd_d'], 1) < 0 else "above zero, no buy possible until it drops below"),
        "spring": f"Weekly %>50DMA {last(I['spring_p50'], 0):.0f}% (needs 80%), %>200DMA {last(I['spring_p200'], 0):.0f}% (needs 40%); both must come back above and hold two weekly closes",
        "amacd": f"MACD {last(I['macd_d'], 1)} vs signal {last(I['macdsig_d'], 1)}; " + ("below zero" if last(I['macd_d'], 1) < 0 else "above zero, no buy possible until it drops below") + "; 50-day average " + ("rising" if bool(I['ma50_up'].iloc[-1]) else "falling, needs a positive divergence"),
        "td_w": f"Buy setup count {sc_w} of 9" + (f", countdown {cc_w} of 13" if cc_w else ", no countdown running"),
        "td_d": f"Buy setup count {sc_d} of 9" + (f", countdown {cc_d} of 13" if cc_d else ", no countdown running"),
        "infl": ("Favourable" if bool(I["infl_ok"].iloc[-1]) else f"Unfavourable on {', '.join(bad)}") + f"; PPI {ppi_n:+.1f}%, CPI {cpi_n:+.1f}%, Prices Paid {pr_n:.1f}",
        "stag": f"Spread {last(I['spread'], 1):+.1f}; " + ("armed, fires above 0" if astag else "not armed, needs a drop below −25 first"),
    }
    arm = {"m440": a440, "rsi": arsi, "msi": amsi, "stag": astag}
    for l in legs:
        l["status"] = status[l["key"]]; l["armed"] = bool(arm.get(l["key"], False))

    # ---------------- payload series ----------------
    sl = slice(ev0, n); idx = D[sl]

    def arr(s, nd=1):
        return [None if (v is None or (isinstance(v, float) and np.isnan(v))) else (int(v) if nd == 0 else round(float(v), nd)) for v in np.asarray(s)[sl]]

    def since(series):
        v = series.values; i = len(v) - 1
        while i > 0 and v[i - 1] == v[-1]:
            i -= 1
        return str(series.index[i].date())

    # weekly-sampled indicator detail (last session of each week)
    wkk = idx.to_period("W-FRI"); wlast = pd.Series(np.arange(len(idx)), index=idx).groupby(wkk).last().values
    wd = [str(idx[i].date()) for i in wlast]
    dly = lambda s: pd.Series(s).reindex(D).ffill() if not isinstance(s, pd.Series) else s.reindex(D.union(s.index)).ffill().reindex(D)

    def wk(s, nd=2):
        v = dly(s).values[sl][wlast]
        return [None if np.isnan(x) else round(float(x), nd) for x in v.astype(float)]

    def trig_w(key):
        ii = np.flatnonzero(T[key].values[sl]); return sorted(set(int(np.searchsorted(wlast, i)) for i in ii))

    C_ = {"a": "#1F4E79", "b": "#70AD47", "c": "#A5A5A5", "d": "#4472C4"}
    detail = {
        "m440": {"series": [["4-week ÷ 40-week average", wk(I["r440"], 3), C_["a"]]], "refs": [[0.95, "Arms at 0.95"], [1.03, "Fires at 1.03"]]},
        "rsi": {"series": [["14-week RSI", wk(I["rsi_w"], 1), C_["a"]]], "refs": [[43, "Arms below 43"], [59.99, "Fires above 59.99"]]},
        "msi": {"series": [["Summation Index (ratio-adjusted)", wk(bb["msi"], 0), C_["a"]]], "refs": [[100, "Arms below +100"], [1000, "Fires above +1,000"]]},
        "oversold": {"series": [["% above 50DMA", wk(bb["p50"], 1), C_["d"]], ["% above 200DMA", wk(bb["p200"], 1), C_["a"]]], "refs": [[10, "50DMA line 10%"], [20, "200DMA line 20%"]]},
        "trifecta": {"series": [["VIX ÷ 3-month VIX", wk(I["ts"], 3), C_["a"]], ["%>50DMA ÷ %>150DMA", wk(I["obos"].clip(upper=2), 3), C_["d"]]], "refs": [[1.0, "Term structure inverts above 1"], [0.5, "Oversold below 0.5"]]},
        "bpi": {"series": [["Bullish Percent Index", wk(bb["bpi"], 1), C_["a"]]], "refs": [[25, "Fires below 25"]]},
        "zbt": {"series": [["10-day EMA of advancing share", wk(I["ema10"], 3), C_["a"]]], "refs": [[0.40, "Setup under 0.40"], [0.615, "Thrust over 0.615"]]},
        "ad10": {"series": [["10-day advances ÷ declines", wk(I["a10"], 2), C_["a"]]], "refs": [[1.97, "Fires above 1.97"]]},
        "spring": {"series": [["% above 50DMA (weekly close)", wk(I["spring_p50"], 1), C_["d"]], ["% above 200DMA (weekly close)", wk(I["spring_p200"], 1), C_["a"]]], "refs": [[80, "50DMA line 80%"], [40, "200DMA line 40%"]]},
        "amacd": {"series": [["Daily MACD", wk(I["macd_d"], 1), C_["a"]], ["Signal line", wk(I["macdsig_d"], 1), C_["d"]]], "refs": [[0, "Zero line"]]},
        "td_w": {"series": [["Buy setup count", wk(I["td_sc_w"], 0), C_["d"]], ["Buy countdown count", wk(I["td_cc_w"], 0), C_["a"]]], "refs": [[9, "Setup completes at 9"], [13, "Countdown completes at 13"]], "step": 1},
        "infl": {"series": [["PPI all commodities, % y/y", wk(I["ppi"], 1), C_["a"]], ["CPI, % y/y", wk(I["cpi"], 1), C_["d"]]], "refs": [[9, "PPI extreme at +9%"], [4, "CPI extreme at +4%"]], "step": 1},
        "stag": {"series": [["ISM New Orders − Prices Paid", wk(I["spread"], 1), C_["a"]]], "refs": [[0, "Favourable above 0"], [-25, "Stagflation zone below −25"]], "step": 1},
    }
    for k in detail:
        detail[k]["trig"] = trig_w(k)

    hi252 = c.rolling(252, min_periods=126).max()
    ism_tail = macro["ism"].tail(1)
    P = {
        "meta": {"asof": str(D[-1].date()), "built": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "evid_start": str(idx[0].date()),
                 "split": SPLIT[:4], "hold": HOLD, "bull_at": BULL_AT, "core_at": thr_core, "n_legs": len(keys), "core": core, "wk_last": wk_last,
                 "members_last": int(b["n"].iloc[-1]), "tickers_priced": int(n_tickers), "membership": notes,
                 "cov_first": r1(b["coverage"].loc["2000"].mean() * 100, 0), "cov_2010": r1(b.loc["2010", "coverage"].mean() * 100, 0),
                 "cov_last": r1(b["coverage"].iloc[-21:].mean() * 100, 0), "macro": macro["notes"],
                 "ism_month": str(ism_tail.index[0].date())[:7], "ism_src": str(ism_tail["src"].iloc[0]),
                 "ppi_month": str(lg["ppi_yoy"].dropna().index[-1].date())[:7], "cpi_month": str(lg["cpi_yoy"].dropna().index[-1].date())[:7],
                 "vix3m_start": str(vix3m.index[0].date())},
        "d": [str(x.date()) for x in idx], "spx": arr(c, 2),
        "cnt": arr(cnt_all, 0), "st": arr(st_all, 0), "cntc": arr(cnt_core, 0), "stc": arr(st_core, 0),
        "act": {k: arr(act[k], 0) for k in keys},
        "inflbad": arr((~I["infl_ok"]).astype(int), 0), "stagbad": arr((sz == -1).astype(int), 0),
        "wd": wd, "wspx": [round(float(x), 1) for x in cv[sl][wlast]], "detail": detail,
        "now": {"spx": r1(cv[-1], 2), "dd": r1((cv[-1] / hi252.iloc[-1] - 1) * 100), "cnt": int(cnt_all.iloc[-1]), "st": int(st_all.iloc[-1]), "st_since": since(st_all),
                "cnt_since": since(cnt_all), "cntc": int(cnt_core.iloc[-1]), "stc": int(st_core.iloc[-1]), "stc_since": since(st_core),
                "infl_ok": bool(I["infl_ok"].iloc[-1]), "infl_since": since(I["infl_ok"]), "infl_bad": bad, "ppi": ppi_n, "cpi": cpi_n, "pr": pr_n,
                "spread": last(I["spread"], 1), "no": r1(float(now_ism["no"]), 1), "stag_zone": int(szv[-1]), "stag_since": since(sz),
                "last_event": log[0] if log else None, "trin_ok": bool(I["trin_ok"].iloc[-1])},
        "legs": legs, "bases": bases, "ev_all": ev_all, "ev_core": ev_core, "regimes": regimes, "macro_gate": macro_gate, "log": log[:60],
    }
    return P



# ============================================================ TEMPLATE
TEMPLATE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>S&amp;P 500 Buy Signal Aggregator</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<!-- House font is Bahnschrift (Light), a Windows system font; Barlow is the web fallback for machines without it. -->
<link href="https://fonts.googleapis.com/css2?family=Barlow:wght@300;400;500;600&display=swap" rel="stylesheet">
<meta name="color-scheme" content="light">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{
  --paper:#FFFFFF; --card:#FFFFFF; --ink:#000000; --body:#262626; --muted:#808080; --hair:#D6E3F0;
  --navy:#1F4E79; --yellow:#FFC000; --hl:#FFFF00; --grid:#BDD7EE; --pale:#DEEBF7;
  --grey:#A5A5A5; --green:#70AD47; --blue:#4472C4; --accent:#ED7D31;
  --font:Bahnschrift,"Barlow","Segoe UI",Arial,sans-serif;
  color-scheme:light;
}
*{box-sizing:border-box}
html{background:#FFFFFF;}
body{margin:0;background:#FFFFFF;color:var(--body);font-family:var(--font);font-weight:300;line-height:1.45;-webkit-font-smoothing:antialiased;}
b,strong{font-weight:500;}
.wrap{max-width:1220px;margin:0 auto;padding:36px 26px 0;}
header{border-bottom:2px solid var(--navy);padding-bottom:18px;margin-bottom:26px;}
.kicker{font-size:13px;font-weight:400;letter-spacing:.06em;text-transform:uppercase;color:var(--navy);margin:0 0 14px;}
h1{font-weight:300;font-size:clamp(28px,4.2vw,44px);line-height:1.18;margin:0 0 14px;color:var(--ink);letter-spacing:0;}
h1 .hl{background:var(--hl);padding:0 .14em;-webkit-box-decoration-break:clone;box-decoration-break:clone;}
.sub{font-size:16px;color:#404040;max-width:80ch;margin:0 0 16px;}
.meta{display:flex;flex-wrap:wrap;gap:6px 22px;font-size:12.5px;color:var(--muted);}
.meta b{color:var(--ink);font-weight:400;}
/* Panels: navy header band, message in black, chart title grey + underlined + centred, legend under the chart */
.card{background:#FFFFFF;border:1px solid var(--hair);border-radius:0;padding:0 20px 16px;margin-bottom:22px;min-width:0;}
.eyebrow{background:var(--navy);color:#FFFFFF;font-size:12.5px;font-weight:400;letter-spacing:.02em;margin:0 -20px 14px;padding:7px 20px;display:flex;align-items:center;gap:10px;}
.livebadge{margin-left:auto;font-size:10.5px;font-weight:500;letter-spacing:.05em;background:var(--yellow);color:var(--navy);padding:1px 7px;border-radius:2px;white-space:nowrap;}
.livebadge::before{content:"";display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--navy);margin-right:5px;vertical-align:middle;}
.card h3{font-weight:300;font-size:18px;color:var(--muted);text-align:center;text-decoration:underline;text-decoration-thickness:1px;text-underline-offset:4px;margin:2px 0 10px;letter-spacing:0;}
.deck{font-size:15px;color:var(--ink);margin:0 0 12px;line-height:1.4;}
.deck p{margin:0 0 8px;} .deck p:last-child{margin:0;}
.deck b{font-weight:500;}
.chartbox{position:relative;width:100%;}
.legendrow{display:flex;flex-wrap:wrap;justify-content:center;gap:4px 16px;font-size:12.5px;color:var(--muted);margin:8px 0 4px;}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;vertical-align:middle;}
.sq{border-radius:0;}
.cap{font-size:11.5px;color:var(--muted);margin:12px 0 0;padding-top:8px;border-top:1px solid #EDEDED;line-height:1.5;}
.cap b{color:var(--navy);font-weight:400;}
.cols{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:22px;}
.cols .card{margin-bottom:22px;}
@media(max-width:900px){.cols{grid-template-columns:1fr;gap:0;}}
/* current-read tiles */
.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:2px 0 14px;}
@media(max-width:900px){.kpis{grid-template-columns:repeat(2,minmax(0,1fr));}}
.kpi{border:1px solid #D9D9D9;border-radius:0;padding:10px 12px;background:#FFFFFF;min-width:0;}
.kpi.now{border:2px solid var(--navy);padding:9px 11px;}
.kpi .lab{font-size:11.5px;font-weight:400;letter-spacing:.06em;text-transform:uppercase;color:var(--navy);}
.kpi .val{font-size:30px;font-weight:300;color:var(--ink);line-height:1.2;margin-top:3px;font-variant-numeric:tabular-nums;}
.kpi .val small{font-size:14px;color:var(--muted);}
.kpi .note{font-size:12.5px;color:#404040;margin-top:5px;line-height:1.45;}
.pill{display:inline-block;font-size:10.5px;letter-spacing:.02em;padding:2px 7px;border-radius:2px;white-space:nowrap;font-weight:400;vertical-align:middle;}
.pill.number{background:var(--navy);color:#FFFFFF;}
.pill.neu{background:var(--pale);color:var(--navy);}
.pill.none{background:transparent;color:var(--muted);border:1px dashed #BFBFBF;}
.pill.bad{background:#C00000;color:#FFFFFF;}
.pill.good{background:#548235;color:#FFFFFF;}
/* window selector and signal picker */
.bar{display:flex;align-items:center;gap:6px;margin:0 0 14px;position:sticky;top:0;background:#FFFFFF;padding:8px 0;z-index:5;flex-wrap:wrap;}
.bar span{font-size:12.5px;color:var(--navy);text-transform:uppercase;letter-spacing:.06em;font-weight:400;margin-right:6px;}
.bar span.gap{margin-left:18px;}
button.b{font-family:var(--font);font-weight:400;font-size:12px;padding:3px 12px;border:1px solid var(--grid);background:#FFFFFF;color:var(--navy);border-radius:2px;cursor:pointer;}
button.b.on{background:var(--navy);color:#FFFFFF;border-color:var(--navy);}
.picker{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 12px;}
/* tables */
.tblwrap{overflow-x:auto;}
table{width:100%;border-collapse:collapse;font-size:12.5px;}
th{background:var(--navy);color:#FFFFFF;font-size:11.5px;font-weight:400;letter-spacing:.01em;text-align:right;padding:6px 8px;border:0;vertical-align:bottom;}
td{padding:7px 8px;border-bottom:1px solid var(--pale);vertical-align:top;line-height:1.35;text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;color:var(--body);}
th:first-child,td:first-child{text-align:left;}
th.t{text-align:left;}
td.t{text-align:left;white-space:normal;font-size:13.5px;font-weight:400;color:var(--ink);}
td.t small{display:block;color:var(--muted);font-size:11.5px;font-weight:300;margin-top:2px;line-height:1.35;}
td.t.r{font-size:12.5px;font-weight:300;color:var(--body);}
td.wr{white-space:normal;text-align:left;line-height:1.7;}
#tE td.t,#tB td.t:first-child{min-width:190px;}
tr.grp td{background:var(--pale);color:var(--navy);font-weight:400;font-size:12.5px;text-align:left;}
tr.base td, tr.base td.t{color:var(--muted);font-weight:300;}
tr.pass td:first-child{box-shadow:inset 3px 0 0 var(--navy);}
.mut{color:var(--muted);}
.notes{font-size:13.5px;color:#404040;margin:0;padding-left:20px;}
.notes li{margin-bottom:7px;line-height:1.5;}
.notes b{color:var(--ink);}
.footbar{background:var(--navy);color:#FFFFFF;margin-top:26px;font-size:14px;font-weight:300;}
.footbar .in{max-width:1220px;margin:0 auto;padding:10px 26px;display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;}
@media(max-width:520px){.wrap{padding:28px 16px 0;}.footbar .in{padding:10px 16px;font-size:12.5px;}}
</style>
</head>
<body>
<div class="wrap">
<header>
  <p class="kicker">Systematic Macro · Buy Signals</p>
  <h1><span class="hl">S&amp;P 500 Buy Signal Aggregator</span></h1>
  <p class="sub">Thirteen buy signals across price momentum, breadth thrusts, washouts, DeMark exhaustion and macro regime, counted the same way as the Breadth Thrust Aggregator. Breadth is rebuilt bottom-up from point-in-time index constituents; the inflation and stagflation models use PPI, CPI and ISM.</p>
  <div class="meta" id="meta"></div>
</header>

<section class="card">
  <p class="eyebrow">Current read<span class="livebadge">LIVE · YAHOO · FRED</span></p>
  <div class="kpis" id="kpis"></div>
  <div class="deck" id="read"></div>
</section>

<div class="bar"><span>Chart window</span><button class="b on" data-r="5Y">5Y</button><button class="b" data-r="10Y">10Y</button><button class="b" data-r="MAX">Max</button>
  <span class="gap">Signals</span><button class="b on" data-m="all" id="mAll"></button><button class="b" data-m="core" id="mCore"></button></div>

<section class="card">
  <p class="eyebrow">Buy signals · aggregator</p>
  <p class="deck" id="sub1"></p>
  <h3>Buy Signal Aggregator</h3>
  <div class="chartbox" id="cv1" style="height:840px"><canvas id="c1"></canvas></div>
  <div class="legendrow" id="lg1"></div>
  <p class="cap" id="fn1"></p>
</section>

<section class="card">
  <p class="eyebrow">Buy signals · board</p>
  <p class="deck">Where each signal stands at the latest close. A signal counts as active for <span id="holdN"></span> sessions after it fires.</p>
  <h3>Signal board</h3>
  <div class="tblwrap"><table id="tB"></table></div>
  <p class="cap" id="fnB"></p>
</section>

<section class="card">
  <p class="eyebrow">Inputs · signal detail</p>
  <p class="deck">The indicator behind each signal, sampled at week-end, with its trigger levels. Vertical lines mark the weeks the signal fired.</p>
  <div class="picker" id="picker"></div>
  <h3 id="h3det"></h3>
  <div class="chartbox" style="height:520px"><canvas id="c2"></canvas></div>
  <div class="legendrow" id="lg2"></div>
  <p class="cap" id="fn2"></p>
</section>

<section class="card">
  <p class="eyebrow">Evidence · signal screen</p>
  <p class="deck" id="subE"></p>
  <h3>Which signals carry evidence</h3>
  <div class="tblwrap"><table id="tE"></table></div>
  <p class="cap" id="fnE"></p>
</section>

<div class="cols">
  <section class="card">
    <p class="eyebrow">Evidence · aggregator state</p>
    <p class="deck">Average S&amp;P 500 forward price return from every day spent in each state, all 13 signals.</p>
    <h3>Does the aggregator state matter?</h3>
    <div class="tblwrap"><table id="tS1"></table></div>
    <p class="cap" id="fnS1"></p>
  </section>
  <section class="card">
    <p class="eyebrow">Evidence · screened signals</p>
    <p class="deck" id="subS2"></p>
    <h3>The same test on the screened signals</h3>
    <div class="tblwrap"><table id="tS2"></table></div>
    <p class="cap" id="fnS2"></p>
  </section>
</div>

<section class="card">
  <p class="eyebrow">Macro backdrop · inflation and stagflation regimes</p>
  <p class="deck">The two macro models read better as regimes than as one-off triggers. Annualised S&amp;P 500 price return while each regime is in force.</p>
  <h3>Returns by macro regime</h3>
  <div class="tblwrap"><table id="tM"></table></div>
  <p class="cap" id="fnM"></p>
</section>

<section class="card">
  <p class="eyebrow">Signal log</p>
  <p class="deck">First firing in each 21-session cluster. Forward returns are S&amp;P 500 price returns from the signal close.</p>
  <h3>Recent buy signals</h3>
  <div class="tblwrap"><table id="tL"></table></div>
</section>

<section class="card">
  <p class="eyebrow">Method and caveats</p>
  <ul class="notes" id="notes"></ul>
</section>
</div>
<div class="footbar"><div class="in"><span id="fb_date"></span><span>S&amp;P 500 Buy Signal Aggregator</span><span>Momentum · Breadth · Washouts · DeMark · Macro</span></div></div>

<script>
const P = /*@DATA*/null/*@END*/;
// Deck palette: navy, yellow, grey, green, blue; orange for overlays. One typeface throughout.
const K = {ink:'#000000', navy:'#1F4E79', yellow:'#FFC000', grey:'#A5A5A5', green:'#70AD47', blue:'#4472C4', accent:'#ED7D31', mute:'#808080', grid:'#BDD7EE', pale:'#DEEBF7',
  up:'#548235', dn:'#C00000', spx:'#000000', count:'#1F4E79', countFill:'#1F4E79D9', bandBad:'#C0000021', bandGood:'#70AD4738'};
const FONT = "Bahnschrift, 'Barlow', 'Segoe UI', Arial, sans-serif", MONO = FONT, SANS = FONT;
Chart.defaults.font.family = FONT; Chart.defaults.font.size = 12; Chart.defaults.font.weight = 300; Chart.defaults.color = K.mute; Chart.defaults.animation = false;
Chart.defaults.scale.border.display = false;

// ---------- formatting ----------
const MIN = '−';
const sgn = (v, d=1) => v == null ? '–' : (v < 0 ? MIN : '+') + Math.abs(v).toFixed(d);
const pc = (v, d=1) => v == null ? '–' : sgn(v, d) + '%';
const num = (v, d=0) => v == null ? '–' : (v < 0 ? MIN : '') + Math.abs(v).toLocaleString('en-US', {minimumFractionDigits:d, maximumFractionDigits:d});
const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const dfmt = s => s ? (+s.slice(8,10)) + ' ' + MON[+s.slice(5,7)-1] + ' ' + s.slice(0,4) : '–';
const mfmt = s => s ? MON[+s.slice(5,7)-1] + ' ' + s.slice(0,4) : '–';
const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const neg = s => String(s).replace(/-(\d)/g, MIN + '$1');

const N = P.now, M = P.meta, LEGS = P.legs, CORE = M.core, B = P.bases['2000'];
const FAM = {'Price momentum':K.navy, 'Breadth thrust':K.green, 'Washout':K.yellow, 'Exhaustion':K.blue, 'Macro regime':K.grey};
const stName = {1:'Bullish', 0:'Neutral', '-1':'No signal'}, stCls = {1:'good', 0:'neu', '-1':'bad'};
const gradeTxt = {pass:'Passes', weak:'Partial', none:'No edge'}, gradeCls = {pass:'number', weak:'neu', none:'none'};
const EA = P.ev_all, EC = P.ev_core;

// ---------- header, tiles, read ----------
const longDate = s => (+s.slice(8,10)) + ' ' + ['January','February','March','April','May','June','July','August','September','October','November','December'][+s.slice(5,7)-1] + ' ' + s.slice(0,4);
$('meta').innerHTML = '<span>Snapshot&nbsp; <b>' + esc(M.built) + '</b></span><span>Data to&nbsp; <b>' + dfmt(M.asof) + ' close</b></span><span>Live&nbsp; <b>Yahoo Finance prices</b> · <b>FRED PPI and CPI</b> · <b>index membership (fja05680 + Wikipedia)</b></span><span>Universe&nbsp; <b>' + M.members_last + ' S&amp;P 500 members, point-in-time</b></span><span>Evidence&nbsp; <b>aggregator ' + M.evid_start.slice(0,4) + ' to date · signals from 1950</b></span>';
$('fb_date').textContent = longDate(M.asof);
$('holdN').textContent = M.hold;
$('mAll').textContent = 'All ' + M.n_legs; $('mCore').textContent = 'Screened ' + CORE.length;
const inflTxt = N.infl_ok ? 'Favourable' : 'Unfavourable';
const zoneTxt = {1:'above 0, favourable', 0:'neutral zone', '-1':'stagflation zone'}[N.stag_zone];
$('kpis').innerHTML = [
  ['Aggregator · all ' + M.n_legs, N.cnt + '<small> active</small>', '<span class="pill ' + stCls[N.st] + '">' + stName[N.st] + '</span> since ' + dfmt(N.st_since)],
  ['Screened · ' + CORE.length + ' signals', N.cntc + '<small> active</small>', '<span class="pill ' + stCls[N.stc] + '">' + stName[N.stc] + '</span> since ' + dfmt(N.stc_since)],
  ['Inflation model', '<span style="font-size:24px">' + inflTxt + '</span>', 'PPI ' + pc(N.ppi) + ', CPI ' + pc(N.cpi) + ', Prices Paid ' + num(N.pr, 1) + '; since ' + dfmt(N.infl_since)],
  ['Stagflation spread', sgn(N.spread, 1), 'New Orders ' + num(N.no, 1) + ' less Prices Paid ' + num(N.pr, 1) + '; ' + zoneTxt],
].map((k, i) => '<div class="kpi' + (i < 2 ? ' now' : '') + '"><div class="lab">' + k[0] + '</div><div class="val">' + k[1] + '</div><div class="note">' + k[2] + '</div></div>').join('');

(function read(){
  const on = LEGS.filter(l => l.active), armed = LEGS.filter(l => l.armed && !l.active), le = N.last_event;
  let p1 = '<b>Aggregator: ' + N.cnt + ' of ' + M.n_legs + ' buy signals active, state ' + stName[N.st].toLowerCase() + ' since ' + dfmt(N.st_since) + '.</b> ';
  p1 += on.length ? 'Active: ' + on.map(l => esc(l.name) + ' (' + l.left + ' sessions left)').join(', ') + '. ' : 'Nothing has fired inside the ' + M.hold + '-session window. ';
  if (le) p1 += 'The last signal was the ' + esc(le.s) + ' on ' + dfmt(le.d) + '. ';
  if (armed.length) p1 += 'Armed and waiting on their trigger level: ' + armed.map(l => esc(l.name)).join(', ') + '. ';
  if (N.st === 0 && N.cnt === 0) p1 += 'With nothing active, the state drops to no signal once the count has been zero for 21 sessions. ';
  let p2 = '<b>What the state has meant since ' + M.evid_start.slice(0,4) + '.</b> Bullish days (21-session high of ' + M.bull_at + ' or more active) were followed by ' + pc(EA.bull.all['12m']) + ' over 12 months, higher ' + EA.bull.all.w12m + '% of the time. Neutral days returned ' + pc(EA.neutral.all['12m']) + ', no-signal days ' + pc(EA.none.all['12m']) + ', and all days ' + pc(B['12m']) + '. ' +
    'Only ' + CORE.length + ' of the ' + M.n_legs + ' signals pass the evidence screen. Counting those alone separates outcomes more sharply: ' + pc(EC.bull.all['12m']) + ' (higher ' + EC.bull.all.w12m + '%) at ' + M.core_at + ' or more active against ' + pc(EC.none.all['12m']) + ' (' + EC.none.all.w12m + '%) at none.';
  const R = P.regimes, G = P.macro_gate;
  let p3 = '<b>Macro backdrop: Inflation Model ' + inflTxt.toLowerCase() + ' since ' + dfmt(N.infl_since) + (N.infl_bad.length ? ' (' + esc(neg(N.infl_bad.join(', '))) + ')' : '') + '; stagflation spread ' + sgn(N.spread, 1) + ', ' + zoneTxt + '.</b> ' +
    'Since 1950 the S&amp;P 500 has annualised ' + pc(R[1].long.ann) + ' with the Inflation Model unfavourable against ' + pc(R[0].long.ann) + ' favourable, and ' + pc(R[4].long.ann) + ' with the spread below ' + MIN + '25. ' +
    'An unfavourable macro regime has also blunted the aggregator: bullish days returned ' + pc(G.bull_bad['12m']) + ' over 12 months under it, against ' + pc(G.bull_ok['12m']) + ' otherwise.';
  $('read').innerHTML = '<p>' + p1 + '</p><p>' + p2 + '</p><p>' + p3 + '</p>';
})();

// ---------- chart plumbing ----------
let RANGE = '5Y', MODE = 'all', DET = 'm440', charts = {};
function startIdx(dates){
  const n = dates.length; if (RANGE === 'MAX') return 0;
  const last = dates[n-1], cut = (+last.slice(0,4) - (RANGE === '5Y' ? 5 : 10)) + last.slice(4);
  const i = dates.findIndex(d => d >= cut); return i < 0 ? 0 : i;
}
function spans(a, test){ const out = []; let s = -1; for (let i = 0; i < a.length; i++){ const t = test(a[i]); if (t && s < 0) s = i; if (!t && s >= 0){ out.push([s, i-1]); s = -1; } } if (s >= 0) out.push([s, a.length-1]); return out; }
function yearTicks(labels){
  const idx = []; for (let i = 1; i < labels.length; i++) if (labels[i].slice(0,4) !== labels[i-1].slice(0,4)) idx.push(i);
  const step = Math.max(1, Math.ceil(idx.length / 14)); return idx.filter((_, k) => (idx.length - 1 - k) % step === 0);
}
const NICE = [300,400,500,600,700,800,1000,1250,1500,2000,2500,3000,3500,4000,5000,6000,7000,8000,10000,12000,15000,20000];
const bandPlugin = {id:'bands', beforeDatasetsDraw(ch, _a, o){
  if (!o || !o.bands) return; const {ctx, chartArea:a, scales:{x}} = ch; const half = (a.right - a.left) / Math.max(1, ch.data.labels.length - 1) / 2;
  const top = o.top ? ch.scales[o.top].top : a.top, bot = o.bottom ? ch.scales[o.bottom].bottom : a.bottom;
  ctx.save(); o.bands.forEach(b => { ctx.fillStyle = b.color; b.spans.forEach(([s, e]) => { const x0 = Math.max(a.left, x.getPixelForValue(s) - half), x1 = Math.min(a.right, x.getPixelForValue(e) + half); ctx.fillRect(x0, top, x1 - x0, bot - top); }); }); ctx.restore(); }};
const crossPlugin = {id:'cross', afterDatasetsDraw(ch){ const t = ch.tooltip; if (!t || !t.getActiveElements().length) return; const {ctx, chartArea:a} = ch; const xp = t.getActiveElements()[0].element.x;
  ctx.save(); ctx.strokeStyle = '#1F4E7988'; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(xp, a.top); ctx.lineTo(xp, a.bottom); ctx.stroke(); ctx.restore(); }};
const endPlugin = {id:'endlab', afterDatasetsDraw(ch, _a, o){ if (!o || o.ds == null) return; const m = ch.getDatasetMeta(o.ds); if (!m.data.length) return; const p = m.data[m.data.length-1]; const {ctx} = ch;
  ctx.save(); ctx.font = "500 12px " + FONT; ctx.fillStyle = K.navy; ctx.textBaseline = 'middle'; ctx.fillText(o.text, p.x + 6, p.y); ctx.restore(); }};
const refPlugin = {id:'refs', afterDatasetsDraw(ch, _a, o){ if (!o || !o.lines) return; const {ctx, chartArea:a} = ch; ctx.save();
  o.lines.forEach(l => { const sc = ch.scales[l.axis]; if (l.v < sc.min || l.v > sc.max) return; const y = sc.getPixelForValue(l.v); ctx.strokeStyle = l.color; ctx.lineWidth = 1; ctx.setLineDash(l.solid ? [] : [5,4]); ctx.beginPath(); ctx.moveTo(a.left, y); ctx.lineTo(a.right, y); ctx.stroke();
    if (l.text){ ctx.setLineDash([]); ctx.font = '300 11px ' + FONT; ctx.fillStyle = K.mute; ctx.textBaseline = 'bottom'; ctx.textAlign = l.align || 'left'; ctx.fillText(l.text, l.align === 'right' ? a.right - 4 : a.left + 5, y - 2); } });
  ctx.restore(); }};
const lanePlugin = {id:'lanes', beforeDatasetsDraw(ch, _a, o){ if (!o || !o.lanes) return; const {ctx, chartArea:a, scales:{x}} = ch; const sc = ch.scales[o.axis]; const half = (a.right - a.left) / Math.max(1, ch.data.labels.length - 1) / 2;
  const n = o.lanes.length, h = (sc.bottom - sc.top) / n; ctx.save();
  o.lanes.forEach((ln, k) => { const y = sc.top + k * h; ctx.fillStyle = K.pale; ctx.fillRect(a.left, y + h * .5 - .5, a.right - a.left, 1); ctx.fillStyle = ln.color;
    if (ln.rule){ ctx.fillStyle = K.navy; ctx.fillRect(a.left, y - .5, a.right - a.left, 1); ctx.fillStyle = ln.color; }
    ln.spans.forEach(([s, e]) => { const x0 = Math.max(a.left, x.getPixelForValue(s) - half), x1 = Math.min(a.right, x.getPixelForValue(e) + half); ctx.fillRect(x0, y + h * .2, Math.max(1.5, x1 - x0), h * .6); });
    ctx.font = (ln.dim ? '300 ' : '400 ') + '11.5px ' + FONT; ctx.fillStyle = ln.dim ? K.mute : K.navy; ctx.textAlign = 'right'; ctx.textBaseline = 'middle'; ctx.fillText(ln.label, a.left - 8, y + h / 2); });
  ctx.restore(); }};
const trigPlugin = {id:'trigs', beforeDatasetsDraw(ch, _a, o){ if (!o || !o.idx) return; const {ctx, chartArea:a, scales:{x}} = ch; ctx.save(); ctx.strokeStyle = o.color || '#ED7D31AA'; ctx.lineWidth = 1.2;
  o.idx.forEach(i => { const xp = x.getPixelForValue(i); if (xp < a.left || xp > a.right) return; ctx.beginPath(); ctx.moveTo(xp, a.top); ctx.lineTo(xp, a.bottom); ctx.stroke(); }); ctx.restore(); }};
Chart.register(bandPlugin, crossPlugin, endPlugin, refPlugin, lanePlugin, trigPlugin);

const GRID = {color:K.grid, drawTicks:false, lineWidth:1};
function xAxis(labels){ const yt = yearTicks(labels); return {type:'category', grid:{display:false}, border:{display:false}, ticks:{autoSkip:false, maxRotation:0, color:K.mute, padding:6, font:{size:12}, callback(v){ return labels[v] ? labels[v].slice(0,4) : ''; }}, afterBuildTicks(ax){ ax.ticks = yt.map(i => ({value:i})); }}; }
function priceAxis(px, weight){ const v = px.filter(x => x != null), lo = Math.min(...v) * .96, hi = Math.max(...v) * 1.04;
  return {type:'logarithmic', position:'left', stack:'s', stackWeight:weight, min:lo, max:hi, grid:GRID, border:{display:false}, title:{display:true, text:'S&P 500 (log)', color:K.mute, font:{size:11}},
    ticks:{color:K.navy, padding:6, font:{size:11.5}, callback:v => num(v)}, afterBuildTicks(ax){ let t = NICE.filter(n => n >= lo * 1.04 && n <= hi); if (t.length > 8) t = t.filter((_, k) => k % 2 === (t.length - 1) % 2); ax.ticks = t.map(n => ({value:n})); }}; }
function panelAxis(o){ const base = {type:'linear', position:'left', stack:'s', stackWeight:1, grid:GRID, border:{display:false}, ticks:{color:K.navy, padding:6, font:{size:11.5}}};
  const out = Object.assign({}, base, o); out.ticks = Object.assign({}, base.ticks, o.ticks || {}); if (out.title) out.title = Object.assign({color:K.mute}, out.title); return out; }
const area = (label, data, color, extra) => Object.assign({type:'line', label, data, backgroundColor:color, borderColor:color, borderWidth:0, pointRadius:0, pointHoverRadius:0, stepped:true, fill:true}, extra);
const line = (label, data, color, extra) => Object.assign({type:'line', label, data, borderColor:color, backgroundColor:color, borderWidth:1.6, pointRadius:0, pointHoverRadius:3, fill:false, tension:0, spanGaps:true}, extra);
const tipBase = {mode:'index', intersect:false, backgroundColor:K.navy, titleColor:'#FFFFFF', bodyColor:'#FFFFFF', footerColor:'#FFFFFF', cornerRadius:0, titleFont:{family:FONT, weight:500, size:12.5}, bodyFont:{family:FONT, weight:300, size:12}, footerFont:{family:FONT, size:12, weight:500}, padding:10, boxWidth:9, boxHeight:9, usePointStyle:false, displayColors:true, callbacks:{}};
function legend(id, items){ $(id).innerHTML = items.map(it => '<span>' + (it.ln ? '<span style="display:inline-block;width:14px;height:' + (it.thin ? 2 : 3) + 'px;background:' + it.c + ';vertical-align:middle;margin-right:6px"></span>'
  : '<span class="dot sq" style="background:' + it.c + (it.band ? ';border:1px solid #D9D9D9;box-sizing:border-box' : '') + '"></span>') + it.t + '</span>').join(''); }
function mk(id, cfg){ if (charts[id]) charts[id].destroy(); charts[id] = new Chart($(id), cfg); return charts[id]; }
const common = (labels, scales, plugins, padL) => ({maintainAspectRatio:false, responsive:true, layout:{padding:{right:58, left:padL || 4, top:4}}, interaction:{mode:'index', intersect:false}, scales:Object.assign({x:xAxis(labels)}, scales), plugins:Object.assign({legend:{display:false}}, plugins)});

// ---------- 1. aggregator ----------
function drawAgg(){
  const i0 = startIdx(P.d), sl = a => a.slice(i0);
  const L = sl(P.d), px = sl(P.spx), core = MODE === 'core';
  const cnt = sl(core ? P.cntc : P.cnt), st = sl(core ? P.stc : P.st), thr = core ? M.core_at : M.bull_at, nL = core ? CORE.length : M.n_legs;
  const legs = LEGS.filter(l => !core || CORE.includes(l.key));
  $('sub1').innerHTML = 'Count of active buy signals out of ' + nL + '; each stays on for ' + M.hold + ' sessions after it fires. Bullish when the 21-session high of the count is ' + thr + ' or more; no signal when nothing has been active for 21 sessions. <b>Latest: ' + (core ? N.cntc : N.cnt) + ' active, ' + stName[core ? N.stc : N.st].toLowerCase() + ' since ' + dfmt(core ? N.stc_since : N.st_since) + '.</b>';
  legend('lg1', [{c:K.countFill, t:'Active signals (count)'}, {c:K.spx, t:'S&amp;P 500', ln:1, thin:1}, {c:K.bandGood, t:'Bullish state', band:1}, {c:K.bandBad, t:'No-signal state', band:1}]
    .concat(Object.keys(FAM).filter(f => legs.some(l => l.family === f)).map(f => ({c:FAM[f], t:f}))).concat([{c:K.accent, t:'Macro regime unfavourable'}]));
  const lanes = legs.map(l => ({label:l.short, color:FAM[l.family], dim:l.grade !== 'pass', spans:spans(sl(P.act[l.key]), v => v === 1)}))
    .concat([{label:'Inflation regime', color:K.accent, rule:1, spans:spans(sl(P.inflbad), v => v === 1)}, {label:'Stagflation zone', color:K.accent, spans:spans(sl(P.stagbad), v => v === 1)}]);
  $('cv1').style.height = (400 + lanes.length * 27) + 'px';
  const ymax = Math.max(thr + 2, Math.max(...cnt)) + .4;
  mk('c1', {data:{labels:L, datasets:[
      line('S&P 500', px, K.spx, {yAxisID:'yP', borderWidth:1.4}),
      area('Active signals', cnt, K.countFill, {yAxisID:'yC', borderColor:K.navy, borderWidth:1})]},
    options:common(L, {
        yL:panelAxis({min:0, max:1, stackWeight:lanes.length * .27, grid:{display:false}, ticks:{display:false}, title:{display:false}}),
        yC:panelAxis({min:0, max:ymax, stackWeight:1.5, title:{display:true, text:'Count', font:{size:11}}, ticks:{stepSize:ymax > 8 ? 2 : 1, callback:v => Number.isInteger(v) ? v : ''}}),
        yP:priceAxis(px, 2.6)},
      {bands:{top:'yP', bottom:'yC', bands:[{color:K.bandGood, spans:spans(st, v => v === 1)}, {color:K.bandBad, spans:spans(st, v => v === -1)}]}, endlab:{ds:0, text:num(px[px.length-1])},
       lanes:{axis:'yL', lanes},
       refs:{lines:[{axis:'yC', v:thr, color:K.accent, text:'Bullish at ' + thr + '+', align:'right'}]},
       tooltip:Object.assign({}, tipBase, {callbacks:{title:it => dfmt(L[it[0].dataIndex]), label:c => c.datasetIndex === 0 ? ' S&P 500  ' + num(c.raw, 2) : ' Active signals  ' + c.raw + ' of ' + nL,
         footer:it => { const i = it[0].dataIndex, on = legs.filter(l => P.act[l.key][i0 + i] === 1).map(l => l.short); const mb = [];
           if (P.inflbad[i0 + i]) mb.push('inflation unfavourable'); if (P.stagbad[i0 + i]) mb.push('stagflation zone');
           return (on.length ? on.join(', ') + '\n' : '') + 'State: ' + stName[st[i]] + (mb.length ? '\nMacro: ' + mb.join(', ') : ''); }}})}, 98)});
  $('fn1').innerHTML = 'Bars in the middle strip show which signal is active; <b>labels in grey do not pass the evidence screen.</b> The two orange lanes are regimes, not signals: they show when the Inflation Model is unfavourable and when the ISM spread is below ' + MIN + '25. Src: Yahoo Finance, FRED, ISM, own calculations.';
}

// ---------- 2. signal inputs ----------
function drawDet(){
  const D = P.detail[DET], leg = LEGS.find(l => l.key === DET), i0 = startIdx(P.wd);
  const L = P.wd.slice(i0), px = P.wspx.slice(i0);
  const ds = [line('S&P 500', px, K.spx, {yAxisID:'yP', borderWidth:1.4})].concat(D.series.map(s => line(s[0], s[1].slice(i0), s[2], {yAxisID:'yI', borderWidth:1.4, stepped:D.step ? true : false})));
  const vals = [].concat(...D.series.map(s => s[1].slice(i0))).filter(v => v != null).concat(D.refs.map(r => r[0]));
  let lo = Math.min(...vals), hi = Math.max(...vals); const pad = (hi - lo) * .06 || 1; lo -= pad; hi += pad;
  legend('lg2', [{c:K.spx, t:'S&amp;P 500', ln:1, thin:1}].concat(D.series.map(s => ({c:s[2], t:esc(s[0]), ln:1}))).concat([{c:K.accent, t:esc(leg.name) + ' fired', ln:1, thin:1}]));
  $('h3det').textContent = leg.name + ' and the S&P 500';
  mk('c2', {data:{labels:L, datasets:ds},
    options:common(L, {yI:panelAxis({min:lo, max:hi, stackWeight:2, afterBuildTicks(ax){ ax.ticks = ax.ticks.filter(t => t.value > lo + (hi - lo) * .02 && t.value < hi - (hi - lo) * .05); }, ticks:{callback:v => num(v, Math.abs(hi - lo) < 5 ? 2 : 0)}}), yP:priceAxis(px, 2)},
      {endlab:{ds:0, text:num(px[px.length-1])}, trigs:{idx:D.trig.map(i => i - i0).filter(i => i >= 0)},
       refs:{lines:D.refs.map(r => ({axis:'yI', v:r[0], color:K.mute, text:neg(r[1])}))},
       tooltip:Object.assign({}, tipBase, {callbacks:{title:it => 'Week to ' + dfmt(L[it[0].dataIndex]), label:c => ' ' + c.dataset.label + '  ' + (c.raw == null ? '–' : num(c.raw, c.datasetIndex === 0 ? 0 : Math.abs(c.raw) < 10 ? 2 : 1))}})})});
  $('fn2').innerHTML = '<b>' + esc(leg.name) + ':</b> ' + esc(leg.rule) + '. Now: ' + esc(neg(leg.status)) + '.';
  document.querySelectorAll('#picker button').forEach(b => b.classList.toggle('on', b.dataset.k === DET));
}
$('picker').innerHTML = LEGS.map(l => '<button class="b" data-k="' + l.key + '">' + esc(l.short) + '</button>').join('');
$('picker').addEventListener('click', e => { const k = e.target.dataset && e.target.dataset.k; if (k){ DET = k; drawDet(); } });
document.querySelectorAll('.bar button[data-r]').forEach(b => b.addEventListener('click', () => { RANGE = b.dataset.r; document.querySelectorAll('.bar button[data-r]').forEach(x => x.classList.toggle('on', x === b)); drawAgg(); drawDet(); }));
document.querySelectorAll('.bar button[data-m]').forEach(b => b.addEventListener('click', () => { MODE = b.dataset.m; document.querySelectorAll('.bar button[data-m]').forEach(x => x.classList.toggle('on', x === b)); drawAgg(); }));

// ---------- tables ----------
const rw = (v, w) => v == null ? '–' : pc(v) + (w == null ? '' : ' <span class="mut">' + w + '%</span>');
(function board(){
  let h = '<tr><th class="t">Signal</th><th class="t">Reading now</th><th>Last fired</th><th>Status</th><th>Screen</th></tr>', fam = '';
  LEGS.forEach(l => { if (l.family !== fam){ fam = l.family; h += '<tr class="grp"><td colspan="5">' + esc(fam) + '</td></tr>'; }
    const pill = l.active ? '<span class="pill good">Active · ' + l.left + ' left</span>' : l.armed ? '<span class="pill neu">Armed</span>' : '<span class="pill none">Idle</span>';
    h += '<tr class="' + l.grade + '"><td class="t">' + esc(l.name) + '<small>' + esc(neg(l.rule)) + '</small></td><td class="t r">' + esc(neg(l.status)) + '</td><td class="nw">' + dfmt(l.last) + '</td><td>' + pill + '</td><td><span class="pill ' + gradeCls[l.grade] + '">' + gradeTxt[l.grade] + '</span></td></tr>'; });
  $('tB').innerHTML = h;
  $('fnB').innerHTML = 'Weekly signals use completed weeks only (latest: week to ' + dfmt(M.wk_last) + '). Macro inputs: PPI ' + mfmt(M.ppi_month + '-01') + ', CPI ' + mfmt(M.cpi_month + '-01') + ', ISM ' + mfmt(M.ism_month + '-01') + '. *Stand-in rule, see notes.';
})();
(function evid(){
  $('subE').innerHTML = 'S&amp;P 500 price return after each signal’s first firing in a 21-session cluster, against the average for all days over the same history. A signal passes if it beats that average at 6 and 12 months, does so at 12 months in both halves of its history, has a placebo p-value under 0.10 at 6 or 12 months, and has at least 10 signals.';
  let h = '<tr><th class="t">Signal</th><th>History</th><th>Signals</th><th>+3m</th><th>+6m</th><th>+12m (higher)</th><th>All days +12m</th><th>Placebo p<br>3m · 6m · 12m</th><th>+12m by half<br>vs all days</th><th>Worst 6m<br>drawdown</th><th>Screen</th></tr>';
  const pf = v => v == null ? '–' : v.toFixed(2);
  LEGS.forEach(l => { const e = l.ev, b = l.base;
    h += '<tr class="' + l.grade + '"><td class="t">' + esc(l.name) + '<small>' + esc(l.src) + '</small></td><td>' + l.since + '+</td><td>' + e.n + '</td><td>' + pc(e['3m']) + '</td><td>' + pc(e['6m']) + '</td><td>' + rw(e['12m'], e.w12m) + '</td><td>' + rw(b['12m'], b.w12m) + '</td>' +
      '<td class="nw">' + pf(e.p3m) + ' · ' + pf(e.p6m) + ' · ' + pf(e.p12m) + '</td><td class="nw">' + pc(e['12m_a']) + ' v ' + pc(e['b12m_a']) + '<br>' + pc(e['12m_b']) + ' v ' + pc(e['b12m_b']) + '</td><td>' + pc(e.dd_worst) + '</td><td><span class="pill ' + gradeCls[l.grade] + '">' + gradeTxt[l.grade] + '</span></td></tr>'; });
  h += '<tr class="base"><td class="t">All days<small>baseline by history window</small></td><td colspan="2"></td><td colspan="8" class="wr">' + Object.keys(P.bases).map(k => '<span style="white-space:nowrap">' + k + '+: ' + pc(P.bases[k]['3m']) + ' / ' + pc(P.bases[k]['6m']) + ' / ' + pc(P.bases[k]['12m']) + '</span>').join(' &nbsp;·&nbsp; ') + ' &nbsp;(3m / 6m / 12m)</td></tr>';
  $('tE').innerHTML = h;
  $('fnE').innerHTML = 'Placebo p: share of 3,000 random date-shifts of the whole signal set that did at least as well, which keeps the clustering of signals intact. Halves split each signal’s own history at its midpoint. Worst 6m drawdown is the deepest S&amp;P 500 close-to-low fall inside six months of any signal. The washout and exhaustion signals (Bullish %, Trifecta, weekly DeMark) show most of their edge at one to three months, which this 6-to-12-month screen does not reward.';
})();
function stateTable(id, E, fn){
  let h = '<tr><th class="t">State</th><th>% of days</th><th>+1m</th><th>+3m</th><th>+6m</th><th>+12m (higher)</th><th>+12m<br>2000–12</th><th>+12m<br>' + M.split + '+</th></tr>';
  [['bull','Bullish · ' + E.thr + '+ active'],['neutral','Neutral'],['none','No signal']].forEach(([k, t]) => { const s = E[k];
    h += '<tr><td class="t">' + t + '</td><td>' + num(s.all.pct, 1) + '</td><td>' + pc(s.all['1m']) + '</td><td>' + pc(s.all['3m']) + '</td><td>' + pc(s.all['6m']) + '</td><td>' + rw(s.all['12m'], s.all.w12m) + '</td><td>' + pc(s.h1['12m']) + '</td><td>' + pc(s.h2['12m']) + '</td></tr>'; });
  h += '<tr class="grp"><td colspan="8">By count on the day</td></tr>';
  E.groups.forEach(g => { h += '<tr><td class="t">' + g.label + ' active</td><td>' + num(g.all.pct, 1) + '</td><td>' + pc(g.all['1m']) + '</td><td>' + pc(g.all['3m']) + '</td><td>' + pc(g.all['6m']) + '</td><td>' + rw(g.all['12m'], g.all.w12m) + '</td><td>' + pc(g.h1['12m']) + '</td><td>' + pc(g.h2['12m']) + '</td></tr>'; });
  h += '<tr class="base"><td class="t">All days</td><td>100.0</td><td>' + pc(B['1m']) + '</td><td>' + pc(B['3m']) + '</td><td>' + pc(B['6m']) + '</td><td>' + rw(B['12m'], B.w12m) + '</td><td colspan="2"></td></tr>';
  $(id).innerHTML = h; $(fn.id).innerHTML = fn.text;
}
stateTable('tS1', EA, {id:'fnS1', text:'Daily observations since ' + dfmt(M.evid_start) + '. State uses the 21-session high of the count.'});
$('subS2').textContent = 'Only the ' + CORE.length + ' signals that pass the screen: ' + LEGS.filter(l => CORE.includes(l.key)).map(l => l.short).join(', ') + '.';
stateTable('tS2', EC, {id:'fnS2', text:'The screen was fitted on the same history, so some of this sharper separation is selection. Treat it as an upper bound.'});
(function macro(){
  const R = P.regimes, G = P.macro_gate;
  let h = '<tr><th class="t">Regime</th><th>% of days<br>1950+</th><th>Annualised<br>1950+</th><th>+12m avg<br>1950+</th><th>% of days<br>2000+</th><th>Annualised<br>2000+</th><th>+12m avg<br>2000+</th></tr>';
  R.forEach((r, i) => { if (i === 0) h += '<tr class="grp"><td colspan="7">Inflation Model · PPI, CPI and ISM Prices Paid</td></tr>'; if (i === 2) h += '<tr class="grp"><td colspan="7">Stagflation Model · ISM New Orders minus Prices Paid</td></tr>';
    h += '<tr><td class="t">' + esc(r.label) + '</td><td>' + num(r.long.pct, 1) + '</td><td>' + pc(r.long.ann) + '</td><td>' + pc(r.long['12m']) + '</td><td>' + num(r.recent.pct, 1) + '</td><td>' + pc(r.recent.ann) + '</td><td>' + pc(r.recent['12m']) + '</td></tr>'; });
  h += '<tr class="grp"><td colspan="7">Aggregator state crossed with the macro regime · 2000+</td></tr>';
  [['bull_ok','Bullish state, macro favourable'],['bull_bad','Bullish state, macro unfavourable'],['rest_ok','Other states, macro favourable'],['rest_bad','Other states, macro unfavourable']].forEach(([k, t]) => { const s = G[k];
    h += '<tr><td class="t">' + t + '</td><td colspan="3" class="mut" style="text-align:left">+3m ' + pc(s['3m']) + ' · +6m ' + pc(s['6m']) + '</td><td>' + num(s.pct, 1) + '</td><td></td><td>' + rw(s['12m'], s.w12m) + '</td></tr>'; });
  $('tM').innerHTML = h;
  $('fnM').innerHTML = 'Macro unfavourable means the Inflation Model is unfavourable or the ISM spread is below ' + MIN + '25. SentimenTrader’s published figures for the spread are +14.8% annualised above 0, +8.4% between, and ' + MIN + '5.9% below ' + MIN + '25. The spread before 2021 is digitised from their chart, so regime dates there are good to about two months.';
})();
(function log(){
  let h = '<tr><th>Date</th><th class="t">Signal</th><th>S&amp;P 500</th><th>+1m</th><th>+3m</th><th>+6m</th><th>+12m</th></tr>';
  P.log.slice(0, 40).forEach(r => { h += '<tr><td>' + dfmt(r.d) + '</td><td class="t">' + esc(r.s) + '</td><td>' + num(r.px) + '</td><td>' + pc(r.f1) + '</td><td>' + pc(r.f3) + '</td><td>' + pc(r.f6) + '</td><td>' + pc(r.f12) + '</td></tr>'; });
  $('tL').innerHTML = h;
})();
$('notes').innerHTML = [
  '<b>Construction.</b> Each signal is a dated trigger. It counts as active for ' + M.hold + ' sessions, the same hold as the Breadth Thrust Aggregator. The state is bullish when the 21-session high of the count is ' + M.bull_at + ' or more (' + M.core_at + ' or more for the screened set) and no signal when nothing has been active for 21 sessions. The two macro signals switch off early if their model turns unfavourable again.',
  '<b>Investec Breadth Thrust is a stand-in.</b> No published rule under that name could be found. The slot uses the 10-day advance/decline ratio crossing above 1.97 on S&amp;P 500 members (Deemer’s breakaway momentum), which is not in the Breadth Thrust Aggregator. Replace it once the actual rule is known.',
  '<b>Humble Student Trifecta runs on two of its three legs.</b> NYSE TRIN is not available from a free source, and a TRIN rebuilt from S&amp;P 500 members did not reproduce the April 2025 spike above 2 in the StockCharts panel. The signal therefore fires on the VIX term-structure inversion plus the %>50DMA ÷ %>150DMA reading below 0.5 (Cam Hui’s published level; the StockCharts panel draws the inverse ratio with a line near 1.7). The TRIN proxy is shown on the board for reference. VIX3M history starts ' + dfmt(M.vix3m_start) + '.',
  '<b>Breadth is rebuilt from index members.</b> Advance/decline, %>50/150/200DMA, the McClellan Summation Index and the Bullish Percent Index are computed from point-in-time S&amp;P 500 constituents on Yahoo prices, not NYSE data. Yahoo drops delisted names, so priced members cover ' + M.cov_first + '% of the index in 2000, ' + M.cov_2010 + '% in 2010 and ' + M.cov_last + '% now. The Summation thrust (below +100 to above +1,000) fires more often here than SentimenTrader’s NYSE version.',
  '<b>Bullish Percent Index.</b> Each member is charted point-and-figure with StockCharts’ traditional box scale, three-box reversal and daily highs and lows on as-traded (split-unadjusted) prices. The latest reading can be checked against $BPSPX.',
  '<b>Removed signals.</b> The daily and weekly MACD buys and the daily DeMark 9-13 were dropped on 18 Nov 2026 (the Advanced MACD Buy below is a filtered version of the daily one). None carried an edge: the MACD buys returned +9.6% (weekly) and +9.2% (daily) over 12 months against +9.4% for all days, and the daily DeMark +9.6% against +9.0%. With them gone the count is no longer padded by frequent, uninformative signals, and the bullish state is rarer and stronger.',
  '<b>Spring Days (added 23 Nov 2026).</b> The CMT definition: %>50DMA moves back above 80% and %>200DMA moves back above 40%, and both must hold. Here it runs on weekly closes of S&amp;P 500 members. Both lines must be at or above their levels for two weekly closes in a row, and each must have been below its level within the prior 13 weeks, so the signal marks a recovery rather than a market that never dipped. Looser readings (no washout, or a 26- to 52-week window) fired three to four times as often and showed little edge. From 2000 it fired 14 times; the 12-month return averaged +12.7% against +7.9% for all days.',
  '<b>Advanced MACD Buy (added 23 Nov 2026).</b> Daily MACD(12,26,9) crosses above its signal line while below zero, and either the 50-day average is rising (above its level ten sessions earlier) or there is a positive divergence. A divergence means the price low during this dip below the signal line undercuts the previous dip’s low while the MACD low is higher, with the earlier dip below zero and ending no more than 90 days before. The filters lift the plain MACD buy’s 3- and 6-month returns slightly but not its 12-month return, so the signal does not pass the screen. It fires about 1.6 times a year, so the bullish threshold rose from 4 to 5 to keep the bullish state about as selective as before.',
  '<b>DeMark 9-13 (weekly bars).</b> Buy setup 9 needs a bearish price flip then nine closes below the close four bars earlier. Buy countdown 13 counts closes at or below the low two bars earlier, with bar 13’s low at or below the close of bar 8; a completed sell setup or a close above the setup’s high cancels it and a new setup restarts it. Either event fires the signal.',
  '<b>Inflation Model.</b> Unfavourable when PPI (all commodities) is at +9% or more or ' + MIN + '12% or less year on year, CPI is at +4% or more or ' + MIN + '3% or less, or ISM Prices Paid is above 85. The buy signal is the day the model turns favourable. PPI and CPI are dated 16 days after month-end and ISM three days after, to respect release lags.',
  '<b>ISM data.</b> ISM does not allow free redistribution of its history. New Orders minus Prices Paid for 1948–2020 is digitised from the SentimenTrader chart (about ±3 points and one to two months). From 2021 the series uses actual prints. Prices Paid before 2021 is implied from the digitised spread and an OECD orders series that tracks ISM New Orders. Run rebuild.py with <span style="font-family:\'IBM Plex Mono\',monospace">--ism file.csv</span> (date, new_orders, prices_paid) to replace all of this with a Bloomberg history.',
  '<b>Evidence windows.</b> Price-only signals are tested from 1950 (DeMark from 1962, when daily highs and lows begin). Breadth signals are tested from 2000 and the Trifecta from 2006. The aggregator is tested from ' + M.evid_start.slice(0,4) + '. Returns are price returns, excluding dividends.',
  '<b>Not investment advice.</b> Thirteen signals across four horizons is a lot of tests, so some passes are luck. The forward-return tables describe history, not a forecast.'
].map(t => '<li>' + t + '</li>').join('');

drawAgg(); drawDet();
</script>
</body>
</html>
'''


# ============================================================ MAIN
def inject(tpl, P):
    data = json.dumps(P, separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")
    out, k = re.subn(r"/\*@DATA\*/.*?/\*@END\*/", lambda m: "/*@DATA*/" + data + "/*@END*/", tpl, count=1, flags=re.S)
    if k != 1:
        raise RuntimeError("data placeholder not found in template")
    return out


def main():
    global CACHE
    ap = argparse.ArgumentParser(description="Rebuild the S&P 500 Buy Signal Aggregator dashboard")
    default_dir = "/mnt/user-data/outputs" if os.path.isdir("/mnt/user-data/outputs") else "."
    ap.add_argument("--out", default=os.path.join(default_dir, "sp500_buy_signal_aggregator.html"))
    ap.add_argument("--cache", default=CACHE)
    ap.add_argument("--refresh", action="store_true", help="ignore cached downloads")
    ap.add_argument("--ism", default=None, help="CSV with date,new_orders,prices_paid to replace the built-in ISM series")
    a = ap.parse_args()
    if not FRED_KEY:
        raise SystemExit("FRED_API_KEY is not set. Add it as a repository secret (or export it locally); see README.")
    CACHE = a.cache
    os.makedirs(CACHE, exist_ok=True)
    if a.refresh:
        for f in ("prices.pkl", "macro.pkl"):
            fp = os.path.join(CACHE, f)
            if os.path.exists(fp):
                os.remove(fp)
    hist, notes = get_membership()
    universe = set().union(*hist[hist["date"] >= "1997-12-01"]["set"])
    log("membership", len(hist), "rows; universe", len(universe), notes)
    recent = set().union(*hist[hist["date"] >= hist["date"].max() - pd.Timedelta(days=400)]["set"])
    px = get_prices(universe, recent)
    missing = [k for k in ("spx", "vix", "vix3m") if k not in px]
    if missing:
        raise RuntimeError(f"index download failed: {missing}; rerun with --refresh")
    log("prices", px.get("mode", ""), px["adj"].shape, "S&P 500 to", px["spx"].index[-1].date())
    b, _ = build_breadth(px, hist, px.get("splits"))
    log("breadth built; Bullish % now", round(float(b["bpi"].dropna().iloc[-1]), 1), "| %>50DMA", round(float(b["p50"].iloc[-1]), 1))
    macro = get_macro(a.ism)
    log("macro", macro["notes"])
    spx = px["spx"]; spx = spx[spx.index >= "1949-01-01"]
    P = build_model(spx, b, px["vix"], px["vix3m"], macro, notes, px["adj"].shape[1])
    out = os.path.abspath(a.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    open(out, "w", encoding="utf-8").write(inject(TEMPLATE, P))
    N = P["now"]
    log(f"wrote {out} ({os.path.getsize(out) // 1024} KB)")
    log(f"as of {P['meta']['asof']}: {N['cnt']} of {P['meta']['n_legs']} active, state {N['st']:+d}; screened {N['cntc']} of {len(P['meta']['core'])}, state {N['stc']:+d}; "
        f"inflation model {'favourable' if N['infl_ok'] else 'unfavourable'}; ISM spread {N['spread']}")
    stale = (pd.Timestamp(dt.datetime.now(dt.timezone.utc).date()) - pd.Timestamp(P["meta"]["ism_month"] + "-01")).days
    if stale > 75:
        log("WARNING: latest ISM print is", P["meta"]["ism_month"], "- add newer months to ISM_ACTUAL or pass --ism")
    log("screened signals:", ", ".join(P["meta"]["core"]))


if __name__ == "__main__":
    main()
