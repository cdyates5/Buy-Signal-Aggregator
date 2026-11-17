# S&P 500 Buy Signal Aggregator — auto-refreshing dashboard

A GitHub Actions workflow rebuilds the dashboard after every US trading day and publishes it to GitHub Pages.
Nothing runs on your computer once it is set up.

```
.github/workflows/refresh.yml   schedule, cache, rebuild, sanity check, deploy, monthly keepalive
rebuild.py                      the whole pipeline: data -> signals -> evidence -> single-file HTML
check.py                        sanity gate: blocks publishing if the build looks broken
data/ism_overrides.csv          ISM New Orders / Prices Paid prints you add by hand when needed
requirements.txt
```

## What happens each night

1. **22:17 UTC, Monday to Friday**, after the US close (about 9:17am Melbourne time in summer).
2. **Price cache restored.** The workflow restores the price cache from the previous run, then tops up only the last
   three weeks for current index members. That's about 7 Yahoo requests instead of about 1,170 tickers. A first run,
   or a run after the cache has expired, downloads the full history, which takes 10–15 minutes.
3. **Macro data fetched:** FRED (PPI, CPI, OECD orders), the DBnomics ISM mirror and Trading Economics for the latest
   ISM print, plus your `data/ism_overrides.csv`.
4. **Page built and checked.** `rebuild.py` writes `site/index.html`, and `check.py` verifies it:
   - the data is no more than 3 business days old
   - about 500 index members are priced
   - all 14 signals are present
   - the S&P 500 series has no gaps

   If any check fails, the job stops and **the previous page stays live**.
5. **Page published** to `https://<your-user>.github.io/<repo-name>/`. A small `status.json` sits next to it.

## Setup (about 10 minutes)

### 1. Get a FRED API key
Free from <https://fred.stlouisfed.org/docs/api/api_key.html>: sign in, choose **Request API key**, and copy the
32-character key.

### 2. Create the repository
On GitHub, choose **New repository** and name it, for example `buy-signal-aggregator`.

- **Public:** GitHub Pages is free for public repositories.
- **Private:** needs a paid plan (Pro, Team or Enterprise) for Pages.
- Don't add a README or .gitignore. This bundle has them.

### 3. Upload the files
With git:

```bash
unzip buy-signal-aggregator.zip && cd buy-signal-aggregator
git init -b main
git add .
git commit -m "Buy Signal Aggregator"
git remote add origin https://github.com/<your-user>/buy-signal-aggregator.git
git push -u origin main
```

Without git: on the new repo page, choose **uploading an existing file** and drag in the *contents* of the unzipped
folder. Include the `.github` and `data` folders. On macOS, press `Cmd+Shift+.` in Finder to show hidden folders.

### 4. Add the FRED key as a secret
Go to **Settings → Secrets and variables → Actions → New repository secret**.

- Name: `FRED_API_KEY`
- Secret: your key

### 5. Turn on GitHub Pages
Go to **Settings → Pages → Build and deployment → Source** and choose **GitHub Actions**.

### 6. Allow the workflow to commit
Go to **Settings → Actions → General → Workflow permissions**, choose **Read and write permissions**, and save.
This is only needed for the monthly keepalive commit (see below).

### 7. Run it once by hand
Go to **Actions → Refresh Buy Signal Aggregator → Run workflow**.

- Leave **full refresh** unticked. The first run downloads everything anyway, which takes 10–15 minutes.
- When both jobs show green ticks, the page URL appears on the **deploy** job and under **Settings → Pages**.

After that it runs on its own every weekday.

## Monthly upkeep

**ISM prints (first business day of each month).** The workflow tries to read the latest ISM Manufacturing New Orders
and Prices Paid from Trading Economics, but that site sometimes blocks GitHub's servers. If the run log shows the
warning *"ISM New Orders/Prices Paid last month is …"*, add a line to `data/ism_overrides.csv` with the month (first of
the month), New Orders and Prices Paid, then commit:

```
date,new_orders,prices_paid
2026-11-01,54.8,72.4
```

These rows override any other source for those months. You can also paste a full Bloomberg history here.

Nothing else needs maintenance. Index membership, prices, PPI and CPI all update automatically.

## Troubleshooting

| Symptom | What it means / what to do |
|---|---|
| Run failed at **Rebuild dashboard** with HTTP 429 or "rate limited" | Yahoo throttled GitHub's shared IP. The step already retries once after 3 minutes. If it still fails, re-run the job later, or leave it: the old page stays up and tomorrow's run catches up. |
| Run failed at **Sanity check** | The log lists the reason, for example stale data or too few members priced. The previous page stays live. Re-run once the source recovers. |
| `FRED_API_KEY is not set` | The secret is missing or misspelt (step 4). |
| Deploy job fails with "Pages not enabled" or "environment protection" | Step 5. Also check **Settings → Environments → github-pages** allows deployments from `main`. |
| A run takes 10–15 minutes again | The cache expired (GitHub deletes caches unused for 7 days) or you ticked full refresh. It rebuilds the cache and later runs are fast again. |
| Data looks off after a big index change or a stock split | Run with **full refresh** ticked. That re-downloads every series from scratch. |
| Scheduled runs stopped | GitHub pauses schedules after 60 days without repository activity. The workflow makes an empty "keepalive" commit on the first Monday of each month to prevent this. If it was paused, re-enable it under **Actions**. |

## Running locally

```bash
pip install -r requirements.txt
export FRED_API_KEY=your_key
python rebuild.py --out site/index.html --ism data/ism_overrides.csv
python check.py site/index.html
```

`--refresh` forces a full download. `--cache DIR` sets where downloads are kept (default `cache/`).

## Notes

- **Schedule:** to change the run time, edit the `cron` line in `.github/workflows/refresh.yml`. It's in UTC.
  `17 22 * * 1-5` is 22:17 UTC on weekdays.
- **Methodology:** signal definitions, data caveats (constituent breadth, the digitised ISM history before 2021, the
  Investec and Trifecta stand-ins) and the evidence screen are documented in the dashboard's *Method and caveats*
  section.
- **Not investment advice.**
