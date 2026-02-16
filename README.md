# BI XML → SFMC Data Extension (Automated)

Converts `PartenaireBI.xml` from SFMC Enhanced FTP into a CSV and uploads it back.  
SFMC Import Activity picks up the CSV and fills the Data Extension automatically.

## Architecture

```
Partner drops XML → Enhanced FTP (/bi/)
                        ↓
        GitHub Actions (daily cron) runs Python script
                        ↓
            Downloads XML → Parses → Generates CSV
                        ↓
        Uploads CSV → Enhanced FTP (/Import/)
                        ↓
        SFMC File Drop trigger detects CSV
                        ↓
        SFMC Import Activity → Data Extension filled ✅
```

## Setup Guide

### Step 1 — GitHub Repository

1. Go to https://github.com/new
2. Create a **private** repository (name it e.g. `sfmc-bi-scraper`)
3. Push this code to it (see commands below)

### Step 2 — Add Secrets

Go to your repo → **Settings → Secrets and variables → Actions → New repository secret**

Add these 4 secrets:

| Name | Value |
|------|-------|
| `FTP_HOST` | `mct8vv9h4h0gy1x8xmv8np06rlpy.ftp.marketingcloudops.com` |
| `FTP_PORT` | `22` |
| `FTP_USERNAME` | `536005700_7` |
| `FTP_PASSWORD` | *(your FTP password from FileZilla)* |

### Step 3 — Adjust Schedule

Edit `.github/workflows/scrape.yml` and change the cron time.  
The partner drops the file daily — set the cron to run **30 minutes after** that time.

```yaml
schedule:
  - cron: "0 7 * * *"   # 7:00 AM UTC = 8:00 AM Morocco time
```

Some common times (UTC):
- `"0 6 * * *"` = 7:00 AM Morocco
- `"0 7 * * *"` = 8:00 AM Morocco
- `"0 8 * * *"` = 9:00 AM Morocco
- `"30 7 * * *"` = 8:30 AM Morocco

### Step 4 — SFMC Import Activity

In Automation Studio, create a new Automation:

1. **Trigger**: File Drop → watches `PartenaireBI.csv`
2. **Step 1**: Import Activity:
   - Source: Enhanced FTP
   - File naming pattern: `PartenaireBI.csv`
   - Delimiter: Comma
   - Destination: `BI_AVP_Program_Scraped_Data`
   - Update type: **Add and Update**

### Step 5 — Push Code & Test

```bash
cd sfmc-bi-scraper
git init
git add .
git commit -m "Initial commit"
git remote add origin git@github.com:YOUR_USERNAME/sfmc-bi-scraper.git
git push -u origin main
```

Then go to **Actions** tab → select the workflow → click **Run workflow** to test it manually.

## Local Testing

```bash
export FTP_PASSWORD='your_password_here'
pip3 install paramiko
python3 xml_to_csv_ftp.py
```
