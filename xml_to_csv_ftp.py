#!/usr/bin/env python3
"""
BI XML → CSV Converter for SFMC
=================================
Downloads PartenaireBI.xml from SFMC Enhanced FTP, parses PROGRAMME blocks,
converts to CSV, uploads to /Import/ on the same FTP.

SFMC Import Activity picks up the CSV automatically.

Runs on: GitHub Actions (daily cron) or locally via terminal.
Requirements: pip install paramiko
"""

import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO, StringIO

import paramiko

# =============================================================================
# CONFIG — reads from environment variables (GitHub Secrets) or fallback values
# =============================================================================

FTP_HOST     = os.environ.get("FTP_HOST",     "mct8vv9h4h0gy1x8xmv8np06rlpy.ftp.marketingcloudops.com")
FTP_PORT     = int(os.environ.get("FTP_PORT",  "22"))
FTP_USERNAME = os.environ.get("FTP_USERNAME",  "536005700_7")
FTP_PASSWORD = os.environ.get("FTP_PASSWORD",  "")  # Never hardcode — use env var or GitHub Secret

XML_PATH = "/bi/PartenaireBI.xml"
CSV_PATH = "/Import/PartenaireBI.csv"

# =============================================================================
# CSV COLUMNS — must match your DE field names exactly
# =============================================================================

CSV_COLUMNS = [
    "Program_URL",
    "Program_Ref",
    "Program_Name",
    "Program_City",
    "Program_ZipCode",
    "Program_Department",
    "Program_Arguments",
    "Scraping_Date",
    "Scraping_Status",
    "Error_Message",
    "Program_Image",
]

# =============================================================================
# XML PARSING — Same logic as your CloudPage / SSJS
# =============================================================================

def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"<[^>]*>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def get_tag_text(element, tag):
    el = element.find(tag)
    if el is not None and el.text:
        return clean_text(el.text)
    return ""


def get_program_url(el, raw_block):
    for url_el in el.iter("URL"):
        if url_el.text and "/programme-neuf-" in url_el.text:
            return url_el.text.strip()
    match = re.search(r"<URL>\s*(.*?/programme-neuf-[^<]*)\s*</URL>", raw_block)
    if match:
        return match.group(1).strip()
    return ""


def get_points_forts(el):
    pf_block = el.find("POINTS_FORTS")
    if pf_block is None:
        return []
    return [clean_text(pf.text) for pf in pf_block.findall("PF") if pf.text]


def build_arguments(el, name):
    pfs = get_points_forts(el)
    if pfs:
        return " | ".join(pfs)
    for tag in ["PROMESSE_PROGRAMME", "DESCRIPTIF_COURT", "DESCRIPTIF_LONG",
                "DESCRIPTIF_CENTRE_D_APPEL"]:
        val = get_tag_text(el, tag)
        if val:
            return val
    return name if name else "N/A"


def get_program_image(el):
    persp = el.find("PERSPECTIVES")
    if persp is None:
        return "NO IMAGE"
    for url_el in persp.findall("URL"):
        if url_el.text and url_el.text.strip():
            return url_el.text.strip()
    return "NO IMAGE"


def parse_xml(xml_content):
    xml_content = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', xml_content, flags=re.DOTALL)
    blocks = re.findall(r"<PROGRAMME>(.*?)</PROGRAMME>", xml_content, re.DOTALL)
    print(f"[PARSE] Found {len(blocks)} PROGRAMME blocks")

    programs = []
    seen = set()
    skipped = 0

    for block_content in blocks:
        raw_block = f"<PROGRAMME>{block_content}</PROGRAMME>"
        try:
            el = ET.fromstring(raw_block)
        except ET.ParseError:
            try:
                el = ET.fromstring(f"<root>{raw_block}</root>").find("PROGRAMME")
            except ET.ParseError:
                skipped += 1
                continue

        ref  = get_tag_text(el, "REF_OPERATION") or get_tag_text(el, "NUMERO")
        name = get_tag_text(el, "NOM")
        city = get_tag_text(el, "VILLE")
        zip_ = get_tag_text(el, "CP")
        dept = get_tag_text(el, "DEPARTEMENT")
        url  = get_program_url(el, raw_block)

        if not all([ref, name, city, zip_, dept, url]):
            skipped += 1
            continue
        if "/programme-neuf-" not in url:
            skipped += 1
            continue

        key = f"{ref}||{url}"
        if key in seen:
            continue
        seen.add(key)

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        programs.append({
            "Program_URL":        url[:500],
            "Program_Ref":        ref[:50],
            "Program_Name":       name[:255],
            "Program_City":       city[:100],
            "Program_ZipCode":    zip_[:10],
            "Program_Department": dept[:2],
            "Program_Arguments":  build_arguments(el, name)[:4000],
            "Scraping_Date":      now_str,
            "Scraping_Status":    "SUCCESS",
            "Error_Message":      "",
            "Program_Image":      (get_program_image(el) or "NO IMAGE")[:500],
        })

    dups = len(blocks) - len(programs) - skipped
    print(f"[PARSE] Valid: {len(programs)}, Skipped: {skipped}, Duplicates: {dups}")
    return programs


# =============================================================================
# CSV
# =============================================================================

def programs_to_csv(programs):
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    for p in programs:
        writer.writerow(p)
    csv_str = output.getvalue()
    print(f"[CSV] Generated {len(programs)} rows, {len(csv_str):,} bytes")
    return csv_str


# =============================================================================
# FTP
# =============================================================================

def ftp_connect():
    print(f"[FTP] Connecting to {FTP_HOST}:{FTP_PORT}...")
    transport = paramiko.Transport((FTP_HOST, FTP_PORT))
    transport.connect(username=FTP_USERNAME, password=FTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("[FTP] Connected")
    return transport, sftp


def ftp_download(sftp, path):
    print(f"[FTP] Downloading {path}...")
    buffer = BytesIO()
    sftp.getfo(path, buffer)
    content = buffer.getvalue().decode("utf-8", errors="replace")
    print(f"[FTP] Downloaded {len(content):,} characters")
    return content


def ftp_upload(sftp, path, content):
    print(f"[FTP] Uploading to {path}...")
    buffer = BytesIO(content.encode("utf-8"))
    sftp.putfo(buffer, path)
    print(f"[FTP] Upload complete ({len(content):,} bytes)")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("BI XML → CSV Converter for SFMC")
    print(f"Started: {datetime.now()}")
    print("=" * 60)

    if not FTP_PASSWORD:
        print("ERROR: FTP_PASSWORD not set.")
        print("  Local:   export FTP_PASSWORD='your_password'")
        print("  GitHub:  Add FTP_PASSWORD to repository Secrets")
        sys.exit(1)

    transport, sftp = ftp_connect()

    try:
        # Download XML
        xml_content = ftp_download(sftp, XML_PATH)

        # Parse
        programs = parse_xml(xml_content)
        if not programs:
            print("[DONE] No valid programs. No CSV created.")
            sys.exit(0)

        # Convert to CSV
        csv_content = programs_to_csv(programs)

        # Ensure /Import/ exists
        try:
            sftp.stat("/Import")
        except FileNotFoundError:
            print("[FTP] Creating /Import/ directory...")
            sftp.mkdir("/Import")

        # Upload CSV
        ftp_upload(sftp, CSV_PATH, csv_content)

    finally:
        sftp.close()
        transport.close()

    print("=" * 60)
    print(f"[DONE] {len(programs)} programs → {CSV_PATH}")
    print(f"Finished: {datetime.now()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
