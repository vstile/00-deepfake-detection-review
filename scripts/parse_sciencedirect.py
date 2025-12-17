#!/usr/bin/env python3
"""
parse_sciencedirect.py

Parser robusto per gli export TXT di ScienceDirect (blocchi di testo).
Estrae record con: source, title, authors (se disponibili), year, doi, url.

Uso:
  python parse_sciencedirect.py /path/to/raw/query-B_ScienceDirect_30.txt --out /path/to/processed/query-B_ScienceDirect_30_parsed.csv

Opzioni:
  - puoi passare più input; se --out è una directory, salva un CSV per ciascun input
  - se --out è un file singolo e ci sono più input, concatena tutto in un unico CSV

Dipendenze: pandas (pip install pandas)
"""
import argparse
import re
import sys
from pathlib import Path

import pandas as pd


DOI_PAT = re.compile(r"(10\.\d{4,9}/\S+)", re.IGNORECASE)
URL_PAT = re.compile(r"(https?://\S+)")
YEAR_PAT = re.compile(r"\b(20\d{2}|19\d{2})\b")


def _norm_doi(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    s = s.replace("https://doi.org/", "").replace("http://doi.org/", "")
    s = s.strip().strip(".").lower()
    m = DOI_PAT.search(s)
    return m.group(1).rstrip(".,;)") if m else ""


def parse_sciencedirect_txt(text: str) -> pd.DataFrame:
    lines = [ln.strip() for ln in text.splitlines()]
    records = []
    for i, ln in enumerate(lines):
        doi_match = DOI_PAT.search(ln)
        if not doi_match:
            continue
        doi = doi_match.group(1).rstrip(".,;)")
        # title: prendi una delle ultime 3 righe non vuote, non-URL, non "doi:"
        title = None
        for j in range(i - 1, max(-1, i - 4), -1):
            if j >= 0 and lines[j] and not lines[j].lower().startswith(("http://", "https://", "doi:")):
                title = lines[j]
                break
        # year: cerca nell'intorno
        window = " ".join(lines[max(0, i - 5): i + 6])
        ym = YEAR_PAT.search(window)
        year = ym.group(1) if ym else None
        # url: se presente nella stessa riga (o precedente)
        url_m = URL_PAT.search(ln) or (URL_PAT.search(lines[i - 1]) if i > 0 else None)
        url = url_m.group(1).rstrip(".,;)") if url_m else None

        records.append({
            "source": "ScienceDirect",
            "title": title,
            "authors": None,  # di solito non presente nel TXT minimale
            "year": year,
            "doi": doi,
            "url": url
        })

    df = pd.DataFrame(records)
    if not df.empty:
        # de-dup per DOI normalizzato
        df["doi_norm"] = df["doi"].map(_norm_doi)
        df = df.drop_duplicates(subset=["doi_norm"]).drop(columns=["doi_norm"])
        df = df.reset_index(drop=True)
    return df


def read_text_file(path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return path.read_text(encoding=enc, errors="ignore")
        except Exception:
            continue
    raise RuntimeError(f"Cannot read {path} with common encodings")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", help="File TXT ScienceDirect da parsare")
    ap.add_argument("--out", required=True, help="CSV di output o directory")
    args = ap.parse_args()

    out_path = Path(args.out)
    inputs = [Path(p) for p in args.inputs]
    frames = []

    if len(inputs) == 1 and out_path.suffix.lower() in (".csv", ".tsv"):
        text = read_text_file(inputs[0])
        df = parse_sciencedirect_txt(text)
        df.to_csv(out_path, index=False)
        print(f"[OK] Saved {len(df)} rows -> {out_path}")
        return

    # Più input: se out è dir, salva un file per input; altrimenti concatena
    if len(inputs) > 1 and (out_path.exists() and out_path.is_dir() or out_path.suffix == ""):
        out_path.mkdir(parents=True, exist_ok=True)
        for p in inputs:
            text = read_text_file(p)
            df = parse_sciencedirect_txt(text)
            dst = out_path / (p.stem + "_parsed.csv")
            df.to_csv(dst, index=False)
            print(f"[OK] {p.name}: {len(df)} rows -> {dst}")
        return

    # Concatena in unico CSV
    for p in inputs:
        text = read_text_file(p)
        df = parse_sciencedirect_txt(text)
        df["__src_file__"] = p.name
        frames.append(df)
    out_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["source", "title", "authors", "year", "doi", "url"]
    )
    out_df.to_csv(out_path, index=False)
    print(f"[OK] Saved {len(out_df)} rows (merged) -> {out_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
