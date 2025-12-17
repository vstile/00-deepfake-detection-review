#!/usr/bin/env python3
"""
make_abc.py

Unisce le tre liste già deduplicate per set (A, B, C) ed elimina duplicati tra set.
Chiave primaria: DOI normalizzato; fallback: titolo normalizzato.
Aggiunge una colonna QuerySets con i set di provenienza (es. "A|B").

Uso:
  python make_abc.py \
    --a /processed/query-A_merged_deduplicated.csv \
    --b /processed/query-B_merged_deduplicated.csv \
    --c /processed/query-C_merged_deduplicated.csv \
    --out /processed/query-ABC_merged_deduplicated.csv \
    --print-stats

Dipendenze: pandas (pip install pandas)
"""
import argparse
import re
import sys
from pathlib import Path

import pandas as pd


def norm_doi(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    s = s.replace("https://doi.org/", "").replace("http://doi.org/", "")
    s = s.strip().strip(".")
    m = re.search(r"(10\.\d{4,9}/\S+)", s)
    return m.group(1).rstrip(".,;)") if m else ""


def norm_title(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_csv_any(p: Path) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(p, encoding=enc, low_memory=False)
        except Exception:
            continue
    raise RuntimeError(f"Cannot read {p} with common encodings")


def standardize(df: pd.DataFrame, qset: str) -> pd.DataFrame:
    # mappa colonne comuni
    title = df["Title"] if "Title" in df.columns else df["title"] if "title" in df.columns else df.iloc[:, 0]
    doi = None
    for col in ("DOI", "doi", "DOI Link", "Link", "DOI URL", "Article DOI", "url", "URL"):
        if col in df.columns:
            doi = df[col]
            break
    url = None
    for col in ("url", "URL", "PDF Link", "Link"):
        if col in df.columns:
            url = df[col]
            break
    authors = None
    for col in ("Authors", "authors", "Authors Full Names", "Author(s)"):
        if col in df.columns:
            authors = df[col]
            break
    year = None
    for col in ("Publication Year", "Year", "year"):
        if col in df.columns:
            year = df[col]
            break

    out = pd.DataFrame({
        "Title": title,
        "DOI": doi if doi is not None else "",
        "URL": url if url is not None else "",
        "Authors": authors if authors is not None else "",
        "Year": year if year is not None else "",
        "QuerySet": qset
    })
    out["doi_norm"] = out["DOI"].map(norm_doi)
    out["title_norm"] = out["Title"].map(norm_title)
    # chiave: DOI se presente, altrimenti titolo
    out["key"] = out.apply(lambda r: r["doi_norm"] if r["doi_norm"] else r["title_norm"], axis=1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="CSV deduplicato per Set A")
    ap.add_argument("--b", required=True, help="CSV deduplicato per Set B")
    ap.add_argument("--c", required=True, help="CSV deduplicato per Set C")
    ap.add_argument("--out", required=True, help="CSV di output unificato")
    ap.add_argument("--print-stats", action="store_true", help="Stampa riepilogo e intersezioni")
    args = ap.parse_args()

    A = standardize(read_csv_any(Path(args.a)), "A")
    B = standardize(read_csv_any(Path(args.b)), "B")
    C = standardize(read_csv_any(Path(args.c)), "C")

    initial_total = len(A) + len(B) + len(C)

    all_df = pd.concat([A, B, C], ignore_index=True)

    # Raggruppa per key e scegli un rappresentante: preferisci record con DOI presente, poi titolo più lungo
    def pick_rep(grp: pd.DataFrame) -> pd.Series:
        g = grp.copy()
        g["has_doi"] = g["doi_norm"].apply(lambda s: 1 if s else 0)
        g["title_len"] = g["Title"].astype(str).apply(len)
        return g.sort_values(["has_doi", "title_len"], ascending=[False, False]).iloc[0]

    reps = all_df.groupby("key", dropna=False).apply(pick_rep).reset_index(drop=True)

    # QuerySets uniti
    qsets = all_df.groupby("key")["QuerySet"].apply(lambda s: "|".join(sorted(set(s)))).reset_index()
    merged = reps.merge(qsets, on="key", suffixes=("", "_merged"))
    merged["QuerySets"] = merged["QuerySet_merged"]
    merged = merged.drop(columns=["QuerySet_merged"])

    final_total = len(merged)
    removed = initial_total - final_total

    # Intersezioni
    A_keys = set(A["key"])
    B_keys = set(B["key"])
    C_keys = set(C["key"])
    inter_AB = len((A_keys & B_keys) - C_keys)
    inter_AC = len((A_keys & C_keys) - B_keys)
    inter_BC = len((B_keys & C_keys) - A_keys)
    inter_ABC = len(A_keys & B_keys & C_keys)

    # Salva
    out_cols = ["Title", "Authors", "Year", "DOI", "URL", "QuerySets"]
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    merged[out_cols].to_csv(args.out, index=False)

    print(f"[OK] Input total: {initial_total} | Unique after cross-dedup: {final_total} | Removed: {removed}")
    if args.print_stats:
        print({
            "A_only": len(A_keys - B_keys - C_keys),
            "B_only": len(B_keys - A_keys - C_keys),
            "C_only": len(C_keys - A_keys - B_keys),
            "A∩B": inter_AB,
            "A∩C": inter_AC,
            "B∩C": inter_BC,
            "A∩B∩C": inter_ABC
        })


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
