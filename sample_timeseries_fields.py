#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
from datetime import datetime

import pandas as pd


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def parse_args():
    p = argparse.ArgumentParser(
        description="Ambil sample random field yang sama untuk seluruh time series dari CSV zonal stats."
    )

    p.add_argument("--s1-csv", help="Path CSV S1")
    p.add_argument("--s2-csv", help="Path CSV S2")

    p.add_argument("--uid-col", default="uid", help="Nama kolom unique id. Default: uid")
    p.add_argument("--sample-size", type=int, default=100, help="Jumlah sample field. Default: 100")
    p.add_argument("--random-seed", type=int, default=42, help="Random seed. Default: 42")

    p.add_argument("--sample-ids-csv", required=True, help="Output CSV daftar sample uid")
    p.add_argument("--s1-sample-csv", help="Output CSV sample S1")
    p.add_argument("--s2-sample-csv", help="Output CSV sample S2")

    p.add_argument("--common-only", action="store_true",
                   help="Kalau S1 dan S2 sama-sama ada, sample diambil dari irisan uid keduanya")

    return p.parse_args()


def read_uid_set(csv_path: str, uid_col: str) -> set:
    log(f"Baca UID unik dari: {csv_path}")
    usecols = [uid_col]
    df = pd.read_csv(csv_path, usecols=usecols)
    if uid_col not in df.columns:
        raise ValueError(f"Kolom UID '{uid_col}' tidak ditemukan di {csv_path}")
    uids = set(df[uid_col].dropna().astype(str).unique().tolist())
    log(f"Total UID unik: {len(uids):,}")
    return uids


def filter_csv_by_sample(csv_path: str, uid_col: str, sampled_uids: set, out_csv: str) -> None:
    log(f"Filter CSV: {csv_path}")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    chunksize = 100_000
    first = True
    total_in = 0
    total_out = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        if uid_col not in chunk.columns:
            raise ValueError(f"Kolom UID '{uid_col}' tidak ditemukan di {csv_path}")

        chunk[uid_col] = chunk[uid_col].astype(str)
        total_in += len(chunk)

        out_chunk = chunk[chunk[uid_col].isin(sampled_uids)].copy()
        total_out += len(out_chunk)

        if len(out_chunk) > 0:
            out_chunk.to_csv(out_csv, mode="w" if first else "a", index=False, header=first)
            first = False

    log(f"Selesai filter -> {out_csv}")
    log(f"  total row input : {total_in:,}")
    log(f"  total row output: {total_out:,}")


def main():
    args = parse_args()

    if not args.s1_csv and not args.s2_csv:
        raise ValueError("Minimal salah satu dari --s1-csv atau --s2-csv harus diisi")

    s1_uids = None
    s2_uids = None

    if args.s1_csv:
        s1_uids = read_uid_set(args.s1_csv, args.uid_col)

    if args.s2_csv:
        s2_uids = read_uid_set(args.s2_csv, args.uid_col)

    if args.common_only and s1_uids is not None and s2_uids is not None:
        candidate_uids = sorted(s1_uids.intersection(s2_uids))
        log(f"Mode common-only aktif. UID irisan S1 & S2: {len(candidate_uids):,}")
    else:
        if s1_uids is not None and s2_uids is not None:
            candidate_uids = sorted(s1_uids.union(s2_uids))
            log(f"Mode union UID. Total kandidat UID: {len(candidate_uids):,}")
        elif s1_uids is not None:
            candidate_uids = sorted(s1_uids)
            log(f"Pakai UID dari S1 saja: {len(candidate_uids):,}")
        else:
            candidate_uids = sorted(s2_uids)
            log(f"Pakai UID dari S2 saja: {len(candidate_uids):,}")

    if len(candidate_uids) == 0:
        raise ValueError("Tidak ada UID kandidat untuk di-sample")

    n = min(args.sample_size, len(candidate_uids))
    sampled = (
        pd.Series(candidate_uids)
        .sample(n=n, random_state=args.random_seed, replace=False)
        .sort_values()
        .tolist()
    )
    sampled_set = set(sampled)

    sample_df = pd.DataFrame({
        args.uid_col: sampled
    })

    os.makedirs(os.path.dirname(args.sample_ids_csv), exist_ok=True)
    sample_df.to_csv(args.sample_ids_csv, index=False)
    log(f"Daftar sample UID ditulis: {args.sample_ids_csv}")
    log(f"Jumlah sample: {len(sampled):,}")

    if args.s1_csv and args.s1_sample_csv:
        filter_csv_by_sample(args.s1_csv, args.uid_col, sampled_set, args.s1_sample_csv)

    if args.s2_csv and args.s2_sample_csv:
        filter_csv_by_sample(args.s2_csv, args.uid_col, sampled_set, args.s2_sample_csv)

    log("Selesai.")


if __name__ == "__main__":
    main()
