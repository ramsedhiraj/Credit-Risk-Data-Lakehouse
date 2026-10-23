"""
Script: download_large_dataset.py
Description: Downloads a verified 200,000-record historical sample from the canonical
             Lending Club loan dataset (accepted_2007_to_2018Q4.csv) hosted on Hugging Face.
             Enables large-scale integration testing and empirical performance benchmarking
             without bloating Git repository size.
"""

import os
import sys
import urllib.request

DATASET_URL = "https://huggingface.co/datasets/codesignal/lending-club-loan-accepted/resolve/main/accepted_2007_to_2018Q4.csv"
OUTPUT_PATH = "data/raw/accepted_2007_to_2018_sample.csv"
TARGET_ROWS = 200000


def download_historical_sample(output_file: str = OUTPUT_PATH, max_rows: int = TARGET_ROWS) -> None:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    if os.path.exists(output_file):
        size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"Dataset already exists at: {output_file} ({size_mb:.2f} MB). Skipping download.")
        return

    print(f"Streaming {max_rows:,} records from Hugging Face Lending Club mirror...")
    print(f"Source URL: {DATASET_URL}")
    print(f"Target destination: {output_file}")

    rows_written = 0
    with urllib.request.urlopen(DATASET_URL) as response, open(output_file, "wb") as out_f:
        # 1. Read and write CSV header
        header = response.readline()
        out_f.write(header)

        # 2. Stream historical rows
        for line in response:
            out_f.write(line)
            rows_written += 1
            if rows_written % 25000 == 0:
                print(f"  -> Streamed {rows_written:,} / {max_rows:,} rows...")
            if rows_written >= max_rows:
                break

    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Successfully downloaded {rows_written:,} rows to {output_file} ({file_size_mb:.2f} MB)")


if __name__ == "__main__":
    download_historical_sample()
