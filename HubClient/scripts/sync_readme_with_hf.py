#!/usr/bin/env python3
"""
Utility to sync README.md files from Hugging Face dataset repos.
For each dataset in the mapping, download the README.md and save it locally
alongside the corresponding parquet file as <base>_README.md.
"""
import os
import sys
import shutil
from huggingface_hub import hf_hub_download
from pathlib import Path
from tqdm import tqdm

def sync_readme_files():
    # Check if HF_TOKEN is set
    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        print("Error: HF_TOKEN environment variable not set")
        sys.exit(1)

    # Mapping of local parquet files to HF dataset repos
    datasets = [
        {"repo_id": "jc4p/farcaster-casts",    "local_file": "farcaster_casts.parquet"},
        {"repo_id": "jc4p/farcaster-links",    "local_file": "farcaster_links.parquet"},
        {"repo_id": "jc4p/farcaster-reactions", "local_file": "farcaster_reactions.parquet"},
    ]

    with tqdm(total=len(datasets), desc="Syncing READMEs", unit="repo") as pbar:
        for item in datasets:
            repo_id = item["repo_id"]
            local_file = Path(item["local_file"])
            # Derive local README filename based on parquet filename
            local_readme = local_file.with_name(local_file.stem + "_README.md")

            tqdm.write(f"Downloading README from {repo_id}...")
            try:
                cache_path = hf_hub_download(
                    repo_id=repo_id,
                    filename="README.md",
                    repo_type="dataset",
                    token=hf_token,
                )
                shutil.copy(cache_path, local_readme)
                tqdm.write(f"Saved README to {local_readme}")
            except Exception as e:
                tqdm.write(f"Warning: could not download README for {repo_id}: {e}")

            pbar.update(1)

if __name__ == "__main__":
    sync_readme_files()
