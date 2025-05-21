import os
import sys
from huggingface_hub import HfApi, upload_file
from pathlib import Path
from tqdm import tqdm
import time

def upload_parquet_files():
    # Check if HF_TOKEN is set
    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        print("Error: HF_TOKEN environment variable not set")
        sys.exit(1)
    
    # Initialize Hugging Face API
    api = HfApi(token=hf_token)
    
    # Mapping of local files to HF datasets and paths
    uploads = [
        {
            "local_file": "farcaster_casts.parquet",
            "repo_id": "jc4p/farcaster-casts",
            "repo_path": "casts.parquet"
        },
        {
            "local_file": "farcaster_links.parquet",
            "repo_id": "jc4p/farcaster-links",
            "repo_path": "farcaster_links.parquet"
        },
        {
            "local_file": "farcaster_reactions.parquet",
            "repo_id": "jc4p/farcaster-reactions",
            "repo_path": "farcaster_reactions.parquet"
        }
    ]
    
    # Create a progress bar for the overall process
    with tqdm(total=len(uploads), desc="Uploading files", unit="file") as pbar:
        # Upload each file
        for upload in uploads:
            local_file = Path(upload["local_file"])
            
            # Check if file exists
            if not local_file.exists():
                print(f"\nWarning: {local_file} not found, skipping upload")
                pbar.update(1)
                continue
            
            file_size = local_file.stat().st_size
            pbar.write(f"\nUploading {local_file} ({file_size/1024/1024:.2f} MB) to {upload['repo_id']}/{upload['repo_path']}...")
            
            try:
                # For large files, create a custom progress bar
                if file_size > 10 * 1024 * 1024:  # If file is larger than 10MB
                    # We can't track actual upload progress from the HF API, so we'll simulate it
                    with tqdm(total=100, desc=f"  {local_file.name}", unit="%", leave=False) as file_pbar:
                        # Start upload in background (this is just for visualization)
                        start_time = time.time()
                        url = api.upload_file(
                            path_or_fileobj=str(local_file),
                            path_in_repo=upload["repo_path"],
                            repo_id=upload["repo_id"],
                            repo_type="dataset"
                        )
                        # Upload is done, fill the progress bar
                        file_pbar.update(100 - file_pbar.n)
                else:
                    # For smaller files, just upload without a special progress bar
                    url = api.upload_file(
                        path_or_fileobj=str(local_file),
                        path_in_repo=upload["repo_path"],
                        repo_id=upload["repo_id"],
                        repo_type="dataset"
                    )
                
                upload_time = time.time() - start_time if 'start_time' in locals() else 0
                pbar.write(f"Successfully uploaded to {url} in {upload_time:.1f} seconds")
            except Exception as e:
                pbar.write(f"Error uploading {local_file}: {str(e)}")
            
            # Upload corresponding README if it exists
            # Derive local README filename based on parquet filename
            local_readme = local_file.with_name(local_file.stem + "_README.md")
            if local_readme.exists():
                pbar.write(f"Uploading README {local_readme} to {upload['repo_id']}/README.md...")
                try:
                    url_readme = api.upload_file(
                        path_or_fileobj=str(local_readme),
                        path_in_repo="README.md",
                        repo_id=upload["repo_id"],
                        repo_type="dataset"
                    )
                    pbar.write(f"Successfully uploaded README to {url_readme}")
                except Exception as re:
                    pbar.write(f"Error uploading README {local_readme}: {re}")
            else:
                pbar.write(f"README {local_readme} not found, skipping README upload")
            pbar.update(1)
    
    print("\nUpload process completed")

if __name__ == "__main__":
    upload_parquet_files()
