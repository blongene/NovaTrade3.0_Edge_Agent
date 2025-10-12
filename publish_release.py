import os, sys, time
from pathlib import Path
from github import Github

# Usage: python publish_release.py <tag> <path_to_zip>
# Example: python publish_release.py v2025-08-21 NovaTrade_Phase17_18_Pack_2025-08-21_v2.zip

TAG   = sys.argv[1]
ZIP   = Path(sys.argv[2]).resolve()
NAME  = ZIP.name
TOKEN = os.environ["GITHUB_TOKEN"]
REPO  = os.environ["GITHUB_REPO"]  # e.g. yourname/NovaTrade-Builds

g = Github(TOKEN)
repo = g.get_repo(REPO)

# Create (or reuse) release
try:
    rel = repo.get_release(TAG)
except:
    rel = repo.create_git_release(TAG, f"Build {TAG}", "Automated build upload", draft=False, prerelease=False)

# Upload asset
with open(ZIP, "rb") as f:
    rel.upload_asset_from_memory(f.read(), NAME, content_type="application/zip")

print(f"✅ Uploaded {NAME} to release {TAG}")
