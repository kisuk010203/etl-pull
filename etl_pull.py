#!/usr/bin/env python3
"""ETL Pull - Automatically sync course files from SNU myETL (Canvas LMS)."""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://myetl.snu.ac.kr"
CONFIG_PATH = Path.cwd() / ".etl_config.json"
STATE_PATH = Path.cwd() / ".etl_state.json"
SYNC_DIR = Path.cwd()


def load_config():
    if CONFIG_PATH.exists():
        return json.loads(CONFIG_PATH.read_text())
    return {}


def save_config(config):
    CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False))


def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {}


def save_state(state):
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def api_get(endpoint, token, params=None):
    """GET with automatic pagination."""
    url = f"{BASE_URL}/api/v1/{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    results = []
    while url:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 401:
            print("Error: Invalid or expired token. Run 'init' again.")
            sys.exit(1)
        if resp.status_code == 403:
            print(f"  Warning: Access denied for {endpoint}, skipping.")
            return results
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            results.extend(data)
        else:
            return data
        url = resp.links.get("next", {}).get("url")
        params = None  # params are already in the next URL
    return results


def sanitize_name(name):
    """Sanitize a course name for use as a directory name."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def get_current_courses(token):
    """Fetch active courses for the current enrollment."""
    courses = api_get(
        "courses",
        token,
        params={"enrollment_state": "active", "per_page": 100},
    )
    return [c for c in courses if c.get("name")]


def build_folder_map(token, course_id):
    """Map folder IDs to their full paths."""
    folders = api_get(f"courses/{course_id}/folders", token, params={"per_page": 100})
    folder_map = {}
    for f in folders:
        # full_name looks like "course files/Lecture Notes/Week 1"
        # Strip the leading "course files" prefix
        full = f.get("full_name", "")
        parts = full.split("/", 1)
        relative = parts[1] if len(parts) > 1 else ""
        folder_map[f["id"]] = relative
    return folder_map


def get_course_files(token, course_id):
    """Fetch all files for a course."""
    return api_get(
        f"courses/{course_id}/files", token, params={"per_page": 100}
    )


def download_file(url, dest_path, token):
    """Download a file to dest_path."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, stream=True)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def sync_course(token, course_id, course_dir, state, course_key):
    """Sync files for a single course. Returns count of new/updated files."""
    course_state = state.get(course_key, {})
    folder_map = build_folder_map(token, course_id)
    files = get_course_files(token, course_id)

    downloaded = 0
    skipped = 0

    for f in files:
        file_id = str(f["id"])
        updated_at = f.get("updated_at", "")
        display_name = f.get("display_name", f.get("filename", "unknown"))

        # Build relative path from folder
        folder_id = f.get("folder_id")
        folder_path = folder_map.get(folder_id, "")
        rel_path = os.path.join(folder_path, display_name) if folder_path else display_name
        dest = course_dir / rel_path

        # Skip if unchanged
        prev = course_state.get(file_id)
        if prev and prev.get("updated_at") == updated_at and dest.exists():
            skipped += 1
            continue

        print(f"    Downloading: {rel_path}")
        try:
            download_file(f["url"], dest, token)
            course_state[file_id] = {
                "updated_at": updated_at,
                "path": rel_path,
            }
            downloaded += 1
        except Exception as e:
            print(f"    Error downloading {rel_path}: {e}")

    state[course_key] = course_state
    return downloaded, skipped


def cmd_init(args):
    """Initialize: configure token, discover courses, download all files."""
    config = load_config()

    token = config.get("token")
    if not token or args.reauth:
        print("Go to https://myetl.snu.ac.kr/profile/settings")
        print("Under 'Approved Integrations', click '+ New Access Token'")
        print("Generate a token and paste it below.\n")
        token = input("Access Token: ").strip()
        if not token:
            print("No token provided. Aborting.")
            sys.exit(1)

    # Verify token
    print("\nVerifying token...")
    user = api_get("users/self", token)
    print(f"Authenticated as: {user.get('name', 'Unknown')}\n")

    # Fetch courses
    print("Fetching current courses...")
    courses = get_current_courses(token)
    if not courses:
        print("No active courses found.")
        sys.exit(1)

    # Let user pick courses or take all
    print(f"\nFound {len(courses)} active course(s):\n")
    for i, c in enumerate(courses, 1):
        print(f"  [{i}] {c['name']} (ID: {c['id']})")

    print(f"\n  [a] All courses")
    choice = input("\nSelect courses (comma-separated numbers, or 'a' for all): ").strip()

    if choice.lower() == "a" or choice == "":
        selected = courses
    else:
        indices = [int(x.strip()) - 1 for x in choice.split(",") if x.strip().isdigit()]
        selected = [courses[i] for i in indices if 0 <= i < len(courses)]

    if not selected:
        print("No courses selected. Aborting.")
        sys.exit(1)

    # Build course map
    course_map = {}
    for c in selected:
        dir_name = sanitize_name(c["name"])
        course_map[str(c["id"])] = {
            "name": c["name"],
            "dir_name": dir_name,
        }

    # Save config
    config["token"] = token
    config["courses"] = course_map
    save_config(config)

    # Create directories
    for cid, info in course_map.items():
        course_dir = SYNC_DIR / info["dir_name"]
        course_dir.mkdir(exist_ok=True)
        print(f"  Created: {info['dir_name']}/")

    # Initial sync
    print("\nDownloading files...\n")
    state = load_state()
    total_dl = 0
    for cid, info in course_map.items():
        print(f"  [{info['name']}]")
        course_dir = SYNC_DIR / info["dir_name"]
        dl, sk = sync_course(token, int(cid), course_dir, state, cid)
        total_dl += dl
        print(f"    -> {dl} downloaded, {sk} skipped\n")
    save_state(state)

    print(f"Done! {total_dl} file(s) downloaded.")


def cmd_pull(args):
    """Pull new/updated files for all configured courses."""
    config = load_config()
    token = config.get("token")
    course_map = config.get("courses")

    if not token or not course_map:
        print("Not initialized. Run 'init' first.")
        sys.exit(1)

    state = load_state()
    total_dl = 0

    print("Syncing courses...\n")
    for cid, info in course_map.items():
        print(f"  [{info['name']}]")
        course_dir = SYNC_DIR / info["dir_name"]
        course_dir.mkdir(parents=True, exist_ok=True)
        dl, sk = sync_course(token, int(cid), course_dir, state, cid)
        total_dl += dl
        print(f"    -> {dl} new/updated, {sk} unchanged\n")
    save_state(state)

    if total_dl == 0:
        print("Everything is up to date.")
    else:
        print(f"Done! {total_dl} file(s) downloaded.")


def cmd_status(args):
    """Show configured courses and file counts."""
    config = load_config()
    course_map = config.get("courses", {})
    state = load_state()

    if not course_map:
        print("Not initialized. Run 'init' first.")
        return

    print("Configured courses:\n")
    for cid, info in course_map.items():
        file_count = len(state.get(cid, {}))
        print(f"  {info['name']}")
        print(f"    ID: {cid} | Dir: courses/{info['dir_name']}/ | Files: {file_count}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Sync course files from SNU myETL"
    )
    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser("init", help="Initialize: set token, select courses, download files")
    p_init.add_argument("--reauth", action="store_true", help="Re-enter access token")

    sub.add_parser("pull", help="Pull new/updated files")
    sub.add_parser("status", help="Show configured courses")

    args = parser.parse_args()

    if args.command == "init":
        cmd_init(args)
    elif args.command == "pull":
        cmd_pull(args)
    elif args.command == "status":
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
