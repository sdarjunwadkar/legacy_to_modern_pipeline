import hashlib
import json
import os

CACHE_FILE = "logs/file_hash_cache.json"

def compute_md5(file_path):
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

def check_and_update_cache(file_path, cache_file=CACHE_FILE):
    """
    Check if the file has changed based on its MD5 hash.
    Updates the cache if it has changed.
    Returns True if changed, False otherwise.
    """
    current_hash = compute_md5(file_path)

    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            cache = json.load(f)
    else:
        cache = {}

    old_hash = cache.get(file_path)
    if old_hash != current_hash:
        # File has changed, update cache
        cache[file_path] = current_hash
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump(cache, f, indent=2)
        return True

    return False