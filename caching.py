import os
import hashlib
import pickle
import logging
from datetime import datetime, timedelta

# Set up logging (shared with app.py)
logger = logging.getLogger(__name__)

# Cache configuration
INDEX_CACHE_DIR = "/Volumes/CORRUPTED/AI/cache"  # Updated to your specified path
INDEX_CACHE_TIMEOUT = timedelta(days=7)  # Cache text for 7 days
MAX_INDEX_CACHE_SIZE = 1000  # Maximum number of cached files

# Ensure cache directory exists
if not os.path.exists(INDEX_CACHE_DIR):
    os.makedirs(INDEX_CACHE_DIR)
    logger.debug(f"Created cache directory: {INDEX_CACHE_DIR}")

def generate_cache_key(file_path):
    """Generate a unique cache key based on file path and last modification time."""
    try:
        mtime = os.path.getmtime(file_path)
        key = f"{file_path}_{mtime}"
        cache_key = hashlib.md5(key.encode()).hexdigest() + ".cache"
        logger.debug(f"Generated cache key for {file_path}: {cache_key}")
        return cache_key
    except Exception as e:
        logger.error(f"Error generating cache key for {file_path}: {e}")
        return None

def get_cache_path(cache_key):
    """Return the full path to the cached file."""
    return os.path.join(INDEX_CACHE_DIR, cache_key)

def is_cache_valid(cache_path):
    """Check if the cached text exists and is within the timeout period."""
    if not os.path.exists(cache_path):
        return False
    try:
        cache_mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
        is_valid = datetime.now() - cache_mtime < INDEX_CACHE_TIMEOUT
        logger.debug(f"Cache at {cache_path} is {'valid' if is_valid else 'expired'}")
        return is_valid
    except Exception as e:
        logger.error(f"Error checking cache validity for {cache_path}: {e}")
        return False

def load_cached_content(cache_path):
    """Load cached text or tuple from file."""
    try:
        with open(cache_path, "rb") as f:
            content = pickle.load(f)
        logger.debug(f"Loaded cached content from {cache_path}: {str(content)[:50]}...")
        return content
    except Exception as e:
        logger.error(f"Error loading cached content from {cache_path}: {e}")
        return None

def save_to_cache(cache_path, content):
    """Save extracted content to cache."""
    try:
        with open(cache_path, "wb") as f:
            pickle.dump(content, f)
        logger.debug(f"Saved content to cache: {cache_path}")
    except Exception as e:
        logger.error(f"Error saving to cache {cache_path}: {e}")

def cleanup_cache():
    """Remove old files if cache exceeds MAX_INDEX_CACHE_SIZE."""
    try:
        files = sorted(
            [(f, os.path.getmtime(os.path.join(INDEX_CACHE_DIR, f))) for f in os.listdir(INDEX_CACHE_DIR)],
            key=lambda x: x[1]
        )
        if len(files) > MAX_INDEX_CACHE_SIZE:
            for file, _ in files[:len(files) - MAX_INDEX_CACHE_SIZE]:
                try:
                    os.remove(os.path.join(INDEX_CACHE_DIR, file))
                    logger.debug(f"Removed old cache file: {file}")
                except Exception as e:
                    logger.error(f"Error removing cache file {file}: {e}")
    except Exception as e:
        logger.error(f"Error cleaning up cache: {e}")

def get_cached_or_extract(file_path, extract_func):
    """
    Retrieve cached content or extract it if not cached.
    extract_func: The original extract_text function from app.py.
    Returns: The content (string or tuple), was_cached (bool).
    """
    cache_key = generate_cache_key(file_path)
    if not cache_key:
        return extract_func(file_path), False
    
    cache_path = get_cache_path(cache_key)
    if is_cache_valid(cache_path):
        cached_content = load_cached_content(cache_path)
        if cached_content is not None:
            return cached_content, True
    
    # Extract if not cached or cache is invalid
    content = extract_func(file_path)
    if content:  # Only cache if extraction succeeds
        save_to_cache(cache_path, content)
        cleanup_cache()
    return content, False

if __name__ == "__main__":
    # Simple test (for debugging purposes, not required in production)
    def dummy_extract(file_path):
        return f"Dummy content for {file_path}"
    
    test_file = "test.txt"
    content, was_cached = get_cached_or_extract(test_file, dummy_extract)
    print(f"Content: {content}, Cached: {was_cached}")
    content, was_cached = get_cached_or_extract(test_file, dummy_extract)
    print(f"Content: {content}, Cached: {was_cached}")