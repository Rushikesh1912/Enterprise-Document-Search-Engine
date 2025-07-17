import os
import shutil
import zipfile
import tempfile
import logging
import xml.etree.ElementTree as ET
from io import StringIO
import pandas as pd
import zipfile as zf
from docx2python import docx2python
import PyPDF2
from pptx import Presentation

# Set up logging (shared with app.py)
logger = logging.getLogger(__name__)

# Custom configuration for corrupt files
CORRUPT_DIR = "/Volumes/CORRUPTED/AI/corruptedfiles"
CORRUPT_ZIP_PATH = os.path.join(CORRUPT_DIR, "corrupt_files.zip")

def attempt_repair(file_path, ext):
    """Attempt to repair a corrupt file and return recovered content."""
    recovered_content = ""
    
    try:
        if ext == ".docx":
            with zipfile.ZipFile(file_path, 'r') as zf:
                if "word/document.xml" in zf.namelist():
                    with zf.open("word/document.xml") as f:
                        tree = ET.parse(f)
                        for elem in tree.iter():
                            if elem.text:
                                recovered_content += elem.text + " "
            logger.debug(f"Recovered partial DOCX content: {recovered_content[:50]}...")
        
        elif ext == ".pdf":
            with open(file_path, "rb") as f:
                raw_data = f.read()
                recovered_content = raw_data.decode("utf-8", errors="ignore")
            logger.debug(f"Recovered raw PDF text: {recovered_content[:50]}...")
        
        elif ext == ".xlsx":
            df = pd.read_excel(file_path, engine="openpyxl", nrows=10, on_bad_lines="skip")
            recovered_content = df.to_string()
            logger.debug(f"Recovered partial XLSX: {recovered_content[:50]}...")
        
        elif ext == ".csv":
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
                recovered_content = pd.read_csv(StringIO("".join(lines)), on_bad_lines="skip").to_string()
            logger.debug(f"Recovered partial CSV: {recovered_content[:50]}...")
        
        elif ext in {".txt", ".rtf", ".doc"}:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                recovered_content = f.read()
            logger.debug(f"Recovered raw text: {recovered_content[:50]}...")
        
        elif ext == ".pptx":
            with zipfile.ZipFile(file_path, 'r') as zf:
                slide_files = [f for f in zf.namelist() if f.startswith("ppt/slides/slide")]
                if slide_files:
                    with zf.open(slide_files[0]) as f:
                        tree = ET.parse(f)
                        for elem in tree.iter():
                            if elem.text:
                                recovered_content += elem.text + " "
            logger.debug(f"Recovered partial PPTX: {recovered_content[:50]}...")
        
        elif ext == ".md":
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                recovered_content = f.read()
            logger.debug(f"Recovered raw MD: {recovered_content[:50]}...")
        
        elif ext in {".png", ".jpeg", ".jpg"}:
            recovered_content = ""
            logger.debug(f"No repair needed for image: {file_path}")
        
        elif ext == ".xml":
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                recovered_content = f.read()
            logger.debug(f"Recovered raw XML: {recovered_content[:50]}...")
    
    except Exception as e:
        logger.error(f"Repair failed for {file_path}: {e}")
    
    return recovered_content.strip()

def extract_with_repair(file_path, original_extract_func):
    """
    Extract text, attempt repair if corrupt, and return result.
    Returns: (content, metadata or None, is_corrupt).
    """
    try:
        result = original_extract_func(file_path)
        if isinstance(result, tuple):
            content, metadata = result
        else:
            content, metadata = result, None
        
        if content or metadata:
            logger.debug(f"Successfully extracted {file_path}: {content[:50]}...")
            return content, metadata, False
        
        raise Exception("No content or metadata extracted")
    
    except Exception as e:
        logger.error(f"Corrupt file detected {file_path}: {e}")
        ext = os.path.splitext(file_path)[1].lower()
        repaired_content = attempt_repair(file_path, ext)
        
        if repaired_content:
            logger.debug(f"Repaired {file_path}: {repaired_content[:50]}...")
            return repaired_content, None, False
        else:
            return "", None, True

def handle_corrupt_file(file_path, corrupt_dir=CORRUPT_DIR):
    """Move corrupt file to specified corrupt_dir and return False to skip indexing."""
    if not os.path.exists(corrupt_dir):
        os.makedirs(corrupt_dir)
        logger.debug(f"Created corrupt directory: {corrupt_dir}")
    
    corrupt_dest = os.path.join(corrupt_dir, os.path.basename(file_path))
    try:
        shutil.move(file_path, corrupt_dest)
        logger.debug(f"Moved corrupt file to {corrupt_dest}")
        return False
    except Exception as e:
        logger.error(f"Error moving corrupt file {file_path}: {e}")
        return False

def zip_corrupt_files(socketio, corrupt_dir=CORRUPT_DIR, zip_path=CORRUPT_ZIP_PATH):
    """Zip files in corrupt_dir to zip_path and clean up, notifying via SocketIO."""
    if not os.listdir(corrupt_dir):
        return 0
    
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(corrupt_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, os.path.basename(file_path))
        corrupt_count = len(os.listdir(corrupt_dir))
        logger.debug(f"Created ZIP of {corrupt_count} corrupt files: {zip_path}")
        
        shutil.rmtree(corrupt_dir)
        os.makedirs(corrupt_dir)
        return corrupt_count
    
    except Exception as e:
        logger.error(f"Error zipping corrupt files: {e}")
        socketio.emit("index_complete", {"message": f"Error zipping corrupt files: {str(e)}"})
        return 0

if __name__ == "__main__":
    def dummy_extract(file_path):
        raise Exception("Simulated corruption")
    
    test_file = "test.docx"
    content, metadata, is_corrupt = extract_with_repair(test_file, dummy_extract)
    print(f"Content: {content}, Metadata: {metadata}, Is Corrupt: {is_corrupt}")
    if is_corrupt:
        handle_corrupt_file(test_file)
    corrupt_count = zip_corrupt_files(None)
    print(f"Zipped {corrupt_count} corrupt files")