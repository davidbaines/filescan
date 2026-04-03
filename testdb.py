from pathlib import Path
from database import FileDB

if __name__ == "__main__":
    db = FileDB("/tmp/test_file_index.db")

    db.upsert_folder("/home/user/docs", "C:", file_count=3, total_bytes=1024, depth=2)

    db.commit()

    fid = db.get_folder_id("/home/user/docs")

    print(f"Folder ID: {fid}")

    db.close()

    Path("/tmp/test_file_index.db").unlink()
