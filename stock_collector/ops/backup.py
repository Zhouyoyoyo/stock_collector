import hashlib
import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path


DATA_DIR = Path("stock_collector/data")
BACKUP_DIR = DATA_DIR / "backup"


def _file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def create_backup_bundle(date_value: str) -> Path:
    """创建备份目录并生成清单。"""
    backup_path = BACKUP_DIR / date_value
    backup_path.mkdir(parents=True, exist_ok=True)

    db_path = DATA_DIR / "stock_daily.db"
    summary_path = DATA_DIR / "summary" / f"{date_value}.json"

    files = []
    for src in [db_path, summary_path]:
        if src.exists():
            dest = backup_path / src.name
            shutil.copy2(src, dest)
            files.append(dest)

    manifest = {
        "date": date_value,
        "created_at": datetime.utcnow().isoformat(),
        "files": [
            {
                "name": file_path.name,
                "size": file_path.stat().st_size,
                "sha256": _file_hash(file_path),
            }
            for file_path in files
        ],
    }
    manifest_path = backup_path / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as file_handle:
        json.dump(manifest, file_handle, ensure_ascii=False, indent=2)
    return backup_path


def cleanup_backups(retention_days: int = 30) -> None:
    """清理过期备份。"""
    if not BACKUP_DIR.exists():
        return
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    for path in BACKUP_DIR.iterdir():
        if not path.is_dir():
            continue
        try:
            date_value = datetime.strptime(path.name, "%Y-%m-%d")
        except ValueError:
            continue
        if date_value < cutoff:
            shutil.rmtree(path, ignore_errors=True)
