import hashlib
import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from stock_collector.config.settings import get_path

# 数据目录与备份目录
DATA_DIR = get_path("data_dir")
BACKUP_DIR = get_path("backup_dir")


# 计算文件 SHA256
def _file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


# 创建备份包（数据库与汇总）
def create_backup_bundle(date_value: str) -> Path:
    # 创建备份目录
    backup_path = BACKUP_DIR / date_value
    backup_path.mkdir(parents=True, exist_ok=True)

    db_path = get_path("db_path")
    summary_path = get_path("summary_dir") / f"{date_value}.json"

    # 复制需要备份的文件
    files = []
    for src in [db_path, summary_path]:
        if src.exists():
            dest = backup_path / src.name
            shutil.copy2(src, dest)
            files.append(dest)

    # 生成清单文件
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


# 清理过期备份
def cleanup_backups(retention_days: int = 30) -> None:
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
