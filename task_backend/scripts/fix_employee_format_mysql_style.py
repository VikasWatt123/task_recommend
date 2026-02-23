from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

from pymongo import UpdateOne

# Ensure the project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.mongodb import get_db


def normalize_kekaemployeenumber(code: str) -> str:
    """Convert employee code to MySQL format: zero-padded 4 digits"""
    if not code:
        return code
    
    # Remove any existing leading zeros, then pad to 4 digits
    clean_code = str(code).strip().lstrip('0')
    if clean_code == '':
        clean_code = '0'
    
    return clean_code.zfill(4)


def normalize_fullname(name: str) -> str:
    """Normalize fullname to match MySQL style"""
    if not name:
        return name
    
    # Basic cleanup: strip extra whitespace
    return str(name).strip()


def build_ops(docs: List[Dict[str, Any]]) -> List[UpdateOne]:
    ops: List[UpdateOne] = []

    for doc in docs:
        _id = doc.get("_id")
        if _id is None:
            continue

        set_ops: Dict[str, Any] = {}

        # Fix kekaemployeenumber - zero pad to 4 digits
        current_keka = doc.get("kekaemployeenumber", "")
        if current_keka:
            normalized_keka = normalize_kekaemployeenumber(current_keka)
            if normalized_keka != current_keka:
                set_ops["kekaemployeenumber"] = normalized_keka

        # Fix fullname - basic cleanup
        current_fullname = doc.get("fullname", "")
        if current_fullname:
            normalized_fullname = normalize_fullname(current_fullname)
            if normalized_fullname != current_fullname:
                set_ops["fullname"] = normalized_fullname

        if set_ops:
            ops.append(UpdateOne({"_id": _id}, {"$set": set_ops}))

    return ops


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fix MongoDB employee fields to match MySQL format: "
            "zero-pad kekaemployeenumber to 4 digits and normalize fullname."
        )
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be changed without applying updates")
    parser.add_argument("--batch-size", type=int, default=500, help="Bulk write batch size")
    args = parser.parse_args()

    db = get_db()

    total = db.employee.count_documents({})
    print("=== MongoDB Employee Format Fix (MySQL Style) ===")
    print(f"Total employee docs: {total}")
    print(f"Dry run: {args.dry_run}")
    print()

    # Get sample to show current vs target format
    sample_docs = list(db.employee.find({}, {"kekaemployeenumber": 1, "fullname": 1, "_id": 0}).limit(10))
    
    print("📋 Sample current format -> target format:")
    for doc in sample_docs:
        current_keka = doc.get("kekaemployeenumber", "")
        current_fullname = doc.get("fullname", "")
        
        target_keka = normalize_kekaemployeenumber(current_keka) if current_keka else current_keka
        target_fullname = normalize_fullname(current_fullname) if current_fullname else current_fullname
        
        keka_change = " -> " if current_keka != target_keka else "   "
        fullname_change = " -> " if current_fullname != target_fullname else "   "
        
        print(f"  kekaemployeenumber: \"{current_keka}\"{keka_change}\"{target_keka}\"")
        print(f"  fullname:          \"{current_fullname}\"{fullname_change}\"{target_fullname}\"")
        print()

    if args.dry_run:
        # Count how many need changes
        need_keka_fix = 0
        need_fullname_fix = 0
        
        for doc in db.employee.find({}, {"kekaemployeenumber": 1, "fullname": 1}):
            keka = doc.get("kekaemployeenumber", "")
            fullname = doc.get("fullname", "")
            
            if keka and normalize_kekaemployeenumber(keka) != keka:
                need_keka_fix += 1
            
            if fullname and normalize_fullname(fullname) != fullname:
                need_fullname_fix += 1
        
        print("📊 Dry-run statistics:")
        print(f"  • Docs needing kekaemployeenumber fix: {need_keka_fix}")
        print(f"  • Docs needing fullname fix: {need_fullname_fix}")
        return

    # Apply changes
    cursor = db.employee.find({})
    
    modified_total = 0
    matched_total = 0

    batch: List[Dict[str, Any]] = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= args.batch_size:
            ops = build_ops(batch)
            batch = []
            if not ops:
                continue
            result = db.employee.bulk_write(ops, ordered=False)
            matched_total += result.matched_count
            modified_total += result.modified_count

    if batch:
        ops = build_ops(batch)
        if ops:
            result = db.employee.bulk_write(ops, ordered=False)
            matched_total += result.matched_count
            modified_total += result.modified_count

    print("\n=== Migration Results ===")
    print(f"Matched docs: {matched_total}")
    print(f"Modified docs: {modified_total}")

    # Post-check
    print("\n=== Post-check Sample ===")
    post_check_docs = list(db.employee.find({}, {"kekaemployeenumber": 1, "fullname": 1, "_id": 0}).limit(10))
    
    for doc in post_check_docs:
        keka = doc.get("kekaemployeenumber", "")
        fullname = doc.get("fullname", "")
        print(f"  • kekaemployeenumber: \"{keka}\" | fullname: \"{fullname}\"")


if __name__ == "__main__":
    main()
