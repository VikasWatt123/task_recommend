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


def build_ops(docs: List[Dict[str, Any]]) -> List[UpdateOne]:
    ops: List[UpdateOne] = []

    for doc in docs:
        _id = doc.get("_id")
        if _id is None:
            continue

        set_ops: Dict[str, Any] = {}

        if doc.get("kekaemployeenumber") is None and doc.get("employee_code") is not None:
            set_ops["kekaemployeenumber"] = doc.get("employee_code")

        if doc.get("fullname") is None and doc.get("employee_name") is not None:
            set_ops["fullname"] = doc.get("employee_name")

        if doc.get("email") is None and doc.get("contact_email") is not None:
            set_ops["email"] = doc.get("contact_email")

        if set_ops:
            ops.append(UpdateOne({"_id": _id}, {"$set": set_ops}))

    return ops


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backwards-compatible MongoDB migration: populate MySQL-aligned fields in the employee collection. "
            "Does NOT remove old fields."
        )
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute counts only; do not write to MongoDB")
    parser.add_argument("--batch-size", type=int, default=500, help="Bulk write batch size")
    args = parser.parse_args()

    db = get_db()

    total = db.employee.count_documents({})
    need_keka = db.employee.count_documents({"kekaemployeenumber": {"$exists": False}, "employee_code": {"$exists": True}})
    need_fullname = db.employee.count_documents({"fullname": {"$exists": False}, "employee_name": {"$exists": True}})
    need_email = db.employee.count_documents({"email": {"$exists": False}, "contact_email": {"$exists": True}})

    print("=== MongoDB Employee Field Compatibility Migration ===")
    print(f"Total employee docs: {total}")
    print(f"Docs missing kekaemployeenumber (but have employee_code): {need_keka}")
    print(f"Docs missing fullname (but have employee_name): {need_fullname}")
    print(f"Docs missing email (but have contact_email): {need_email}")
    print(f"Dry run: {args.dry_run}")

    if args.dry_run:
        return

    cursor = db.employee.find(
        {
            "$or": [
                {"kekaemployeenumber": {"$exists": False}, "employee_code": {"$exists": True}},
                {"fullname": {"$exists": False}, "employee_name": {"$exists": True}},
                {"email": {"$exists": False}, "contact_email": {"$exists": True}},
            ]
        },
        {
            "employee_code": 1,
            "employee_name": 1,
            "contact_email": 1,
            "kekaemployeenumber": 1,
            "fullname": 1,
            "email": 1,
        },
    )

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
    after_need_keka = db.employee.count_documents({"kekaemployeenumber": {"$exists": False}, "employee_code": {"$exists": True}})
    after_need_fullname = db.employee.count_documents({"fullname": {"$exists": False}, "employee_name": {"$exists": True}})
    after_need_email = db.employee.count_documents({"email": {"$exists": False}, "contact_email": {"$exists": True}})

    print("\n=== Post-check ===")
    print(f"Still missing kekaemployeenumber (but have employee_code): {after_need_keka}")
    print(f"Still missing fullname (but have employee_name): {after_need_fullname}")
    print(f"Still missing email (but have contact_email): {after_need_email}")


if __name__ == "__main__":
    main()
