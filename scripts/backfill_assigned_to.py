"""
Backfill Migration Script: Fix assigned_to field in existing task documents.

The assign_task function was writing employee_code instead of assigned_to,
leaving assigned_to as null for all assigned tasks. This script:

1. Finds all tasks where assigned_to is null but employee_code is set
2. Copies employee_code value to assigned_to
3. Looks up employee_name and sets assigned_to_name
4. Reports results

Usage:
    python scripts/backfill_assigned_to.py [--dry-run]
"""

import sys
import os
import argparse
import logging

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db.mongodb import get_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def backfill_assigned_to(dry_run: bool = False):
    db = get_db()

    # Find tasks where assigned_to is null/missing but employee_code is set
    query = {
        "$and": [
            {"$or": [
                {"assigned_to": None},
                {"assigned_to": {"$exists": False}},
            ]},
            {"employee_code": {"$exists": True, "$ne": None}},
        ]
    }

    affected_tasks = list(db.tasks.find(query, {
        "_id": 1,
        "task_id": 1,
        "employee_code": 1,
        "status": 1,
    }))

    logger.info(f"Found {len(affected_tasks)} tasks with null assigned_to but employee_code set")

    if not affected_tasks:
        logger.info("Nothing to backfill. All tasks are consistent.")
        return

    # Build employee name lookup
    employee_codes = list(set(t.get("employee_code") for t in affected_tasks if t.get("employee_code")))
    employees = list(db.employee.find(
        {"$or": [
            {"employee_code": {"$in": employee_codes}},
            {"kekaemployeenumber": {"$in": employee_codes}},
        ]},
        {"_id": 0, "employee_code": 1, "kekaemployeenumber": 1, "employee_name": 1}
    ))
    name_lookup = {}
    for emp in employees:
        code = emp.get("employee_code") or emp.get("kekaemployeenumber")
        if code:
            name_lookup[code] = emp.get("employee_name", "Unknown")

    updated = 0
    skipped = 0
    for task in affected_tasks:
        emp_code = task.get("employee_code")
        emp_name = name_lookup.get(emp_code, "Unknown")
        task_id = task.get("task_id", str(task.get("_id")))

        if dry_run:
            logger.info(f"[DRY-RUN] Would set assigned_to={emp_code}, assigned_to_name={emp_name} on task {task_id}")
            updated += 1
            continue

        result = db.tasks.update_one(
            {"_id": task["_id"]},
            {"$set": {
                "assigned_to": emp_code,
                "assigned_to_name": emp_name,
            }}
        )
        if result.modified_count:
            updated += 1
            logger.info(f"Updated task {task_id}: assigned_to={emp_code}, assigned_to_name={emp_name}")
        else:
            skipped += 1
            logger.warning(f"Skipped task {task_id}: no modification")

    logger.info(f"Backfill complete: {updated} updated, {skipped} skipped out of {len(affected_tasks)} total")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill assigned_to field in tasks collection")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to database")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY RUN MODE — no changes will be written ===")

    backfill_assigned_to(dry_run=args.dry_run)
