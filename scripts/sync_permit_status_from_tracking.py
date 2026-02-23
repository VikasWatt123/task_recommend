#!/usr/bin/env python3
"""
Two-part sync script:
1. For every permit_files record, sync its status/current_stage from file_tracking
   (so backfilled records show the real stage, not the stub IN_PRELIMS).
2. For every task that has source.permit_file_id but no file_id, backfill file_id.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.db.mongodb import get_db
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

STAGE_TO_STATUS = {
    "PRELIMS":    "IN_PRELIMS",
    "PRODUCTION": "IN_PRODUCTION",
    "COMPLETED":  "COMPLETED",
    "QC":         "IN_QC",
    "DELIVERED":  "DELIVERED",
}

def part1_sync_permit_status():
    db = get_db()
    logger.info("=== Part 1: Sync permit_files status from file_tracking ===")
    updated = 0
    skipped = 0

    all_pf = list(db.permit_files.find({}, {"file_id": 1, "status": 1, "current_stage": 1}))
    for pf in all_pf:
        fid = pf.get("file_id")
        if not fid:
            continue
        ft = db.file_tracking.find_one({"file_id": fid}, {"current_stage": 1, "current_status": 1})
        if not ft:
            skipped += 1
            continue
        real_stage = ft.get("current_stage") or "PRELIMS"
        real_status = STAGE_TO_STATUS.get(real_stage, "IN_PRELIMS")
        # Only update if different
        if pf.get("current_stage") != real_stage or pf.get("status") != real_status:
            db.permit_files.update_one(
                {"file_id": fid},
                {"$set": {"current_stage": real_stage, "status": real_status}}
            )
            logger.info(f"  Updated {fid}: {pf.get('status')} → {real_status} (stage {real_stage})")
            updated += 1
        else:
            skipped += 1

    logger.info(f"Part 1 done: {updated} updated, {skipped} already correct or no tracking")


def part2_backfill_file_id():
    db = get_db()
    logger.info("\n=== Part 2: Backfill file_id on tasks that only have source.permit_file_id ===")
    updated = 0

    # Find tasks where file_id is None/missing but source.permit_file_id is set
    tasks = list(db.tasks.find(
        {
            "source.permit_file_id": {"$ne": None, "$exists": True},
            "$or": [{"file_id": None}, {"file_id": {"$exists": False}}]
        },
        {"task_id": 1, "source.permit_file_id": 1}
    ))
    logger.info(f"Found {len(tasks)} tasks needing file_id backfill")

    for t in tasks:
        src_fid = t.get("source", {}).get("permit_file_id")
        if src_fid:
            db.tasks.update_one(
                {"task_id": t["task_id"]},
                {"$set": {"file_id": str(src_fid)}}
            )
            updated += 1
            logger.info(f"  Backfilled task {t['task_id']}: file_id={src_fid}")

    logger.info(f"Part 2 done: {updated} tasks backfilled")


if __name__ == "__main__":
    part1_sync_permit_status()
    part2_backfill_file_id()
    print("\nAll done.")
