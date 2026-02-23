#!/usr/bin/env python3
"""
Backfill: For tasks that have a numeric MySQL file_id (e.g. '22') but no
corresponding permit_files record in MongoDB, create a stub permit_files
document so the file appears on the Permit Files page.

Also initializes stage tracking for each backfilled file.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.mongodb import get_db
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def backfill():
    db = get_db()

    # Find all tasks with a file_id that has no permit_files record
    all_file_ids = db.tasks.distinct("file_id", {"file_id": {"$ne": None}})
    logger.info(f"Found {len(all_file_ids)} distinct file_ids in tasks collection")

    created = 0
    skipped = 0

    for fid in all_file_ids:
        fid_str = str(fid)
        if not fid_str:
            continue

        # Check if permit_files record already exists
        existing = db.permit_files.find_one({"file_id": fid_str})
        if existing:
            skipped += 1
            continue

        # Try to get MySQL data for numeric IDs
        permit_name = fid_str
        permit_address = ""
        mysql_id = fid_str if fid_str.isdigit() else None

        if mysql_id:
            try:
                from app.db.mysql import mysql_service
                mysql_data = mysql_service.get_permit_by_id(mysql_id)
                if mysql_data:
                    permit_name = str(mysql_data.get("name") or mysql_data.get("file_name") or fid_str)
                    permit_address = str(mysql_data.get("address") or "")
                    logger.info(f"  MySQL data found for id={mysql_id}: name='{permit_name}' address='{permit_address}'")
                else:
                    logger.warning(f"  No MySQL data for id={mysql_id}, using stub")
            except Exception as e:
                logger.warning(f"  MySQL lookup failed for {mysql_id}: {e}")

        # Determine stage from existing tasks for this file
        tasks_for_file = list(db.tasks.find(
            {"file_id": fid_str},
            {"stage": 1, "status": 1}
        ))
        stages = [t.get("stage") for t in tasks_for_file if t.get("stage")]
        current_stage = stages[0] if stages else "PRELIMS"

        # Map stage to status
        stage_to_status = {
            "PRELIMS": "IN_PRELIMS",
            "PRODUCTION": "IN_PRODUCTION",
            "QC": "IN_QC",
            "COMPLETED": "COMPLETED",
            "DELIVERED": "DELIVERED",
        }
        status = stage_to_status.get(current_stage, "IN_PRELIMS")

        pf_doc = {
            "file_id": fid_str,
            "mysql_id": mysql_id,
            "file_name": permit_name,
            "file_info": {
                "original_filename": permit_name,
                "stored_filename": permit_name,
                "file_size": None,
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
                "file_path": None,
            },
            "project_details": {
                "client_name": permit_name,
                "project_name": permit_address or permit_name,
            },
            "address": permit_address,
            "status": status,
            "workflow_step": current_stage,
            "assigned_to_lead": None,
            "current_stage": current_stage,
            "metadata": {
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "uploaded_by": "backfill_script",
                "source": "mysql_backfill",
            },
            "updated_at": datetime.now(timezone.utc),
        }

        db.permit_files.insert_one(pf_doc)
        created += 1
        logger.info(f"  Created permit_files: file_id={fid_str} name='{permit_name}' stage={current_stage}")

    logger.info(f"\nBackfill complete: {created} created, {skipped} already existed")

if __name__ == "__main__":
    backfill()
