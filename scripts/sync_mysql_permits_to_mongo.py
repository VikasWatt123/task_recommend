#!/usr/bin/env python3
"""
Sync MySQL permits table into MongoDB permit_files collection.
MySQL permits table has: id, name, address, ... columns.
Creates a MongoDB permit_files document for each MySQL permit that doesn't already exist.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.mongodb import get_db
from app.db.mysql import mysql_service
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sync_mysql_permits():
    db = get_db()
    
    try:
        permits = mysql_service.get_permit_files()
    except Exception as e:
        logger.error(f"Failed to fetch MySQL permits: {e}")
        return

    logger.info(f"Found {len(permits)} permits in MySQL")
    if not permits:
        return

    logger.info(f"MySQL permit columns: {list(permits[0].keys())}")

    created = 0
    skipped = 0

    for permit in permits:
        mysql_id = str(permit.get("id") or "")
        name = permit.get("name") or permit.get("file_name") or f"Permit-{mysql_id}"
        address = permit.get("address") or ""
        status = permit.get("status") or "PENDING"

        if not mysql_id:
            continue

        # Check if already synced (by mysql_id field)
        existing = db.permit_files.find_one({"mysql_id": mysql_id})
        if existing:
            skipped += 1
            continue

        # Also check if a file_id like "PF-MYSQL-{id}" already exists
        synthetic_file_id = f"PF-MYSQL-{mysql_id}"
        existing2 = db.permit_files.find_one({"file_id": synthetic_file_id})
        if existing2:
            # Backfill mysql_id if missing
            db.permit_files.update_one({"file_id": synthetic_file_id}, {"$set": {"mysql_id": mysql_id}})
            skipped += 1
            continue

        # Create a new permit_files document from MySQL data
        doc = {
            "file_id": synthetic_file_id,
            "mysql_id": mysql_id,
            "file_name": name,
            "file_info": {
                "original_filename": name,
                "stored_filename": name,
                "file_size": None,
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
                "file_path": None,
            },
            "project_details": {
                "client_name": permit.get("client_name") or permit.get("client") or name,
                "project_name": permit.get("project_name") or address or name,
            },
            "status": status,
            "workflow_step": permit.get("workflow_step") or "PRELIMS",
            "assigned_to_lead": permit.get("assigned_to_lead") or None,
            "address": address,
            "metadata": {
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "uploaded_by": "mysql_sync",
                "source": "mysql",
            },
            "current_stage": "PRELIMS",
            "updated_at": datetime.now(timezone.utc),
        }

        db.permit_files.insert_one(doc)
        created += 1
        logger.info(f"  Created: {synthetic_file_id} | name={name}")

    logger.info(f"\nSync complete: {created} created, {skipped} skipped")

    # Also update tasks that reference numeric MySQL IDs to use the new synthetic file_id
    logger.info("\nUpdating tasks with numeric MySQL file_ids...")
    updated_tasks = 0
    for permit in permits:
        mysql_id = str(permit.get("id") or "")
        if not mysql_id:
            continue
        synthetic_file_id = f"PF-MYSQL-{mysql_id}"
        result = db.tasks.update_many(
            {"file_id": mysql_id},
            {"$set": {"file_id": synthetic_file_id, "mysql_file_id": mysql_id}}
        )
        if result.modified_count > 0:
            logger.info(f"  Updated {result.modified_count} tasks: file_id {mysql_id} -> {synthetic_file_id}")
            updated_tasks += result.modified_count

    logger.info(f"Updated {updated_tasks} tasks with synthetic file_ids")

if __name__ == "__main__":
    sync_mysql_permits()
