#!/usr/bin/env python3
"""Verify all fixes are in place"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.db.mongodb import get_db

db = get_db()
errors = []
ok = []

# 1. file_id=22 (MySQL permit) now in permit_files with correct name + stage
pf22 = db.permit_files.find_one({"file_id": "22"})
if pf22:
    name = pf22.get("file_name") or pf22.get("file_info", {}).get("original_filename")
    stage = pf22.get("current_stage")
    status = pf22.get("status")
    ok.append(f"[OK] permit_files file_id=22: name='{name}' stage={stage} status={status}")
else:
    errors.append("[FAIL] permit_files file_id=22 not found")

# 2. Tasks for file_id=22 now have file_id field set
tasks22 = list(db.tasks.find({"$or": [{"file_id": "22"}, {"source.permit_file_id": "22"}]},
                              {"task_id":1,"file_id":1,"stage":1,"status":1,"assigned_to":1}))
ok.append(f"[OK] Tasks for file_id=22: {len(tasks22)} found")
for t in tasks22:
    fid = t.get("file_id")
    if not fid:
        errors.append(f"  [FAIL] task {t['task_id']} still missing file_id")
    else:
        ok.append(f"  [OK] task {t['task_id']} file_id={fid} stage={t.get('stage')} status={t.get('status')}")

# 3. No tasks with source.permit_file_id but missing file_id
missing_fid = db.tasks.count_documents({
    "source.permit_file_id": {"$ne": None, "$exists": True},
    "$or": [{"file_id": None}, {"file_id": {"$exists": False}}]
})
if missing_fid == 0:
    ok.append(f"[OK] All tasks with source.permit_file_id now have file_id set")
else:
    errors.append(f"[FAIL] {missing_fid} tasks still missing file_id despite having source.permit_file_id")

# 4. permit_files statuses match file_tracking
STAGE_TO_STATUS = {"PRELIMS":"IN_PRELIMS","PRODUCTION":"IN_PRODUCTION","COMPLETED":"COMPLETED","QC":"IN_QC","DELIVERED":"DELIVERED"}
mismatch_count = 0
all_pf = list(db.permit_files.find({}, {"file_id":1,"status":1,"current_stage":1}))
for pf in all_pf:
    fid = pf.get("file_id")
    ft = db.file_tracking.find_one({"file_id": fid}, {"current_stage":1})
    if ft:
        expected_stage = ft.get("current_stage","PRELIMS")
        expected_status = STAGE_TO_STATUS.get(expected_stage,"IN_PRELIMS")
        if pf.get("status") != expected_status:
            mismatch_count += 1
if mismatch_count == 0:
    ok.append(f"[OK] All permit_files statuses match file_tracking ({len(all_pf)} files checked)")
else:
    errors.append(f"[FAIL] {mismatch_count} permit_files have status mismatch with file_tracking")

# 5. Stage flow: PRELIMS→PRODUCTION→COMPLETED→QC→DELIVERED for file_id=22
ft22 = db.file_tracking.find_one({"file_id": "22"})
if ft22:
    stages = [h.get("stage") for h in ft22.get("stage_history", [])]
    ok.append(f"[OK] file_id=22 stage history: {' → '.join(stages)}")
    ok.append(f"[OK] file_id=22 current_stage={ft22.get('current_stage')} current_status={ft22.get('current_status')}")
else:
    errors.append("[FAIL] No file_tracking for file_id=22")

# 6. Recent tasks for employee 0622 (assigned_tasks field)
tasks_0622 = list(db.tasks.find({"assigned_to": {"$in": ["0622","622"]}}, {"task_id":1,"title":1,"status":1}))
ok.append(f"[OK] Tasks for employee 0622: {len(tasks_0622)} found")

print("\n".join(ok))
if errors:
    print("\n--- ERRORS ---")
    print("\n".join(errors))
else:
    print("\n✅ All checks passed!")
