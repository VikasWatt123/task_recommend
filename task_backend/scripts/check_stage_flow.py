#!/usr/bin/env python3
"""Check stage tracking flow for file_id=22 and other files"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.db.mongodb import get_db

db = get_db()

# Check tasks for file_id=22 - source.permit_file_id vs file_id
print("=== Tasks for file_id=22 ===")
tasks = list(db.tasks.find({"file_id": "22"}, {"task_id":1,"title":1,"stage":1,"status":1,"assigned_to":1,"source":1}))
for t in tasks:
    src = t.get("source", {})
    print(f"  task={t.get('task_id')} stage={t.get('stage')} status={t.get('status')} assigned_to={t.get('assigned_to')}")
    print(f"    source.permit_file_id={src.get('permit_file_id')} file_id={t.get('file_id')}")

# Check file_tracking for file_id=22
print("\n=== file_tracking for file_id=22 ===")
ft = db.file_tracking.find_one({"file_id": "22"})
if ft:
    ft.pop("_id", None)
    print(f"  current_stage={ft.get('current_stage')} current_status={ft.get('current_status')}")
    for h in ft.get("stage_history", []):
        print(f"    history: stage={h.get('stage')} status={h.get('status')}")
else:
    print("  NO file_tracking record for file_id=22")

# Check permit_files for file_id=22
print("\n=== permit_files for file_id=22 ===")
pf = db.permit_files.find_one({"file_id": "22"})
if pf:
    pf.pop("_id", None)
    print(f"  file_name={pf.get('file_name')} status={pf.get('status')} current_stage={pf.get('current_stage')}")
    print(f"  project_details={pf.get('project_details')}")
else:
    print("  NO permit_files record for file_id=22")

# Check source.permit_file_id alignment
print("\n=== source.permit_file_id vs file_id alignment (sample 10 tasks) ===")
tasks_all = list(db.tasks.find(
    {"file_id": {"$ne": None}},
    {"task_id":1,"file_id":1,"source.permit_file_id":1}
).limit(10))
for t in tasks_all:
    fid = t.get("file_id")
    src_fid = t.get("source", {}).get("permit_file_id")
    match = "OK" if fid == src_fid else "MISMATCH"
    print(f"  {match} | file_id={fid} | source.permit_file_id={src_fid}")
