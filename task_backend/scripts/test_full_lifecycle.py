#!/usr/bin/env python3
"""
End-to-end lifecycle test for the Smart Task Assignee system.
Tests the complete flow:
  1. Address → ZIP → Team Lead selection
  2. Task creation with file_id (MySQL permit)
  3. Task assignment to employee → stage tracking initialized
  4. Employee profile shows task in Recent Activity
  5. Permit Files page shows file with correct name/stage
  6. Task Board shows assigned task
  7. Employee completes task → PRELIMS→PRODUCTION auto-progression
  8. PRELIMS duplicate guard blocks second PRELIMS task
  9. PRODUCTION task created → assigned → completed → COMPLETED auto-progression
 10. QC task created → assigned → completed → DELIVERED auto-progression
 11. Time tracking per stage per employee verified
 12. Team Lead Board shows correct stats
"""
import sys, os, time, requests, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.mongodb import get_db
from datetime import datetime, timezone

BASE = "http://localhost:8000/api/v1"
db = get_db()

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

results = []

def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((status, label, detail))
    print(f"  {status} {label}" + (f" | {detail}" if detail else ""))
    return condition

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def api(method, path, timeout=60, **kwargs):
    try:
        r = getattr(requests, method)(f"{BASE}{path}", timeout=timeout, **kwargs)
        return r
    except Exception as e:
        return type("R", (), {"status_code": 0, "text": str(e), "json": lambda self: {}})()

# ─────────────────────────────────────────────────────────────────────────────
# Setup: pick a real employee and a test file_id
# ─────────────────────────────────────────────────────────────────────────────
section("SETUP")

# Pick a real permanent employee
emp = db.employee.find_one({"status_1": "Permanent"}, {"employee_code": 1, "employee_name": 1})
check("Real employee found in DB", emp is not None, str(emp))
EMPLOYEE_CODE = emp["employee_code"] if emp else "0622"
EMPLOYEE_NAME = emp.get("employee_name", "Test Employee") if emp else "Test Employee"
print(f"  Using employee: {EMPLOYEE_CODE} ({EMPLOYEE_NAME})")

# Use a unique test file_id so we don't pollute real data
TEST_FILE_ID = f"TEST-LIFECYCLE-{int(time.time())}"
ASSIGNED_BY = "1030"
print(f"  Test file_id: {TEST_FILE_ID}")

# Clean up any leftover test data from previous runs
db.tasks.delete_many({"file_id": {"$regex": "^TEST-LIFECYCLE-"}})
db.permit_files.delete_many({"file_id": {"$regex": "^TEST-LIFECYCLE-"}})
db.file_tracking.delete_many({"file_id": {"$regex": "^TEST-LIFECYCLE-"}})
db.profile_building.delete_many({"file_id": {"$regex": "^TEST-LIFECYCLE-"}})
print(f"  Cleaned up old test data")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Address → ZIP → Team Lead
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 1: Address → ZIP → Team Lead Resolution")

ADDRESS = "182 Manchester Cir, Pittsburgh, PA 15237, USA"
r = api("post", "/tasks/recommend", timeout=60, json={
    "task_description": "ARORA prelims layout work for residential permit",
    "address": ADDRESS,
    "top_k": 4,
    "creatorparentid": ASSIGNED_BY
})
check("Recommend endpoint responds 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    data = r.json()
    recs = data.get("recommendations", [])
    qi = data.get("query_info", {})
    check("ZIP or team lead resolved from address", bool(qi.get("resolved_zip") or qi.get("team_lead_code") or qi.get("team_lead_name")), str(qi)[:200])
    check("Team lead resolved", bool(qi.get("team_lead_code") or qi.get("team_lead_name")), f"team_lead_name={qi.get('team_lead_name')} team_lead_code={qi.get('team_lead_code')}")
    check("At least 1 recommendation returned", len(recs) >= 1, f"count={len(recs)}")
    if recs:
        print(f"  Top recommendation: {recs[0].get('employee_name')} ({recs[0].get('employee_code')}) score={recs[0].get('similarity_score'):.3f}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Create PRELIMS task with file_id
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 2: Create PRELIMS Task")

r = api("post", "/tasks/create", timeout=60, json={
    "title": "ARORA prelims layout",
    "description": "ARORA prelims layout work for residential permit",
    "id": TEST_FILE_ID,
    "creatorparentid": ASSIGNED_BY,
    "skills_required": ["ARORA", "layout"]
})
check("Task create responds 200", r.status_code == 200, f"status={r.status_code} body={r.text[:200]}")
PRELIMS_TASK_ID = None
if r.status_code == 200:
    data = r.json()
    PRELIMS_TASK_ID = data.get("task_id")
    check("Task ID returned", bool(PRELIMS_TASK_ID), PRELIMS_TASK_ID)
    check("Stage detected as PRELIMS", data.get("detected_stage") == "PRELIMS", f"stage={data.get('detected_stage')}")
    check("Tracking mode FILE_BASED", data.get("tracking_mode") == "FILE_BASED", f"mode={data.get('tracking_mode')}")
    print(f"  Created task: {PRELIMS_TASK_ID}")

# Verify file_tracking initialized
ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
check("file_tracking initialized after task create", ft is not None, f"stage={ft.get('current_stage') if ft else 'MISSING'}")

# Verify permit_files record created
pf = db.permit_files.find_one({"file_id": TEST_FILE_ID})
check("permit_files record created", pf is not None, f"name={pf.get('file_name') if pf else 'MISSING'}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: PRELIMS duplicate guard
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 3: PRELIMS Duplicate Guard")

r2 = api("post", "/tasks/create", timeout=60, json={
    "title": "ARORA prelims duplicate attempt",
    "description": "ARORA prelims layout work again",
    "id": TEST_FILE_ID,
    "creatorparentid": ASSIGNED_BY,
})
check("Second PRELIMS task blocked with 400", r2.status_code == 400,
      f"status={r2.status_code} detail={r2.text[:200]}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Assign PRELIMS task → stage tracking registers employee
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 4: Assign PRELIMS Task to Employee")

if PRELIMS_TASK_ID:
    r = api("post", f"/tasks/{PRELIMS_TASK_ID}/assign", json={
        "employee_code": EMPLOYEE_CODE,
        "assigned_by": ASSIGNED_BY
    })
    check("Assign responds 200", r.status_code == 200, f"status={r.status_code} body={r.text[:200]}")
    if r.status_code == 200:
        data = r.json()
        check("No duplicate warning on first assign", data.get("duplicate_warning") is None,
              f"warning={data.get('duplicate_warning')}")

    # Verify task updated in DB
    task_doc = db.tasks.find_one({"task_id": PRELIMS_TASK_ID})
    check("Task assigned_to set", task_doc and task_doc.get("assigned_to") == EMPLOYEE_CODE,
          f"assigned_to={task_doc.get('assigned_to') if task_doc else 'MISSING'}")
    check("Task status=ASSIGNED", task_doc and task_doc.get("status") == "ASSIGNED",
          f"status={task_doc.get('status') if task_doc else 'MISSING'}")

    # Verify profile_building entry
    pb = db.profile_building.find_one({"task_id": PRELIMS_TASK_ID, "employee_code": EMPLOYEE_CODE})
    check("profile_building entry created", pb is not None, str(pb.get("title") if pb else "MISSING"))

    # Verify stage tracking has employee registered
    ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
    ca = (ft or {}).get("current_assignment") or {}
    check("Stage tracking has current_assignment", bool(ca), f"assignment={ca}")
    check("Stage tracking employee matches", ca.get("employee_code") == EMPLOYEE_CODE,
          f"got={ca.get('employee_code')}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Employee profile shows task in Recent Activity
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 5: Employee Profile — Recent Activity")

r = api("get", f"/employee-tasks/{EMPLOYEE_CODE}")
check("Employee tasks endpoint responds 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    data = r.json()
    assigned = data.get("assigned_tasks", [])
    task_ids = [t.get("task_id") for t in assigned]
    check("PRELIMS task visible in assigned_tasks", PRELIMS_TASK_ID in task_ids,
          f"found={PRELIMS_TASK_ID in task_ids} total={len(assigned)}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Permit Files page shows file with correct stage
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 6: Permit Files Page — File Visible")

r = api("get", "/permit-files/")
check("Permit files endpoint responds 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    files = r.json()
    file_ids = [f.get("file_id") for f in files]
    check("Test file visible on Permit Files page", TEST_FILE_ID in file_ids,
          f"found={TEST_FILE_ID in file_ids} total={len(files)}")
    test_file = next((f for f in files if f.get("file_id") == TEST_FILE_ID), None)
    if test_file:
        check("File has correct status (IN_PRELIMS or PRELIMS)", 
              test_file.get("status") in ("IN_PRELIMS", "PRELIMS"),
              f"status={test_file.get('status')}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: Task Board shows assigned task
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 7: Task Board — Assigned Task Visible")

r = api("get", "/tasks/assigned")
check("Tasks assigned endpoint responds 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    data = r.json()
    tasks = data if isinstance(data, list) else data.get("tasks", [])
    task_ids = [t.get("task_id") for t in tasks]
    check("PRELIMS task visible on Task Board", PRELIMS_TASK_ID in task_ids,
          f"found={PRELIMS_TASK_ID in task_ids} total={len(tasks)}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: Complete PRELIMS task → auto-progress to PRODUCTION
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 8: Complete PRELIMS Task → Auto-Progress to PRODUCTION")

time.sleep(1)  # ensure time difference is measurable

if PRELIMS_TASK_ID:
    r = api("post", f"/employee-tasks/{EMPLOYEE_CODE}/complete", json={
        "task_id": PRELIMS_TASK_ID,
        "completion_notes": "PRELIMS completed by lifecycle test"
    })
    check("Complete task responds 200", r.status_code == 200, f"status={r.status_code} body={r.text[:300]}")
    if r.status_code == 200:
        data = r.json()
        check("Task status=COMPLETED in response", data.get("status") == "COMPLETED",
              f"status={data.get('status')}")
        sp = data.get("stage_progression", {})
        check("Stage progression triggered", bool(sp), f"progression={sp}")
        check("Previous stage=PRELIMS", sp.get("previous_stage") in ("PRELIMS", "FileStage.PRELIMS"),
              f"prev={sp.get('previous_stage')}")
        check("Next stage=PRODUCTION", sp.get("next_stage") in ("PRODUCTION", "FileStage.PRODUCTION"),
              f"next={sp.get('next_stage')}")

    # Verify DB state
    task_doc = db.tasks.find_one({"task_id": PRELIMS_TASK_ID})
    check("Task marked COMPLETED in DB", task_doc and task_doc.get("status") == "COMPLETED",
          f"status={task_doc.get('status') if task_doc else 'MISSING'}")
    check("completed_at set", task_doc and task_doc.get("completed_at") is not None,
          f"completed_at={task_doc.get('completed_at') if task_doc else 'MISSING'}")

    # Verify file moved to PRODUCTION
    ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
    check("file_tracking moved to PRODUCTION", ft and ft.get("current_stage") == "PRODUCTION",
          f"stage={ft.get('current_stage') if ft else 'MISSING'}")

    # Verify permit_files updated
    pf = db.permit_files.find_one({"file_id": TEST_FILE_ID})
    check("permit_files status=IN_PRODUCTION", pf and pf.get("status") == "IN_PRODUCTION",
          f"status={pf.get('status') if pf else 'MISSING'}")

    # Verify PRELIMS time tracked
    ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
    history = ft.get("stage_history", []) if ft else []
    prelims_hist = next((h for h in history if h.get("stage") in ("PRELIMS", "FileStage.PRELIMS")), None)
    check("PRELIMS stage history recorded", prelims_hist is not None,
          f"history_stages={[h.get('stage') for h in history]}")
    if prelims_hist:
        check("PRELIMS has employee recorded", 
              bool((prelims_hist.get("assigned_to") or {}).get("employee_code") or prelims_hist.get("employee_code")),
              f"assigned_to={prelims_hist.get('assigned_to')}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 9: Create PRODUCTION task → assign → complete → auto COMPLETED
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 9: PRODUCTION Task → Assign → Complete → COMPLETED")

r = api("post", "/tasks/create", timeout=60, json={
    "title": "Structural and electrical permit design",
    "description": "Structural design and electrical drawing for production stage",
    "id": TEST_FILE_ID,
    "creatorparentid": ASSIGNED_BY,
    "skills_required": ["structural design", "electrical drawing"]
})
check("PRODUCTION task create responds 200", r.status_code == 200,
      f"status={r.status_code} body={r.text[:200]}")
PROD_TASK_ID = None
if r.status_code == 200:
    data = r.json()
    PROD_TASK_ID = data.get("task_id")
    check("Stage detected as PRODUCTION", data.get("detected_stage") == "PRODUCTION",
          f"stage={data.get('detected_stage')}")
    print(f"  Created PRODUCTION task: {PROD_TASK_ID}")

if PROD_TASK_ID:
    # Assign
    r = api("post", f"/tasks/{PROD_TASK_ID}/assign", json={
        "employee_code": EMPLOYEE_CODE,
        "assigned_by": ASSIGNED_BY
    })
    check("PRODUCTION task assign responds 200", r.status_code == 200,
          f"status={r.status_code} body={r.text[:200]}")

    time.sleep(1)

    # Complete
    r = api("post", f"/employee-tasks/{EMPLOYEE_CODE}/complete", json={
        "task_id": PROD_TASK_ID,
        "completion_notes": "PRODUCTION completed by lifecycle test"
    })
    check("PRODUCTION task complete responds 200", r.status_code == 200,
          f"status={r.status_code} body={r.text[:300]}")
    if r.status_code == 200:
        sp = r.json().get("stage_progression", {})
        check("PRODUCTION→COMPLETED progression", sp.get("next_stage") in ("COMPLETED", "FileStage.COMPLETED"),
              f"next={sp.get('next_stage')}")

    # Verify file in COMPLETED
    ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
    check("file_tracking moved to COMPLETED", ft and ft.get("current_stage") == "COMPLETED",
          f"stage={ft.get('current_stage') if ft else 'MISSING'}")
    pf = db.permit_files.find_one({"file_id": TEST_FILE_ID})
    check("permit_files status=COMPLETED", pf and pf.get("status") == "COMPLETED",
          f"status={pf.get('status') if pf else 'MISSING'}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 9b: Manager moves file from COMPLETED → QC (required before QC task)
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 9b: Manager moves file COMPLETED → QC")

r = api("post", f"/stage-tracking/move-to-qc/{TEST_FILE_ID}", timeout=30, params={"employee_code": ASSIGNED_BY})
check("move-to-qc responds 200", r.status_code == 200,
      f"status={r.status_code} body={r.text[:200]}")

ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
check("file_tracking moved to QC", ft and ft.get("current_stage") == "QC",
      f"stage={ft.get('current_stage') if ft else 'MISSING'}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 10: Create QC task → assign → complete → auto DELIVERED
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 10: QC Task → Assign → Complete → DELIVERED")

r = api("post", "/tasks/create", timeout=60, json={
    "title": "Quality control review",
    "description": "QC quality control review and compliance check",
    "id": TEST_FILE_ID,
    "creatorparentid": ASSIGNED_BY,
    "skills_required": ["QC", "quality control"]
})
check("QC task create responds 200", r.status_code == 200,
      f"status={r.status_code} body={r.text[:200]}")
QC_TASK_ID = None
if r.status_code == 200:
    data = r.json()
    QC_TASK_ID = data.get("task_id")
    check("Stage detected as QC", data.get("detected_stage") == "QC",
          f"stage={data.get('detected_stage')}")
    print(f"  Created QC task: {QC_TASK_ID}")

if QC_TASK_ID:
    r = api("post", f"/tasks/{QC_TASK_ID}/assign", json={
        "employee_code": EMPLOYEE_CODE,
        "assigned_by": ASSIGNED_BY
    })
    check("QC task assign responds 200", r.status_code == 200,
          f"status={r.status_code} body={r.text[:200]}")

    time.sleep(1)

    r = api("post", f"/employee-tasks/{EMPLOYEE_CODE}/complete", json={
        "task_id": QC_TASK_ID,
        "completion_notes": "QC completed by lifecycle test"
    })
    check("QC task complete responds 200", r.status_code == 200,
          f"status={r.status_code} body={r.text[:300]}")
    if r.status_code == 200:
        sp = r.json().get("stage_progression", {})
        check("QC→DELIVERED progression", sp.get("next_stage") in ("DELIVERED", "FileStage.DELIVERED"),
              f"next={sp.get('next_stage')}")

    ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
    check("file_tracking moved to DELIVERED", ft and ft.get("current_stage") == "DELIVERED",
          f"stage={ft.get('current_stage') if ft else 'MISSING'}")
    pf = db.permit_files.find_one({"file_id": TEST_FILE_ID})
    check("permit_files status=DELIVERED", pf and pf.get("status") == "DELIVERED",
          f"status={pf.get('status') if pf else 'MISSING'}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 11: Time tracking per stage
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 11: Time Tracking Per Stage")

ft = db.file_tracking.find_one({"file_id": TEST_FILE_ID})
if ft:
    history = ft.get("stage_history", [])
    print(f"  Total stage history entries: {len(history)}")
    for h in history:
        stage = h.get("stage", "?")
        status = h.get("status", "?")
        entered = h.get("entered_stage_at", "?")
        completed = h.get("completed_stage_at", "?")
        duration = h.get("total_duration_minutes", "?")
        emp = (h.get("assigned_to") or {}).get("employee_code", "?")
        print(f"    Stage={stage} status={status} employee={emp} duration={duration}min")
    
    stages_recorded = [h.get("stage") for h in history]
    check("PRELIMS stage in history", "PRELIMS" in stages_recorded, str(stages_recorded))
    check("PRODUCTION stage in history", "PRODUCTION" in stages_recorded, str(stages_recorded))
    check("COMPLETED stage in history", "COMPLETED" in stages_recorded, str(stages_recorded))
    check("QC stage in history", "QC" in stages_recorded, str(stages_recorded))
    check("DELIVERED stage in history", "DELIVERED" in stages_recorded, str(stages_recorded))
    
    # Check total duration is tracked
    has_duration = ft.get("total_duration_minutes") is not None or any(
        h.get("total_duration_minutes") is not None for h in history
    )
    check("Duration tracked (file or stage level)", has_duration,
          f"total_duration={ft.get('total_duration_minutes')}min history_durations={[h.get('total_duration_minutes') for h in history]}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 12: Stage Tracking Dashboard shows file
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 12: Stage Tracking Dashboard")

r = api("get", "/stage-tracking/dashboard")
check("Stage tracking dashboard responds 200", r.status_code == 200, f"status={r.status_code}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 13: Team Lead Board stats
# ─────────────────────────────────────────────────────────────────────────────
section("STEP 13: Team Lead Board Stats")

r = api("get", "/tasks/team-lead-stats", headers={"Authorization": f"Bearer {ASSIGNED_BY}"})
check("Team lead stats responds 200", r.status_code == 200, f"status={r.status_code}")
if r.status_code == 200:
    data = r.json()
    leads = data.get("team_lead_stats") or data.get("team_stats") or []
    check("Team lead stats returned", len(leads) > 0, f"count={len(leads)}")

# ─────────────────────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
section("FINAL SUMMARY")

passed = sum(1 for s, _, _ in results if s == PASS)
failed = sum(1 for s, _, _ in results if s == FAIL)
total = len(results)

print(f"\n  Total: {total}  |  {PASS} Passed: {passed}  |  {FAIL} Failed: {failed}")

if failed > 0:
    print(f"\n  Failed checks:")
    for s, label, detail in results:
        if s == FAIL:
            print(f"    {FAIL} {label}" + (f" | {detail}" if detail else ""))

# Cleanup test data
db.tasks.delete_many({"file_id": TEST_FILE_ID})
db.permit_files.delete_many({"file_id": TEST_FILE_ID})
db.file_tracking.delete_many({"file_id": TEST_FILE_ID})
db.profile_building.delete_many({"file_id": TEST_FILE_ID})
print(f"\n  Test data cleaned up.")

sys.exit(0 if failed == 0 else 1)
