#!/usr/bin/env python3
"""
Pigment → Headcount Dashboard Refresh Script
Fetches live data from Pigment API and rewrites index.html data section.
Run locally or via GitHub Actions (uses PIGMENT_EXPORT_TOKEN env var).
"""

import os, sys, urllib.request, urllib.error, json, csv, io, re
from datetime import datetime, timezone
from collections import defaultdict

# ── Tokens ──────────────────────────────────────────────────────────────────
EXPORT_TOKEN = os.environ.get(
    "PIGMENT_EXPORT_TOKEN",
    "pgmt_lKKEfrO2aangjJIDrCYYypTnNnfgtd2t1q5E4l9k1P59AB"
)
BASE = "https://pigment.app/api"

# ── Block IDs ────────────────────────────────────────────────────────────────
BLOCK_HC_TOTAL    = "370a3e3e-af73-4a7c-be87-83249b7abd9c"   # TT_Stats_Headcount
BLOCK_ANS         = "2f3e3431-63d7-4608-b27d-2ddb4b4180f4"   # EE_Stats_ANS
BLOCK_TBH_PLAN    = "108e73c2-b9c4-4323-9118-ce4a3ad64899"   # [Tbl] TBH Planning
BLOCK_EE_HC       = "42829695-83cf-484e-b474-b0444ba22859"   # EE_Stats_Headcount (employee-level)
BLOCK_EE_DIV      = "1ebf2164-5cd9-4c7b-970c-a70295544fb4"   # EE_Data_Division
BLOCK_ROSTER      = "5ac27c97-9f0b-42e2-80eb-7eb48e8d23de"   # [Tbl] Employee Roster Details

# ── Helpers ──────────────────────────────────────────────────────────────────
def normalize_division(div):
    """Collapse sub-branded division names to a single label (e.g. HATCo Operations → HATCo)."""
    if div.lower().startswith("hatco"):
        return "HATCo"
    return div

def export_csv(block_id, block_type="metric"):
    """Fetch a Pigment block as CSV rows (list of dicts)."""
    url = f"{BASE}/v1/export/{block_type}/{block_id}"
    method = "GET" if block_type == "view" else "POST"
    data = b"{}" if method == "POST" else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {EXPORT_TOKEN}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "*/*")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8-sig")
    except urllib.error.HTTPError as e:
        print(f"  ❌ HTTP {e.code} fetching {block_type}/{block_id}: {e.read().decode()[:200]}")
        sys.exit(1)
    return list(csv.DictReader(io.StringIO(raw), delimiter=";"))


def fmt_name(pigment_name):
    """Convert 'Last, First' → 'First Last'."""
    if not pigment_name:
        return ""
    if "," in pigment_name:
        last, first = pigment_name.split(",", 1)
        return f"{first.strip()} {last.strip()}"
    return pigment_name.strip()


def cur_month_key():
    """Return Pigment month key for today, e.g. 'Apr 26'."""
    return datetime.now().strftime("%b %y")


def js_str(v):
    return "'" + str(v).replace("'", "\\'") + "'"


def js_obj(**kwargs):
    parts = []
    for k, v in kwargs.items():
        if isinstance(v, bool):
            parts.append(f"{k}:{'true' if v else 'false'}")
        elif isinstance(v, int):
            parts.append(f"{k}:{v}")
        else:
            parts.append(f"{k}:{js_str(v)}")
    return "{ " + ", ".join(parts) + " }"


# ── Fetch data ────────────────────────────────────────────────────────────────
print("Fetching Pigment data …")
cur_mon = cur_month_key()
print(f"  Current month key: {cur_mon}")

print("  → TT_Stats_Headcount")
hc_rows = export_csv(BLOCK_HC_TOTAL, "metric")

print("  → EE_Stats_ANS")
ans_rows = export_csv(BLOCK_ANS, "metric")

print("  → [Tbl] TBH Planning")
tbh_rows = export_csv(BLOCK_TBH_PLAN, "table")

print("  → EE_Stats_Headcount (for dept→division mapping)")
ee_hc_rows = export_csv(BLOCK_EE_HC, "metric")

print("  → EE_Data_Division (for dept→division mapping)")
ee_div_rows = export_csv(BLOCK_EE_DIV, "metric")

print("  → [Tbl] Employee Roster Details (for In-Seat count)")
roster_rows = export_csv(BLOCK_ROSTER, "table")

print(f"  ✓ Fetched {len(hc_rows)} HC rows, {len(ans_rows)} ANS rows, "
      f"{len(tbh_rows)} TBH rows, {len(roster_rows)} roster rows")

# ── Build dept→division mapping from EE data (most complete source) ──────────
emp_div_map = {}   # pigment_name → division
for r in ee_div_rows:
    if r.get("_month_GDBAK8") != cur_mon:
        continue
    name = (r.get("employee_S1XCS2") or "").strip()
    div  = (r.get("ee_data_division_9K9VIT") or "").strip()
    if name and div and not div.startswith("z_"):
        emp_div_map[name] = normalize_division(div)

# dept → division: majority-vote from EE employees in that dept
dept_div_votes = defaultdict(lambda: defaultdict(int))
for r in ee_hc_rows:
    if r.get("_month_GDBAK8") != cur_mon:
        continue
    name = (r.get("employee_S1XCS2") or "").strip()
    dept = (r.get("departments_ITJAES") or "").strip()
    div  = emp_div_map.get(name, "")
    if dept and div:
        dept_div_votes[dept][div] += 1

dept_div = {}
for dept, votes in dept_div_votes.items():
    dept_div[dept] = max(votes, key=votes.get)

# Also add mappings from TBH Planning (covers depts with no current employees)
# (done later inside the TBH loop)

# ── Build teamStats ───────────────────────────────────────────────────────────
dept_hc   = defaultdict(lambda: defaultdict(float))  # dept → status → count (kept for segment only)
dept_seg  = {}                                        # dept → segment

# Build In-Seat from Employee Roster Details — matches Pigment board's exact filter:
#   active=TRUE, non-z_ dept, exclude unplanned hires who started THIS month
#   (those have hire_date_fcst_filter=TRUE & in_plan="Not in plan" & hire_month=cur_mon,
#    e.g. Kinney, Genevieve hired 2026-04-01 — not yet in the headcount plan)
dept_inseat = defaultdict(int)
for r in roster_rows:
    active  = (r.get("ee_card_active__current_month_I86B2I") or "").strip()
    status  = (r.get("ee_card_hc_status_current_month_HS12V0") or "").strip()
    dept    = (r.get("ee_card_department_current_month_NH1GGR") or "").strip()
    inplan  = (r.get("ee_text_in_plan_current_month_SMAXK2") or "").strip()
    hire_mo = (r.get("ee_card_hire_month_current_month_JM58I2") or "").strip()

    if status != "In-Seat" or active != "TRUE":
        continue
    if dept.startswith("z_"):          # unassigned dept (e.g. z_No Department)
        continue
    # Exclude unplanned hires who started in the current month — not yet in the plan
    if inplan == "Not in plan" and hire_mo == cur_mon:
        continue
    dept_inseat[dept] += 1

# Build name → actual title from Roster (used for ANS employee records)
roster_title_map = {}
for r in roster_rows:
    name  = (r.get("employee_S1XCS2") or "").strip()
    title = (r.get("ee_card_title_current_month_AL0IHH") or "").strip()
    if name and title:
        roster_title_map[name] = title

# TT_Stats_Headcount kept only for segment metadata
for r in hc_rows:
    if r.get("_month_GDBAK8") != cur_mon:
        continue
    dept   = r.get("departments_ITJAES", "").strip()
    status = r.get("hc_status_GVAXIK", "").strip()
    val    = float(r.get("tt_stats_headcount_A6LQWZ") or 0)
    if dept and val:
        dept_hc[dept][status] += val  # used only for dept_seg lookup below

# In-Market + TBH + ANS counts from TBH Planning (single source of truth)
dept_im  = defaultdict(int)
dept_tbh = defaultdict(int)
dept_ans = defaultdict(int)   # ANS counts from TBH Planning (replaces TT_Stats_Headcount ANS)

in_market_roles = []
tbh_roles       = []
ans_employees_raw = []        # built directly from TBH Planning ANS rows
seen_ans_names  = set()

for r in tbh_rows:
    dept      = (r.get("tbh_department_UOEVEH") or "").strip()
    div       = normalize_division((r.get("tbh_division_HMK6KN")   or "").strip())
    seg       = (r.get("tbh_segment_C56MGS")     or "Functional").strip()
    hc_type   = (r.get("tbh_hc_reporting_type_3JWELK") or "").strip()
    pos_status= (r.get("tbh_position_status_0ZF730")   or "").strip()
    active    = (r.get("tbh__active__YMOLOB")          or "").strip().upper()
    emp_name  = (r.get("tbh_position_employee_H8NO5P") or "").strip()
    emp_hc    = (r.get("tbh_position_employee_hc_status_BNTCXA") or "").strip()
    hr_status = (r.get("tbh_hr_status_PWOWYR")         or "").strip()
    tbhg_status=(r.get("tbhg_status_3NJS4Z")           or "").strip()

    if dept and seg:
        dept_seg[dept] = seg

    # Fill in division from TBH Planning if not already known from EE data
    tbh_div = normalize_division((r.get("tbh_division_HMK6KN") or "").strip())
    if dept and tbh_div and dept not in dept_div:
        dept_div[dept] = tbh_div

    # ── ANS: count from TBH Planning (source of truth) ───────────────────────
    # Pigment counts ANS from TBH positions where emp_hc = "ANS", regardless of
    # whether those employees have been entered into the HR system yet.
    if emp_hc == "ANS" and emp_name and dept:
        if emp_name not in seen_ans_names:
            seen_ans_names.add(emp_name)
            dept_ans[dept] += 1
            city = (r.get("tbh_city_OH6H5T") or r.get("tbh_text_city_XLRCXO") or "").strip()
            if city.lower() in ("bangalore", "bengaluru"):
                city = "Bengaluru"
            ans_employees_raw.append(dict(
                name        = fmt_name(emp_name),
                title       = roster_title_map.get(emp_name) or (r.get("tbh2_final_candidate_title_HQUHBH") or "").strip() or (r.get("tbh_position_title_08ZQWH") or "").strip(),
                division    = div or dept,
                team        = dept,
                hireDate    = (r.get("tbh_actual_hire_date_87ML4V") or r.get("tbh_hire_date_F35HTM") or "").strip(),
                type        = (r.get("tbh_approval_status_20UO98") or "In plan").strip(),
                city        = city,
                country     = (r.get("tbh_country_QW1BMZ") or r.get("tbh_text_country_W6HD13") or "").strip(),
                backfillFor = fmt_name(r.get("tbh_text_backfill_for_MW82VF") or ""),
            ))

    # ── Open positions: In Market & TBH ──────────────────────────────────────
    # Skip filled positions
    if pos_status == "Filled":
        continue
    if hc_type not in ("Roles In Market", "TBH"):
        continue

    # For planned TBH: only count valid+in-plan roles (is_valid=TRUE supersedes active flag)
    is_valid = (r.get("tbh_isvalid_and_inplan__NBFM6C") or "").strip()
    if hc_type == "TBH" and is_valid != "TRUE":
        continue
    # For Roles In Market: skip explicitly deactivated rows (active=FALSE) unless valid+inplan
    if hc_type == "Roles In Market" and active == "FALSE" and is_valid != "TRUE":
        continue

    # Skip stale positions: employee is already In-Seat or Terminated (Pigment pos_status not updated)
    # Also skip if HR or TBH group status already shows Filled
    if emp_hc in ("In-Seat", "Terminated"):
        continue
    if hr_status == "Filled" or tbhg_status == "Filled":
        continue

    # Skip deprecated roles — these are cancelled/superseded positions
    deprecated = (r.get("tbh2_deprecated__V2ZOJ0") or "").strip()
    appr_status = (r.get("tbh_approval_status_20UO98") or "").strip()
    if deprecated == "TRUE" or appr_status == "Deprecated":
        continue

    # Normalize city (Bangalore / Bengaluru)
    city = (r.get("tbh_city_OH6H5T") or r.get("tbh_text_city_XLRCXO") or "").strip()
    if city.lower() in ("bangalore", "bengaluru"):
        city = "Bengaluru"

    role = dict(
        id        = r.get("tbh_id_2KZFRN", "").replace("TBH ", ""),
        title     = (r.get("tbh_position_title_08ZQWH") or "").strip(),
        division  = div,
        team      = dept,
        hireDate  = r.get("tbh_hire_date_F35HTM", ""),
        type      = (r.get("tbh_approval_status_20UO98") or "").strip(),
        city      = city,
        country   = (r.get("tbh_country_QW1BMZ") or r.get("tbh_text_country_W6HD13") or "").strip(),
        recruitStatus = (r.get("tbh_recruitment_status_text_0XDP9R") or "").strip(),
        hiringMgr = (r.get("tbhg_manager_KGNAN5") or "").strip(),
        backfillFor = (r.get("tbh_text_backfill_for_MW82VF") or "").strip(),
        notes     = (r.get("tbh_notes_NJ6AKX") or "").strip(),
    )

    if hc_type == "Roles In Market":
        dept_im[dept]  += 1
        in_market_roles.append(role)
    else:
        dept_tbh[dept] += 1
        tbh_roles.append(role)

# Combine all departments
all_depts = (
    set(dept_inseat.keys()) |
    set(dept_im.keys()) |
    set(dept_tbh.keys()) |
    set(dept_ans.keys())
) - {""}

team_stats = []
for dept in sorted(all_depts):
    team_stats.append(dict(
        team     = dept,
        division = dept_div.get(dept, dept_seg.get(dept, "Other")),
        segment  = dept_seg.get(dept, "Functional"),
        inSeat   = dept_inseat.get(dept, 0),   # from Roster (matches Pigment board)
        ans      = dept_ans.get(dept, 0),       # from TBH Planning (source of truth)
        inMarket = dept_im.get(dept, 0),
        tbh      = dept_tbh.get(dept, 0),
    ))

# ── ansEmployees: already built from TBH Planning in the loop above ──────────
# ans_employees_raw contains all unique ANS employees with full TBH enrichment.
# Sort by hire date ascending.
ans_employees = sorted(ans_employees_raw, key=lambda x: x.get("hireDate") or "9999")

# ── Print summary ─────────────────────────────────────────────────────────────
total_in_seat  = sum(t["inSeat"]   for t in team_stats)
total_ans      = sum(t["ans"]      for t in team_stats)
total_in_market= sum(t["inMarket"] for t in team_stats)
total_tbh      = sum(t["tbh"]      for t in team_stats)

print(f"\n📊 Summary for {cur_mon}:")
print(f"   In Seat:     {total_in_seat}")
print(f"   ANS:         {total_ans}")
print(f"   In Market:   {total_in_market}")
print(f"   Planned TBH: {total_tbh}")
print(f"   Teams:       {len(team_stats)}")
print(f"   ANS people:  {len(ans_employees)}")
print(f"   In-Mkt roles:{len(in_market_roles)}")
print(f"   TBH roles:   {len(tbh_roles)}")

# ── Generate JavaScript data block ───────────────────────────────────────────
refresh_dt = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # UTC

lines = []
lines.append(f"const TODAY = new Date('{datetime.now().strftime('%Y-%m-%d')}');")
lines.append(f"const DATA_REFRESHED = '{refresh_dt}';")
lines.append("")

# teamStats
lines.append("const teamStats = [")
for t in team_stats:
    lines.append(
        f"  {{ team:{js_str(t['team'])}, division:{js_str(t['division'])}, segment:{js_str(t['segment'])}, "
        f"inSeat:{t['inSeat']}, ans:{t['ans']}, inMarket:{t['inMarket']}, tbh:{t['tbh']} }},"
    )
lines.append("];")
lines.append("")

# ansEmployees
lines.append("const ansEmployees = [")
for e in ans_employees:
    lines.append(
        f"  {{ name:{js_str(e['name'])}, title:{js_str(e['title'])}, "
        f"division:{js_str(e['division'])}, team:{js_str(e['team'])}, "
        f"hireDate:{js_str(e['hireDate'])}, type:{js_str(e['type'])}, "
        f"city:{js_str(e['city'])}, country:{js_str(e['country'])}, "
        f"backfillFor:{js_str(e['backfillFor'])} }},"
    )
lines.append("];")
lines.append("")

# inMarketRoles
lines.append("const inMarketRoles = [")
for r in sorted(in_market_roles, key=lambda x: x.get("hireDate") or ""):
    lines.append(
        f"  {{ id:{js_str(r['id'])}, title:{js_str(r['title'])}, "
        f"division:{js_str(r['division'])}, dept:{js_str(r['team'])}, "
        f"status:{js_str(r['recruitStatus'])}, planStatus:{js_str(r['type'])}, "
        f"hireDate:{js_str(r['hireDate'])}, hiringMgr:{js_str(r['hiringMgr'])}, "
        f"city:{js_str(r['city'])}, backfillFor:{js_str(r['backfillFor'])} }},"
    )
lines.append("];")
lines.append("")

# tbhRoles
lines.append("const tbhRoles = [")
for r in sorted(tbh_roles, key=lambda x: x.get("hireDate") or ""):
    lines.append(
        f"  {{ id:{js_str(r['id'])}, title:{js_str(r['title'])}, "
        f"division:{js_str(r['division'])}, dept:{js_str(r['team'])}, "
        f"planStatus:{js_str(r['type'])}, hireDate:{js_str(r['hireDate'])}, "
        f"hiringMgr:{js_str(r['hiringMgr'])}, city:{js_str(r['city'])}, "
        f"notes:{js_str(r['notes'])} }},"
    )
lines.append("];")

new_data_block = "\n".join(lines)

# ── Patch index.html ──────────────────────────────────────────────────────────
html_path = os.path.join(os.path.dirname(__file__), "index.html")
print(f"\nPatching {html_path} …")

with open(html_path, "r", encoding="utf-8") as f:
    html = f.read()

# Replace everything between the data fence comments and the STATE comment
pattern = re.compile(
    r"(const TODAY\s*=.*?)(// ═+\s*\n// STATE)",
    re.DOTALL
)

replacement = new_data_block + "\n\n// ═══════════════════════════════════════════\n// STATE"
new_html, count = pattern.subn(replacement, html)

if count == 0:
    print("❌ Could not find data section in index.html — pattern not matched")
    sys.exit(1)

with open(html_path, "w", encoding="utf-8") as f:
    f.write(new_html)

print(f"✅ index.html updated  ({count} replacement(s))")
print(f"   Refresh timestamp: {refresh_dt}")
