import pandas as pd
from datetime import datetime
import random
from apify import Actor
import aiohttp
import asyncio

async def main():
    # === Initialize actor ===
    await Actor.init()

    # === Get input JSON from Make.com / Apify ===
    input_data = await Actor.get_input()
    setup_shifts_url = input_data.get("setup_shifts_tsv")
    employee_availability_url = input_data.get("employee_availability_tsv")

    # === Shift rules ===
    SHIFT_RULES = {
        "Midday": {"min": 3, "max": 5},
        "Night": {"min": 8, "max": 10},
    }

    # === Helper to parse times ===
    def parse_time(t):
        if pd.isna(t):
            return None
        t = str(t).strip()
        if not t:
            return None
        for fmt in ["%I:%M:%S %p", "%I:%M %p", "%I %p", "%H:%M:%S", "%H:%M", "%H:%M:%S.%f"]:
            try:
                return datetime.strptime(t, fmt).time()
            except Exception:
                pass
        try:
            return datetime.fromisoformat(t).time()
        except Exception:
            return None

    # === Fetch TSVs from URLs ===
    async def fetch_tsv(url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                text = await resp.text()
                return pd.read_csv(pd.compat.StringIO(text), sep="\t")

    shifts = await fetch_tsv(setup_shifts_url)
    avail = await fetch_tsv(employee_availability_url)

    # === Clean column names ===
    shifts.columns = [c.strip() for c in shifts.columns]
    avail.columns = [c.strip() for c in avail.columns]

    shifts["Shift Start Parsed"] = shifts["Shift Start Time"].apply(parse_time)
    shifts["Shift End Parsed"] = shifts["Shift End Time"].apply(parse_time)
    shifts["Hours"] = pd.to_numeric(shifts["Hours"], errors="coerce").fillna(0.0)
    shifts["Date Parsed"] = pd.to_datetime(shifts["Date"], errors="coerce")
    shifts = shifts.sort_values(["Date Parsed", "Shift Start Parsed"]).reset_index(drop=True)

    # === Employee availability map ===
    weekday_cols = {
        "Monday": "Monday Availability",
        "Tuesday": "Tuesday Availability",
        "Wednesday": "Wednesday Availability",
        "Thursday": "Thursday Availability",
        "Friday": "Friday Availability",
        "Saturday": "Saturday Availability",
        "Sunday": "Sunday Availability",
    }

    employees = {}
    for _, row in avail.iterrows():
        name = str(row["Name"]).strip()
        emp_av = {}
        for wd, col in weekday_cols.items():
            val = str(row.get(col, "")).strip().lower()
            if "midday" in val:
                emp_av[wd] = "Midday"
            elif "night" in val:
                emp_av[wd] = "Night"
            elif "both" in val:
                emp_av[wd] = "Both"
            else:
                emp_av[wd] = ""
        employees[name] = {"availability": emp_av, "assigned_hours": 0.0, "assignments": []}

    # === Scheduling logic ===
    assignments = []

    for _, shift in shifts.iterrows():
        date = str(shift["Date"]).strip()
        weekday = str(shift["Day of the Week"]).strip()
        shift_type = str(shift["Midday or Night Shift"]).strip().capitalize()
        start = shift["Shift Start Parsed"]
        end = shift["Shift End Parsed"]
        hours = float(shift["Hours"])

        rules = SHIFT_RULES.get(shift_type, {"min": 0, "max": 0})
        min_needed, max_needed = rules["min"], rules["max"]

        # Find available employees
        candidates = []
        for name, info in employees.items():
            av = info["availability"].get(weekday, "")
            if av in ("Both", shift_type):
                candidates.append((name, info["assigned_hours"]))

        # Sort by hours and shuffle ties
        candidates.sort(key=lambda x: x[1])
        random.shuffle(candidates)

        # Ensure minimum staffing
        if len(candidates) < min_needed:
            chosen = [c[0] for c in candidates]
            extras_needed = min_needed - len(chosen)
            other_emps = [(n, info["assigned_hours"]) for n, info in employees.items() if n not in chosen]
            other_emps.sort(key=lambda x: x[1])
            chosen.extend([c[0] for c in other_emps[:extras_needed]])
        else:
            chosen = [c[0] for c in candidates[:max_needed]]

        # Assign employees
        for emp_name in chosen:
            employees[emp_name]["assigned_hours"] += hours
            employees[emp_name]["assignments"].append((date, start, end))
            assignments.append({
                "Date": date,
                "Day of the Week": weekday,
                "Midday or Night Shift": shift_type,
                "Shift Start Time": shift["Shift Start Time"],
                "Shift End Time": shift["Shift End Time"],
                "Employee Name": emp_name,
                "Hours": hours,
            })

    # === Convert to DataFrame and TSV string ===
    final_df = pd.DataFrame(assignments)
    tsv_content = final_df.to_csv(sep="\t", index=False)

    # === Save to Key-Value Store (optional) ===
    await Actor.set_value("final_schedule.tsv", tsv_content)

    # === Print TSV content so Make.com can capture it ===
    print(tsv_content)

    # === Exit actor ===
    await Actor.exit()

# Run the async main
asyncio.run(main())







