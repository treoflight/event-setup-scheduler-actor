from apify import Actor
import pandas as pd
from datetime import datetime
import random
import traceback

# --- SHIFT RULES ---
SHIFT_RULES = {
    "Midday": {"min": 3, "max": 5},
    "Night": {"min": 8, "max": 10},
}

def parse_time(t):
    if pd.isna(t): return None
    t = str(t).strip()
    if not t: return None
    for fmt in ["%I:%M:%S %p", "%I:%M %p", "%I %p", "%H:%M:%S", "%H:%M", "%H:%M:%S.%f"]:
        try: return datetime.strptime(t, fmt).time()
        except Exception: pass
    try: return datetime.fromisoformat(t).time()
    except Exception: return None

async def main():
    async with Actor:
        try:
            print("🚀 Starting scheduling process...")
            input_data = await Actor.get_input() or {}
            shifts_url = input_data.get("setup_shifts_tsv")
            avail_url = input_data.get("employee_availability_tsv")

            if not shifts_url or not avail_url:
                print("❌ Missing input URLs.")
                await Actor.fail("Input URLs missing. Provide both TSV URLs.")
                return

            print(f"📥 Loading setup shifts from: {shifts_url}")
            print(f"📥 Loading employee availability from: {avail_url}")

            shifts = pd.read_csv(shifts_url, sep="\t")
            avail = pd.read_csv(avail_url, sep="\t")

            print("✅ Files loaded successfully.")

            shifts.columns = [c.strip() for c in shifts.columns]
            avail.columns = [c.strip() for c in avail.columns]

            shifts["Shift Start Parsed"] = shifts["Shift Start Time"].apply(parse_time)
            shifts["Shift End Parsed"] = shifts["Shift End Time"].apply(parse_time)
            shifts["Hours"] = pd.to_numeric(shifts["Hours"], errors="coerce").fillna(0.0)
            shifts["Date Parsed"] = pd.to_datetime(shifts["Date"], errors="coerce")
            shifts = shifts.sort_values(["Date Parsed", "Shift Start Parsed"]).reset_index(drop=True)

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
                    if "midday" in val: emp_av[wd] = "Midday"
                    elif "night" in val: emp_av[wd] = "Night"
                    elif "both" in val: emp_av[wd] = "Both"
                    else: emp_av[wd] = ""
                employees[name] = {"availability": emp_av, "assigned_hours": 0.0, "assignments": []}

            print(f"👷 Found {len(employees)} employees.")

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

                candidates = [(n, info["assigned_hours"])
                              for n, info in employees.items()
                              if info["availability"].get(weekday, "") in ("Both", shift_type)]
                candidates.sort(key=lambda x: x[1])
                random.shuffle(candidates)

                if len(candidates) < min_needed:
                    print(f"⚠️ Not enough available employees for {shift_type} on {date}. Filling with others.")
                    chosen = [c[0] for c in candidates]
                    extras_needed = min_needed - len(chosen)
                    other = [(n, info["assigned_hours"]) for n, info in employees.items() if n not in chosen]
                    other.sort(key=lambda x: x[1])
                    chosen.extend([c[0] for c in other[:extras_needed]])
                else:
                    chosen = [c[0] for c in candidates[:max_needed]]

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

            final_df = pd.DataFrame(assignments)
            print(f"✅ Generated {len(assignments)} total shift assignments.")

            # Save TSV to key-value store
            await Actor.set_value("final_schedule.tsv", final_df.to_csv(sep="\t", index=False))
            print("💾 Saved final_schedule.tsv to Key-Value Store.")

            # Also push structured data
            await Actor.push_data(assignments)
            print("📤 Pushed assignments to default dataset.")

            print("🎉 Scheduling completed successfully!")

        except Exception as e:
            print("❌ ERROR OCCURRED:")
            print(traceback.format_exc())
            await Actor.fail(f"Error during scheduling: {str(e)}")


