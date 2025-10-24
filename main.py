import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from statistics import mean
from collections import Counter

base = Path("C:/Users/sambh/OneDrive/Desktop/Python code/app_pipeline") 
path = base / "data" / "app_usage_raw.csv"


def parse_date(date_str):
    """Try multiple formats; return a standardized YYYY-MM-DD string."""
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None  # invalid date


#reading data and cleaning
def clean_csv(input_file):
    cleaned = []
    with input_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Remove extra spaces
            user_id = row['user_id'].strip()
            user_name = row['user_name'].strip()
            country = row['country'].strip().title()   # “france” → “France”
            session = row['session_time'].strip()
            device = row['device'].strip().capitalize()
            last_login = row['last_login'].strip()
            version = row['app_version'].strip()

            # 1️⃣ Skip if essential fields missing
            if not user_id or not session or float(session) <= 0:
                continue
            if not country:
                continue

            # 2️⃣ Parse date safely
            fixed_date = parse_date(last_login)
            if not fixed_date:
                continue  # skip invalid date formats

            # 3️⃣ Convert session to number
            try:
                session = float(session)
            except ValueError:
                continue

            # 4️⃣ Build cleaned record
            cleaned.append({
                "user_id": int(user_id),
                "user_name": user_name,
                "country": country,
                "session_time": session,
                "device": device,
                "last_login": fixed_date,
                "app_version": version
            })
    return cleaned


cleaned_rows = clean_csv(path)

#Writting data
output = Path("C:/Users/sambh/OneDrive/Desktop/Python code/app_pipeline/output/app_usage_clean.csv")
with output.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=cleaned_rows[0].keys())
    writer.writeheader()
    writer.writerows(cleaned_rows)

avg_sessions = defaultdict(list)
for row in cleaned_rows:
    avg_sessions[row['country']].append(row['session_time'])

devices = Counter(row['device'] for row in cleaned_rows)

report = Path(r"C:\Users\sambh\OneDrive\Desktop\Python code\app_pipeline\output\summary.txt")
with report.open("w", encoding="utf-8") as f:
    f.write("=== APP USAGE SUMMARY REPORT ===\n\n")
    f.write("Average Session Time per Country:\n")
    for country, times in avg_sessions.items():
        avg_time = round(mean(times), 2)
        f.write(f"  {country}: {avg_time} mins\n")
    f.write("\nDevice Usage Counts:\n")
    for dev, cnt in devices.items():
        f.write(f"  {dev}: {cnt}\n")

print()
print("Gello")


