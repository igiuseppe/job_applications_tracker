import os
import glob
from csv_to_sheet import append_csv_to_sheet

CSV_DIR = "output/outreach"

def upload_all_outreach_csvs():
    # Find all outreach CSV files
    pattern = os.path.join(CSV_DIR, "outreach_*.csv")
    csv_files = sorted(glob.glob(pattern))
    
    if not csv_files:
        print(f"No outreach CSV files found in {CSV_DIR}")
        return
    
    print(f"Found {len(csv_files)} outreach CSV file(s)")
    
    for csv_path in csv_files:
        csv_filename = os.path.basename(csv_path)
        print(f"\nProcessing: {csv_filename}")
        try:
            append_csv_to_sheet(csv_filename)
        except Exception as e:
            print(f"Error processing {csv_filename}: {e}")
            continue
    
    print(f"\nCompleted processing {len(csv_files)} file(s)")

if __name__ == "__main__":
    upload_all_outreach_csvs()

