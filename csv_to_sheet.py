import gspread
from google.oauth2.service_account import Credentials
import csv
import os
import argparse
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Paths
CSV_DIR = "output/outreach"

def append_csv_to_sheet(csv_file_name):
    # Get configuration from environment variables
    google_sheet_id = os.getenv("GOOGLE_SHEET_ID")
    google_key_json = os.getenv("GOOGLE_KEY_JSON")
    
    if not google_sheet_id:
        raise ValueError("GOOGLE_SHEET_ID environment variable is not set")
    if not google_key_json:
        raise ValueError("GOOGLE_KEY_JSON environment variable is not set")
    
    # Parse JSON key from string
    try:
        key_dict = json.loads(google_key_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in GOOGLE_KEY_JSON: {e}")
    
    # Authenticate with service account
    scope = ["https://spreadsheets.google.com/feeds", 
             "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(key_dict, scopes=scope)
    client = gspread.authorize(creds)
    
    # Open the Google Sheet
    sheet = client.open_by_key(google_sheet_id)
    
    # Get sheet name from CSV filename (without extension)
    sheet_name = os.path.splitext(csv_file_name)[0]
    
    # Check if sheet already exists
    try:
        worksheet = sheet.worksheet(sheet_name)
        print(f"Sheet '{sheet_name}' already exists. Skipping append.")
        return
    except gspread.exceptions.WorksheetNotFound:
        # Sheet doesn't exist, create it
        worksheet = sheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
    
    # Read CSV file
    csv_path = os.path.join(CSV_DIR, csv_file_name)
    with open(csv_path, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f)
        rows = list(csv_reader)
    
    if not rows:
        print(f"No data found in {csv_file_name}")
        return
    
    headers = rows[0]
    data_rows = rows[1:]
    
    # Add headers and data to new sheet
    worksheet.append_row(headers, value_input_option='RAW')
    print(f"Added headers to sheet '{sheet_name}'")
    
    # Append data rows
    if data_rows:
        worksheet.append_rows(data_rows, value_input_option='RAW')
        print(f"Appended {len(data_rows)} rows to sheet '{sheet_name}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append CSV file to Google Sheet")
    parser.add_argument("csv_file", help="Name of CSV file in output/outreach/")
    args = parser.parse_args()
    
    append_csv_to_sheet(args.csv_file)

