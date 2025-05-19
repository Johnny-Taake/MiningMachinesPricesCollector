from datetime import datetime
from pathlib import Path
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build  # ← Drive API
from src.config import settings


def ensure_folder(drive, folder): 
    # None → root
    if not folder:  
        return None

    # Id is 25–60-characters long
    looks_like_id = len(folder) > 20 and " " not in folder and "/" not in folder
    if looks_like_id:
        # Ensure the folder exists
        try:
            drive.files().get(fileId=folder, fields="id").execute()
            return folder
        except Exception:
            raise ValueError("Папка с таким ID не найдена")

    # Otherwise, search by name
    q = f"mimeType='application/vnd.google-apps.folder' and name='{folder}' and trashed=false"
    res = drive.files().list(q=q, fields="files(id)", pageSize=1).execute()
    if res["files"]:
        return res["files"][0]["id"]

    # Create a new folder
    body = {"name": folder, "mimeType": "application/vnd.google-apps.folder"}
    folder_id = drive.files().create(body=body, fields="id").execute()["id"]
    return folder_id


def upload_collected_files_to_google_sheets():
    EMAILS = settings.google_sheets.emails
    # Remove duplicates
    EMAILS = list(dict.fromkeys(settings.google_sheets.emails))
    
    EXCEL_DIR = Path(settings.prepared_excels_dir)
    CREDS_PATH = settings.google_sheets.client_secret_file
    TARGET_FLD = settings.google_sheets.files_folder_name
    DATETIME_FMT = "%Y_%m_%d_%H:%M"

    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    drive_svc = build("drive", "v3", credentials=creds, cache_discovery=False)

    # Ensure the target folder exists
    folder_id = ensure_folder(drive_svc, TARGET_FLD)

    # Share the folder with specified emails
    if folder_id:
        for email in EMAILS:
            drive_svc.permissions().create(
                fileId=folder_id,
                sendNotificationEmail=False,
                body={"type": "user", "role": "writer", "emailAddress": email},
            ).execute()

    # Create a new spreadsheet
    title = datetime.now().strftime(DATETIME_FMT)
    spreadsheet = gc.create(title, folder_id=folder_id)

    # Share the spreadsheet with specified emails
    for email in EMAILS:
        spreadsheet.share(email, perm_type="user", role="writer")

    file_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
    print("Документ: ", file_link)

    # Upload Excel files
    xlsx_files = sorted(EXCEL_DIR.glob("*.xlsx"))
    if not xlsx_files:
        print("Нет .xlsx")
        return

    for i, xls in enumerate(xlsx_files, 1):
        df = pd.read_excel(xls, sheet_name=0, dtype=str).fillna("")
        sheet_title = xls.stem[:100]
        ws = (
            spreadsheet.sheet1
            if i == 1
            else spreadsheet.add_worksheet(
                title=sheet_title, rows=len(df) + 10, cols=len(df.columns) + 5
            )
        )
        if i == 1:
            ws.update_title(sheet_title)
        ws.update([df.columns.tolist()] + df.values.tolist())

    print("✅ Готово")
    return file_link
