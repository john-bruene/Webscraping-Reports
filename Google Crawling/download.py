import requests
from config import headers
import os
from write_results import write_json
# import dropbox

# def dropbox_upload(filepath, company, dbx):
#     """
#     Args:
#         filepath (string): pdf path to save
#         company (string)
#         dbx (Dropbox)
#     """
#     with open(filepath, "rb") as f:
#         dbx.files_upload(f.read(), "/2023-05-19 (msci)/"+company+"/"+filepath, mode=dropbox.files.WriteMode("overwrite"))

# Alternativ direkt hier definieren oder aus config importieren
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

def download_pdf(link, yearString, companyName, query, sort):
    """
    Lädt einen PDF-Link herunter und speichert ihn im lokalen Dateisystem.

    Args:
        link (str): PDF-Link
        yearString (str): Jahr des Berichts
        companyName (str): Firmenname
        query (str): Ursprüngliche Suchanfrage
        sort (str): "found" oder "doubt"

    Returns:
        str | None: Lokaler Pfad der gespeicherten Datei oder None bei Fehler
    """

    base_path = os.path.join("C:/SFDH/Downloaded Reports")
    folder_type = "foundPDFs" if sort == "found" else "doubtPDFs"

    # Berichtstyp ermitteln
    if "sustainability" in query.lower():
        report_type = "_sustainability"
    elif "annual" in query.lower():
        report_type = "_annual"
    else:
        report_type = ""

    # Zielordner
    target_folder = os.path.join(base_path, folder_type, companyName)
    os.makedirs(target_folder, exist_ok=True)

    filename = f"{yearString}{report_type}_report.pdf"
    filepath = os.path.join(target_folder, filename)

    print(f"[⇩] {companyName} ({yearString}) – Downloading: {link}")

    try:
        response = requests.get(link, headers=headers, timeout=30)
        if response.status_code == 200 and response.headers.get("Content-Type", "").lower().startswith("application/pdf"):
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"[💾] Saved to: {filepath}")
            return filepath
        else:
            raise Exception(f"Unexpected status {response.status_code} or content type {response.headers.get('Content-Type')}")
    except Exception as e:
        print(f"[❌] Download failed for {companyName} ({yearString}): {e}")
        write_json({'company': companyName, 'link': link}, 'exception_at_download.json', yearString)

        # Fehlerhafte Antwort speichern zur Analyse
        error_dir = os.path.join(base_path, "download_errors")
        os.makedirs(error_dir, exist_ok=True)
        error_filename = f"{companyName}_{yearString}.html".replace(" ", "_")
        error_path = os.path.join(error_dir, error_filename)
        try:
            with open(error_path, "wb") as f:
                f.write(response.content)
            print(f"[🕵️] Saved error HTML to: {error_path}")
        except Exception as e_inner:
            print(f"[⚠️] Failed to save error HTML: {e_inner}")

        return None