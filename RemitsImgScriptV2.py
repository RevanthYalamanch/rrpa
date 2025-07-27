import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
POPPLER_PATH = r"C:\Poppler\poppler-24.08.0\Library\bin"

PDF_FOLDER = r"C:\Users\Revanth.REVANTH-SL3\Desktop\Blue Cross PDFs\BC TEST"
OUTPUT_EXCEL = r"C:\Users\Revanth.REVANTH-SL3\Downloads\BC - Provider Claim Payments(Sheet1).csv"
DEBUG_FOLDER = r"C:\Users\Revanth.REVANTH-SL3\Desktop\Blue Cross PDFs\Debug folder"

COLUMNS = ["Tax:", "Provider", "Date of Service From", "Date of Service Thru", "Payment"]

def ocr_extract_text(pdf_path):
    images = convert_from_path(pdf_path, dpi=250, poppler_path=POPPLER_PATH)
    text_content = [pytesseract.image_to_string(img) for img in images]
    full_text = "\n".join(text_content)
    os.makedirs(DEBUG_FOLDER, exist_ok=True)
    debug_file = os.path.join(DEBUG_FOLDER, os.path.basename(pdf_path) + "_OCR.txt")
    with open(debug_file, "w", encoding="utf-8") as f:
        f.write(full_text)
    return full_text

COLUMNS = ["Tax:", "Provider", "Date of Service From", "Date of Service Thru", "Payment"]

def parse_text_to_data(text, file_name):
    tax_pattern = r"TAX:\s*(\d+)"
    provider_pattern = r"PROVIDER:\s*(\d+)"
    additional_identifier_pattern = r"Additional Identifier:\s*TJ\s*/\s*(\d+)"
    date_pattern = r"\d{2}/\d{2}/\d{2,4}"
    payment_pattern = r"\d+\.\d{2}"
    tax_match = re.search(tax_pattern, text)
    provider_match = re.search(provider_pattern, text)
    additional_identifier_match = re.search(additional_identifier_pattern, text)
    tax_value = tax_match.group(1) if tax_match else ""
    provider_value = provider_match.group(1) if provider_match else ""
    if not tax_value and additional_identifier_match:
        tax_value = additional_identifier_match.group(1)
    dates = re.findall(date_pattern, text)
    payments = re.findall(payment_pattern, text)
    date_from = dates[::2]
    date_thru = dates[1::2]
    data = []
    max_len = max(len(date_from), len(date_thru), len(payments))
    data.append(
            {"Tax:": file_name, "Provider": "", "Date of Service From": "", "Date of Service Thru": "", "Payment": ""}
        )
    for i in range(max_len):
        data.append({
            "Tax:": tax_value,
            "Provider": provider_value,
            "Date of Service From": date_from[i] if i < len(date_from) else "",
            "Date of Service Thru": date_thru[i] if i < len(date_thru) else "",
            "Payment": payments[i] if i < len(payments) else ""
        })
        
    
    return pd.DataFrame(data, columns=COLUMNS)


def process_single_pdf(file):
    pdf_path = os.path.join(PDF_FOLDER, file)
    text = ocr_extract_text(pdf_path)
    return parse_text_to_data(text, file)

def process_pdfs_parallel():
    pdf_files = [f for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf")]
    total_files = len(pdf_files)
    all_data = []
    print(f"Starting OCR on {total_files} PDFs using {cpu_count()} CPU cores...\n")
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_single_pdf, file): file for file in pdf_files}
        for i, future in enumerate(as_completed(futures), 1):
            file = futures[future]
            try:
                result = future.result()
                all_data.append(result)
                print(f"[{i}/{total_files}] Completed: {file}")
            except Exception as e:
                print(f"[ERROR] Failed on {file}: {e}")
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        os.makedirs(os.path.dirname(OUTPUT_EXCEL), exist_ok=True)
        combined.to_csv(OUTPUT_EXCEL, index=False)
        print(f"\nData saved to {OUTPUT_EXCEL}")
    else:
        print("No data extracted — check DebugOCR folder for OCR outputs.")

if __name__ == "__main__":
    process_pdfs_parallel()
    # add space between each line
