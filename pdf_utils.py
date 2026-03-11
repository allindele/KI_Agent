from pypdf import PdfReader
from config import logger

def read_pdf_content(file_path_or_stream):
    try:
        reader = PdfReader(file_path_or_stream)
        full_text = ""
        for page in reader.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"
        return full_text
    except Exception as e:
        logger.error(f"Fehler beim Lesen der PDF: {e}")
        return ""
