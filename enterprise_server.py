import uuid
import csv
import io
import traceback
import os
import json
import sqlite3
import re
import logging
import uvicorn
import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
# NEU: BackgroundTasks importiert
from fastapi import FastAPI, Form, Request, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from fpdf import FPDF
# NEU: Import für PDF-Lesen
from pypdf import PdfReader

# --- 1. KONFIGURATION ---
load_dotenv()
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PUBLIC_URL = os.getenv("PUBLIC_URL")

PENDING_BATCHES = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("TÜV_AI")


# --- 2. DATENBANK ---
def get_db_connection():
    db_path = os.path.expanduser("~/tuev_nord.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def setup_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS employees')

    cursor.execute('CREATE TABLE employees (name TEXT, email TEXT, department TEXT, role TEXT)')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT,
            assignee TEXT,
            department TEXT,
            deadline TEXT,
            status TEXT,
            priority TEXT,
            original_text TEXT,
            rephrased_text TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    employees = []
    # IT SECURITY
    employees.append(("Mathias Lennart", "mervicellium@gmail.com", "IT Security", "Head"))
    employees.append(("Franck Effa", "loiceffa9@gmail.com", "IT Security", "Staff"))
    employees.append(("Florian Wirtz", "tervicellium@gmail.com", "IT Security", "Staff"))

    # RENEWABLE ENERGY
    employees.append(("Dr. Green", "franckloiceffaawoulbe@gmail.com", "Renewable Energy", "Head"))
    employees.append(("Derick Tage", "pervicellium@gmail.com", "Renewable Energy", "Staff"))
    employees.append(("Kevin Opa", "effaawoulbefranckloic@gmail.com", "Renewable Energy", "Staff"))

    # ELEKTROTECHNIK
    employees.append(("Lars Fischer", "mervicellium@gmail.com", "Elektrotechnik", "Head"))
    employees.append(("Hans Kabel", SENDER_EMAIL, "Elektrotechnik", "Staff"))

    # GENERAL MANAGEMENT
    employees.append(("General Manager", "gamescomlg2024@gmail.com", "General Management", "Head"))

    cursor.executemany('INSERT INTO employees VALUES (?, ?, ?, ?)', employees)
    conn.commit()
    conn.close()
    logger.info("✅ Datenbank initialisiert.")


# NEU: Funktion zum Bereinigen der alten Aufgaben
def clear_tasks_table():
    try:
        conn = get_db_connection()
        conn.execute("DELETE FROM tasks")  # Löscht alle Einträge
        # Optional: Setze Auto-Increment zurück (sqlite spezifisch)
        conn.execute("DELETE FROM sqlite_sequence WHERE name='tasks'")
        conn.commit()
        conn.close()
        logger.info("🧹 Datenbank bereinigt: Alte Aufgaben entfernt für neuen Durchlauf.")
    except Exception as e:
        logger.error(f"⚠️ Fehler beim Bereinigen der Datenbank: {e}")


# --- HELPER: PDF Reader ---
def read_pdf_content(file_path_or_stream):
    """Liest Text aus einer PDF Datei (Pfad oder Stream)"""
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


# --- 3. KI-ENGINE (Nur Gemini) ---

def run_with_timeout(func, timeout=2):
    """Führt func() aus und stoppt garantiert nach <timeout> Sekunden."""
    import threading

    result = {}

    def wrapper():
        try:
            result["value"] = func()
        except Exception as e:
            result["error"] = e

    t = threading.Thread(target=wrapper)
    t.start()
    t.join(timeout)

    if not t.is_alive():
        return result.get("value", None)

    logger.error(f"⏳ KI Timeout – Anfrage nach {timeout}s abgebrochen.")
    return None


def get_system_prompt(available_depts, sentences):
    """Generiert den Prompt für die KI."""
    return f"""
        Du bist ein intelligenter Dispatcher für den TÜV Nord.
        Verfügbare Abteilungen: {", ".join(available_depts)}

        REGELN FÜR DIE ANTWORT:
        1. Analysiere jeden Satz. Korrigiere ZUERST alle Rechtschreibfehler.
        2. Ordne den Satz einer Abteilung zu.
        3. Erstelle im Feld 'rephrased_text' eine neue Formulierung basierend auf folgenden Schablonen:

           - WENN Abteilung == 'IT Security':
             Nutze eine geeignete Corporate Langage wie: "Es wurde eine potenzielle Sicherheitslücke identifiziert; bitte prüfen Sie, ob (Name des Mitarbeiters, wenn vorgeschlagen) diese entsprechend analysieren und beheben kann."

           - WENN Abteilung == 'Renewable Energy':
             Nutze eine geeignete Corporate Langage im Form von dem vom IT Security

           - WENN Abteilung == 'Elektrotechnik':
             Nutze eine geeignete Corporate Langage im Form von dem vom IT Security

           - Sonst (General Management):
             Nutze: "Es liegt ein Vorgang vor, der Ihrer Aufmerksamkeit bedarf: (hier steht deine gute Umformulierung). Bitte veranlassen Sie die weitere Bearbeitung."


        4. EXTRAHIERE METADATEN (deadline, status, priority).

        Eingabe-Sätze:
        {json.dumps(sentences)}

        Antworte als JSON-Liste: 
        [
            {{
                "index": 0, 
                "dept": "Abteilung", 
                "summary": "Kurzbeschreibung", 
                "person": "Name oder null",
                "rephrased_text": "Der Schablonen-Text",
                "deadline": "...",
                "status": "...",
                "priority": "..."
            }},
            ...
        ]
        """


def call_gemini_engine(prompt):
    """
    Haupt-Engine: Google Gemini mit SMART MODEL SWITCHING.
    Nutzt die vom User ermittelten schnellsten Modelle ohne Diagnose-Overhead.
    """
    try:
        import google.generativeai as genai

        if not GEMINI_API_KEY:
            logger.error("❌ FEHLER: Variable GEMINI_API_KEY ist leer!")
            return None

        genai.configure(api_key=GEMINI_API_KEY)

        # Deine Liste der funktionierenden Modelle (Priorität: Schnellste zuerst)
        model_priority = [
            'gemini-2.5-flash-preview-09-2025',
            'gemini-flash-latest',
            'gemini-2.5-flash-lite',
            'gemini-flash-lite-latest',
            'gemini-2.5-flash-lite-preview-09-2025',
            'gemini-2.5-flash'
        ]

        for model_name in model_priority:
            try:
                model = genai.GenerativeModel(model_name)

                # Sende Anfrage
                response = model.generate_content(prompt)

                if not response.text:
                    continue  # Nächstes Modell probieren

                clean_json = response.text.replace("```json", "").replace("```", "").strip()
                logger.info(f"✅ Gemini Antwort erhalten ({model_name}).")
                return json.loads(clean_json)

            except Exception as api_error:
                err_msg = str(api_error)

                # Fehlerbehandlung ohne Wartezeit - direktes Umschalten
                if "429" in err_msg:
                    continue
                elif "404" in err_msg or "not found" in err_msg.lower():
                    continue
                elif "401" in err_msg:
                    logger.critical("⛔ API Key ungültig! Abbruch.")
                    return None
                else:
                    logger.warning(f"⚠️ Fehler bei {model_name}: {err_msg}")
                    continue

        logger.error("❌ Alle KI-Modelle sind beschäftigt oder nicht verfügbar.")
        return None

    except Exception as e:
        logger.error(f"❌ Kritischer KI-Fehler: {e}")
        return None


def ask_ai_batch(sentences, available_depts):
    """
    Orchestriert den KI-Aufruf (Nur Gemini).
    Nutzt run_with_timeout, um ewiges Hängen zu verhindern.
    """
    prompt = get_system_prompt(available_depts, sentences)
    # 180s Timeout
    return run_with_timeout(lambda: call_gemini_engine(prompt), timeout=180)


def local_text_cleanup(text):
    """Notfall-Korrektur."""
    text = text.replace("Probblem", "Problem").replace("probblem", "Problem")
    text = text.replace("Energ ", "Energie ")
    text = text.replace("Energiequelle", "Energiequelle")
    text = text.strip()
    return text


def local_keyword_classifier(text):
    """Fallback Logik."""
    text_lower = text.lower()
    scores = {"IT Security": 0, "Renewable Energy": 0, "Elektrotechnik": 0}

    if re.search(r'\bit\b', text_lower): scores["IT Security"] += 10
    if any(w in text_lower for w in
           ["server", "cyber", "firewall", "patch", "sicherheitslücke", "software", "hack", "code"]):
        scores["IT Security"] += 5

    if any(w in text_lower for w in
           ["wind", "solar", "energie", "energy", "energ", "renewable", "quelle", "kraftwerk"]):
        scores["Renewable Energy"] += 5

    if any(w in text_lower for w in
           ["kabel", "spannung", "volt", "elektro", "auto", "fahrzeug", "startet", "akku", "batterie"]):
        scores["Elektrotechnik"] += 5

    best_dept = max(scores, key=scores.get)
    if scores[best_dept] == 0: return "General Management"
    return best_dept


def intelligent_split_and_process(full_text):
    # Splitter
    clean_text = full_text.replace(" und ", ". ").replace(" sowie ", ". ").replace(" außerdem ", ". ")

    # WICHTIGE ÄNDERUNG: Wir splitten jetzt auch bei ZEILENUMBRÜCHEN (\n).
    # Das ist extrem wichtig für Tabellen in PDFs, da diese oft keine Punkte haben.
    raw_sentences = [s.strip() for s in re.split(r'[.?!]+|\n', clean_text) if len(s.strip()) > 2]

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT department FROM employees")
    depts_db = [row[0] for row in cursor.fetchall()]
    conn.close()

    tasks = []

    # 1. KI Analyse (Nur Gemini)
    ai_results = ask_ai_batch(raw_sentences, depts_db)

    if ai_results:
        logger.info(f"🧠 Gemini erfolgreich.")
        for res in ai_results:
            dept = res.get('dept')
            if dept not in depts_db: dept = "General Management"

            tasks.append({
                "dept": dept,
                "text": res.get('rephrased_text', raw_sentences[res['index']]),
                "person": res.get('person'),
                "deadline": res.get('deadline', 'beliebig'),
                "status": res.get('status', 'in Bearbeitung'),
                "priority": res.get('priority', 'Mittel')
            })
    else:
        # 2. Lokaler Fallback
        logger.info("⚠️ Lokaler Fallback (Keine KI verfügbar oder Timeout).")
        last_dept = "General Management"

        for s in raw_sentences:
            s_clean = local_text_cleanup(s)
            dept = local_keyword_classifier(s_clean)

            if dept == "General Management" and last_dept != "General Management" and len(s.split()) < 8:
                dept = last_dept
            else:
                last_dept = dept

            suggested_person = None
            if "franck" in s.lower(): suggested_person = "Franck Effa"
            if "kevin" in s.lower(): suggested_person = "Kevin Opa"
            if "derick" in s.lower(): suggested_person = "Derick Tage"

            tasks.append({
                "dept": dept,
                "text": s_clean,
                "person": suggested_person,
                "deadline": "beliebig",
                "status": "in Bearbeitung",
                "priority": "Mittel"
            })

    return tasks


# --- 4. EMAIL SERVICE ---
# FIX: 'attachment_path' wurde entfernt, da wir nur noch Links senden
def send_email_smtp(to_email, subject, body, is_html=False):
    logger.info(f"📧 [SMTP] Vorbereitung E-Mail an {to_email}...")
    msg = MIMEMultipart()
    msg['From'] = f"TÜV Nord AI <{SENDER_EMAIL}>"
    msg['To'] = to_email
    msg['Subject'] = subject
    type = 'html' if is_html else 'plain'
    msg.attach(MIMEText(body, type))

    # Anhang-Logik ENTFERNT -> Das verhindert Timeouts bei großen Dateien!

    try:
        # Normales Timeout von 30s reicht jetzt locker, da die Mail nur wenige KB hat
        logger.info(f"⏳ [SMTP] Verbinde zu smtp.gmail.com:587 (STARTTLS)...")
        with smtplib.SMTP('smtp.gmail.com', 587, timeout=30) as server:
            # server.set_debuglevel(1) # Kannst du aktivieren, wenn es immer noch hakt
            server.ehlo()
            server.starttls()
            server.ehlo()

            logger.info(f"🔑 [SMTP] Login als {SENDER_EMAIL}...")
            server.login(SENDER_EMAIL, SENDER_PASSWORD)

            logger.info(f"📤 [SMTP] Sende Link-Mail an {to_email}...")
            server.sendmail(SENDER_EMAIL, to_email, msg.as_string())

        logger.info(f"✅ [SMTP] Link-Mail erfolgreich gesendet an {to_email}")
        return True
    except socket.timeout:
        logger.error(f"❌ [SMTP] ZEITÜBERSCHREITUNG bei {to_email}.")
        return False
    except Exception as e:
        logger.error(f"❌ [SMTP] Fehler bei {to_email}: {e}")
        return False


# --- 5. DISPATCHING ---
def dispatch_department_batch(dept, task_items):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, email FROM employees WHERE department=? AND role='Head'", (dept,))
    head = cursor.fetchone()

    staff_options = []
    is_general_management = (dept == "General Management")

    if is_general_management:
        cursor.execute("SELECT DISTINCT department FROM employees WHERE department != 'General Management'")
        staff_options = [f"ABTEILUNG: {r[0]}" for r in cursor.fetchall()]
    else:
        cursor.execute("SELECT name FROM employees WHERE department=? AND role='Staff'", (dept,))
        staff_options = [r[0] for r in cursor.fetchall()]

    staff_options.append("SELF (Selbst erledigen)")
    conn.close()

    if not head: return f"Fehler: Kein Chef für {dept}"

    batch_id = str(uuid.uuid4())

    prio_map = {"Hoch": 1, "Mittel": 2, "Niedrig": 3}
    task_items.sort(key=lambda x: prio_map.get(x.get('priority', 'Mittel'), 2))

    PENDING_BATCHES[batch_id] = {
        "dept": dept,
        "head_name": head['name'],
        "head_email": head['email'],
        "items": [],
        "options": staff_options,
        "is_general": is_general_management
    }

    tasks_html = ""
    for item in task_items:
        suggestion = item['person']
        if not suggestion:
            suggestion = "Bitte wählen" if is_general_management else (staff_options[0] if staff_options else "Keiner")

        task_id = str(uuid.uuid4())
        PENDING_BATCHES[batch_id]['items'].append({
            "id": task_id,
            "text": item['text'],
            "suggestion": suggestion,
            "meta": {
                "deadline": item.get('deadline'),
                "status": item.get('status'),
                "priority": item.get('priority')
            }
        })

        prio_color = "red" if item.get('priority') == "Hoch" else "black"
        tasks_html += f"""
        <div style="background:#f9f9f9; padding:15px; margin-bottom:10px; border-left:4px solid #333;">
            <p style="white-space: pre-line;">{item['text']}</p>
            <div style="font-size:0.9em; color:#555; margin-top:5px; display:flex; gap:15px;">
                <span>📅 Deadline: <strong>{item.get('deadline')}</strong></span>
                <span>⚡ Status: <strong>{item.get('status')}</strong></span>
                <span style="color:{prio_color}">🔥 Prio: <strong>{item.get('priority')}</strong></span>
            </div>
            <p style="margin-top:10px;">👉 <em>Vorschlag: {suggestion}</em></p>
        </div>
        """

    link = f"{PUBLIC_URL}/batch_validation?batch_id={batch_id}"
    color = "#003366"
    if is_general_management: color = "#B22222"

    html_body = f"""
    <html><body style="font-family: Arial;">
        <div style="background:{color}; color:white; padding:10px;"><h2>Validierung: {dept}</h2></div>
        <p>Hallo {head['name']},</p>
        <p>Es liegen <strong>{len(task_items)} neue Vorgänge</strong> vor (sortiert nach Priorität):</p>
        {tasks_html}
        <br>
        <a href="{link}" style="background-color: {color}; color: white; padding: 12px 20px; text-decoration: none; border-radius: 5px;">
            Zum Entscheidungs-Cockpit öffnen
        </a>
    </body></html>
    """

    send_email_smtp(head['email'], f"📋 Batch ({len(task_items)}) - {dept}", html_body, is_html=True)
    return f"{dept}: {len(task_items)} Aufgaben gesendet."


# --- 6. API SERVER ---
app = FastAPI()
setup_database()


class UserInput(BaseModel):
    text: str


@app.post("/submit_task")
async def submit_task(input: UserInput):
    try:
        # WICHTIG: Tabelle leeren, bevor neue Aufgaben verarbeitet werden
        clear_tasks_table()

        raw_input = input.text.strip()
        text_to_process = raw_input

        # CHECK: Ist es ein Dateipfad zu einer PDF?
        if raw_input.lower().endswith(".pdf"):
            if os.path.exists(raw_input):
                logger.info(f"📂 Lokale PDF erkannt: {raw_input}")
                pdf_content = read_pdf_content(raw_input)
                if pdf_content:
                    text_to_process = pdf_content
                    logger.info(f"📄 PDF Text extrahiert ({len(text_to_process)} Zeichen)")
                else:
                    return {"status": "Error", "msg": "PDF ist leer oder nicht lesbar."}
            else:
                # WENN FILE NICHT GEFUNDEN: Sofortiger Abbruch mit Fehlermeldung
                logger.warning(f"Datei nicht gefunden: {raw_input}")
                return {"status": "Error", "msg": f"❌ Datei '{raw_input}' wurde nicht im Server-Verzeichnis gefunden!"}
        else:
            logger.info(f"Multi-Input (Text): {text_to_process[:50]}...")

        all_tasks = await run_in_threadpool(intelligent_split_and_process, text_to_process)

        dept_groups = {}
        for t in all_tasks:
            d = t['dept']
            if d not in dept_groups: dept_groups[d] = []
            dept_groups[d].append(t)

        report_lines = []
        for dept, items in dept_groups.items():
            if items:
                res = await run_in_threadpool(dispatch_department_batch, dept, items)
                report_lines.append(res)

        return {"status": "OK", "msg": " | ".join(report_lines)}

    except Exception as e:
        logger.error(f"SERVER ERROR: {e}")
        return {"status": "Error", "msg": str(e)}


@app.post("/upload_transcript")
async def upload_transcript(file: UploadFile = File(...)):
    try:
        # WICHTIG: Auch beim Datei-Upload erst aufräumen
        clear_tasks_table()

        filename = file.filename
        full_text = ""

        # Unterscheidung CSV oder PDF
        if filename.lower().endswith(".pdf"):
            logger.info(f"📂 PDF Upload erkannt: {filename}")
            # SpooledTemporaryFile direkt an PdfReader übergeben
            full_text = read_pdf_content(file.file)
        else:
            # Annahme: CSV / Text
            content = await file.read()
            text_content = content.decode("utf-8")

            csv_file = io.StringIO(text_content)
            reader = csv.reader(csv_file)
            for row in reader:
                row_text = " ".join([cell.strip() for cell in row if cell and cell.strip()])
                if row_text:
                    full_text += row_text + ". "

        logger.info(f"Upload verarbeitet. Textlänge: {len(full_text)}")
        all_tasks = await run_in_threadpool(intelligent_split_and_process, full_text)

        dept_groups = {}
        for t in all_tasks:
            d = t.get('dept', 'General')
            if d not in dept_groups:
                dept_groups[d] = []
            dept_groups[d].append(t)

        report_lines = []
        for dept, items in dept_groups.items():
            if items:
                res = await run_in_threadpool(dispatch_department_batch, dept, items)
                report_lines.append(str(res))

        return {"status": "OK", "msg": "Datei verarbeitet: " + " | ".join(report_lines)}

    except Exception as e:
        logger.error(f"UPLOAD ERROR: {e}")
        return {"status": "Error", "msg": str(e)}


# --- 7. WEB PORTAL ---
@app.get("/batch_validation", response_class=HTMLResponse)
def get_batch_form(batch_id: str):
    if batch_id not in PENDING_BATCHES: return "<h1>Link abgelaufen.</h1>"
    data = PENDING_BATCHES[batch_id]

    task_rows = ""
    for item in data['items']:
        options_html = ""
        for opt in data['options']:
            sel = "selected" if opt == item['suggestion'] else ""
            options_html += f"<option value='{opt}' {sel}>{opt}</option>"

        meta = item['meta']

        task_rows += f"""
        <div style="border:1px solid #ddd; padding:15px; margin-bottom:15px; border-radius:5px;">
            <p style="white-space: pre-line; margin-bottom:5px;">{item['text']}</p>
            <div style="background:#eee; padding:5px; font-size:0.85em; margin-bottom:10px; border-radius:4px;">
                <strong>Deadline:</strong> {meta['deadline']} | 
                <strong>Status:</strong> {meta['status']} | 
                <strong>Prio:</strong> {meta['priority']}
            </div>
            <label>Zuweisen / Weiterleiten an:</label><br>
            <select name="assign_{item['id']}" style="width:100%; padding:8px;">{options_html}</select>
        </div>
        """

    return f"""
    <html><body style="font-family:sans-serif; padding:20px; background:#f0f2f5;">
        <div style="background:white; padding:30px; max-width:800px; margin:auto;">
            <h2 style="color:#003366;">Cockpit: {data['dept']}</h2>
            <form action="/process_batch_approval" method="post">
                <input type="hidden" name="batch_id" value="{batch_id}">
                {task_rows}
                <button type="submit" style="background:#28a745; color:white; padding:15px 30px; border:none; width:100%; font-size:1.1em; cursor:pointer;">
                    ✔ Alle freigeben & Verteilen
                </button>
            </form>
        </div>
    </body></html>
    """


@app.post("/process_batch_approval")
async def process_batch_approval(request: Request):
    form_data = await request.form()
    batch_id = form_data.get("batch_id")
    if batch_id not in PENDING_BATCHES: return HTMLResponse("<h1>Fehler.</h1>")

    data = PENDING_BATCHES[batch_id]
    conn = get_db_connection()
    cursor = conn.cursor()

    pm_report = f"--- BERICHT: {data['dept']} ---\nFreigegeben von: {data['head_name']}\n\n"

    for item in data['items']:
        field_name = f"assign_{item['id']}"
        selected_target = form_data.get(field_name)
        meta = item['meta']

        if selected_target.startswith("ABTEILUNG: "):
            target_dept_name = selected_target.replace("ABTEILUNG: ", "")
            # Rekursiv dispatchen (Aufgabe wird weitergeleitet)
            dispatch_department_batch(target_dept_name,
                                      [{"text": item['text'], "person": None, "deadline": meta['deadline'],
                                        "status": meta['status'], "priority": meta['priority']}])
            pm_report += f"↪ WEITERGELEITET an Abteilung: {target_dept_name}\n"

        else:
            target_email = ""
            if "SELF" in selected_target:
                target_email = data['head_email']
            else:
                cursor.execute("SELECT email FROM employees WHERE name=?", (selected_target,))
                res = cursor.fetchone()
                target_email = res['email'] if res else "tervicellium@gmail.com"

            # Speichern
            cursor.execute("""
                INSERT INTO tasks (description, assignee, department, deadline, status, priority, original_text, rephrased_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (item['text'], selected_target, data['dept'], meta.get('deadline'), meta.get('status'),
                  meta.get('priority'), meta.get('summary', item['text']), item['text']))

            # Mail an Mitarbeiter
            body = f"""
            Hallo {selected_target},
            Ihnen wurde folgende Aufgabe zugewiesen:
            "{item['text']}"
            Deadline: {meta['deadline']} | Prio: {meta['priority']}
            Genehmigt von: {data['head_name']}
            """
            send_email_smtp(target_email, "ARBEITSAUFTRAG", body)
            pm_report += f"✅ '{item['text'][:30]}...' -> {selected_target} (Prio: {meta['priority']})\n"

    conn.commit()
    conn.close()

    send_email_smtp(SENDER_EMAIL, f"✅ Report: {data['dept']} erledigt", pm_report)

    # Batch aus der Liste entfernen
    del PENDING_BATCHES[batch_id]

    # CHECK: Sind ALLE Batches erledigt?
    if not PENDING_BATCHES:
        # GM Email dynamisch holen
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT email FROM employees WHERE role='Head' AND department='General Management'")
        gm_res = cursor.fetchone()
        gm_email = gm_res['email'] if gm_res else "gamescomlg2024@gmail.com"
        conn.close()

        approval_link = f"{PUBLIC_URL}/approve_pdf_ui"
        body = f"""
        Hallo General Manager,

        alle offenen Aufgaben-Batches wurden erfolgreich bearbeitet.
        Bitte bestätigen Sie die Erstellung und den Versand des Abschlussberichts (PDF).

        Hier klicken zur Freigabe:
        {approval_link}
        """
        # WICHTIG: Email wird JETZT IMMER gesendet, egal wer genehmigt hat
        send_email_smtp(gm_email, "✅ Freigabe Abschlussbericht erforderlich", body, is_html=False)

        # Wenn der GM selbst gerade den letzten Batch bearbeitet hat, leiten wir ihn ZUSÄTZLICH direkt weiter
        if data['head_email'] == gm_email:
            return approve_pdf_ui()

        return HTMLResponse(
            "<h1 style='color:green; text-align:center;'>Erledigt. General Manager wurde zur Bericht-Freigabe benachrichtigt.</h1>")

    return HTMLResponse(
        "<h1 style='color:green; text-align:center;'>Batch erledigt. Warte auf andere Abteilungen...</h1>")


# NEU: Der Dialog, den NUR der General Manager sieht (per Link oder Redirect)
@app.get("/approve_pdf_ui", response_class=HTMLResponse)
def approve_pdf_ui():
    # FIX: Mit Javascript-Feedback für den Klick!
    return """
        <html>
        <head>
            <script>
                function handleProcess(btn) {
                    btn.innerHTML = "⏳ Wird verarbeitet...";
                    btn.style.opacity = "0.7";
                    btn.style.cursor = "not-allowed";
                    // Das Formular wird normal abgesendet
                }
            </script>
        </head>
        <body style="font-family:sans-serif; background:#f0f2f5; display:flex; justify-content:center; align-items:center; height:100vh;">
            <div style="background:white; padding:40px; border-radius:10px; box-shadow:0 4px 15px rgba(0,0,0,0.1); text-align:center; max-width:500px;">
                <h1 style="color:#28a745;">Aufgaben erfolgreich verteilt!</h1>
                <p style="font-size:1.1em; color:#555;">Der Prozess für diesen Batch ist abgeschlossen.</p>
                <hr style="margin:20px 0; border:0; border-top:1px solid #eee;">

                <p style="font-weight:bold; margin-bottom:20px;">Möchten Sie jetzt den offiziellen Abschlussbericht per E-Mail generieren und verteilen?</p>
                <p style="font-size:0.9em; color:#777; margin-bottom:30px;">(PDF wird generiert und Download-Link verschickt)</p>

                <div style="display:flex; gap:15px; justify-content:center;">
                    <form action="/trigger_pdf_report" method="post">
                        <button type="submit" onclick="handleProcess(this)" style="background:#003366; color:white; padding:12px 25px; border:none; border-radius:5px; font-size:1.1em; cursor:pointer; font-weight:bold;">
                            JA, PDF-Link senden
                        </button>
                    </form>

                    <a href="#" onclick="document.body.innerHTML='<h1 style=\'text-align:center; margin-top:50px; color:#555;\'>Vorgang beendet. Keine E-Mail gesendet.</h1>'; return false;" 
                       style="display:inline-block; padding:12px 25px; border:1px solid #ccc; border-radius:5px; color:#555; text-decoration:none; font-size:1.1em; line-height:1.2;">
                        NEIN
                    </a>
                </div>
            </div>
        </body>
        </html>
    """


# --- NEU: Endpunkt zum Herunterladen des finalen Berichts (OHNE Token) ---
@app.get("/download_final_report")
def download_final_report():
    file_path = "TUEV_Abschlussbericht.pdf"
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=file_path, media_type='application/pdf')
    return HTMLResponse("<h1>Bericht wurde noch nicht generiert oder gelöscht.</h1>")


# --- NEU: Hintergrund-Logik für PDF & E-Mail ---
def background_report_process():
    logger.info("🎬 [BG-TASK] Starte PDF Report Generierung...")
    conn = get_db_connection()
    tasks = conn.execute("""
        SELECT * FROM tasks 
        ORDER BY CASE priority 
            WHEN 'Hoch' THEN 1 
            WHEN 'Mittel' THEN 2 
            WHEN 'Niedrig' THEN 3 
            ELSE 4 END
    """).fetchall()

    # NEU: Hole GM Email dynamisch aus DB, um sicherzugehen
    cursor = conn.cursor()
    cursor.execute("SELECT email FROM employees WHERE role='Head' AND department='General Management'")
    gm_res = cursor.fetchone()
    gm_email = gm_res['email'] if gm_res else "gamescomlg2024@gmail.com"
    conn.close()

    filename = "TUEV_Abschlussbericht.pdf"

    # 1. PDF Generieren (Wieder aktiviert!)
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", 'B', 16)
        pdf.cell(190, 10, "TUEV Nord - Abschlussbericht", ln=True, align='C')
        pdf.ln(10)

        pdf.set_font("Arial", 'B', 10)
        pdf.cell(90, 10, "Aufgabe", 1)
        pdf.cell(40, 10, "Verantwortlich", 1)
        pdf.cell(30, 10, "Deadline", 1)
        pdf.cell(30, 10, "Prio", 1)
        pdf.ln()

        pdf.set_font("Arial", '', 9)
        for t in tasks:
            def clean_str(s):
                return str(s).encode('latin-1', 'replace').decode('latin-1')

            txt = t['description']
            txt = (txt[:45] + '...') if len(txt) > 45 else txt
            pdf.cell(90, 10, clean_str(txt), 1)
            pdf.cell(40, 10, clean_str(t['assignee']), 1)
            pdf.cell(30, 10, clean_str(t['deadline']), 1)
            pdf.cell(30, 10, clean_str(t['priority']), 1)
            pdf.ln()

        pdf.output(filename)
        logger.info(f"✅ [BG-TASK] PDF erfolgreich erstellt und lokal gespeichert: {filename}")

    except Exception as e:
        logger.error(f"❌ [BG-TASK] Fehler bei PDF Erstellung: {e}")
        return  # Abbruch bei Fehler

    # 2. Email senden (NUR LINK, KEIN ANHANG!)
    # Liste der Empfänger: GM und PM
    recipients = [gm_email, "hervicellium9@gmail.com"]
    success_count = 0

    # Download Link konstruieren OHNE Token für dauerhaften Zugriff
    download_link = f"{PUBLIC_URL}/download_final_report"

    for email in recipients:
        logger.info(f"🚀 [BG-TASK] Starte Link-Versand für: {email}...")
        role = "General Manager" if email == gm_email else "Projektmanager"

        # HTML Body mit schönem Button
        full_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">
                <h2 style="color: #003366;">TÜV Nord - Abschlussbericht</h2>
                <p>Sehr geehrte Damen und Herren,</p>
                <p>der offizielle Abschlussbericht wurde erfolgreich generiert und auf dem Sicherheitsserver hinterlegt.</p>

                <div style="text-align: center; margin: 30px 0;">
                    <a href="{download_link}" style="background-color: #003366; color: white; padding: 15px 25px; text-decoration: none; font-weight: bold; border-radius: 5px; font-size: 16px;">
                        ⬇️ PDF Bericht herunterladen
                    </a>
                </div>

                <p style="font-size: 0.9em; color: #666;">
                    Falls der Button nicht funktioniert, kopieren Sie diesen Link:<br>
                    <a href="{download_link}">{download_link}</a>
                </p>

                <hr style="margin-top: 30px; border: 0; border-top: 1px solid #eee;">
                <p style="font-size: 0.8em; color: #999;">TÜV Nord AI System | Automatisch generierte Nachricht</p>
            </div>
        </body>
        </html>
        """

        # Sende als HTML (ohne Anhang -> Schnell & Zuverlässig)
        if send_email_smtp(email, f"TÜV Abschlussbericht: Download bereit ({role})", full_body, is_html=True):
            success_count += 1
            logger.info(f"✅ [BG-TASK] Email erfolgreich an {email}")
        else:
            logger.error(f"❌ [BG-TASK] Email fehlgeschlagen an {email}")

    logger.info(f"🏁 [BG-TASK] Prozess beendet. Mails gesendet: {success_count}/{len(recipients)}")


# FIX: Neuer Endpunkt für PDF-Trigger (JETZT MIT BACKGROUND TASKS!)
@app.post("/trigger_pdf_report")
async def trigger_pdf_report(background_tasks: BackgroundTasks):
    # Füge den schweren Task zur Warteschlange hinzu
    background_tasks.add_task(background_report_process)

    # Gib SOFORT eine Antwort zurück an den Browser
    return HTMLResponse(f"""
        <div style="font-family:sans-serif; text-align:center; padding:50px;">
            <h1 style="color:green;">✅ Prozess gestartet!</h1>
            <p>Die Generierung des Berichts und der Versand der E-Mails laufen im Hintergrund.</p>
            <p>Sie erhalten in Kürze eine E-Mail mit dem Download-Link.</p>
            <br>
            <p style="color:gray; font-size:0.9em;">(Sie können dieses Fenster jetzt schließen)</p>
        </div>
    """)


# --- 8. PDF EXPORT ENDPOINT (Bleibt für manuellen Download) ---
@app.get("/download_report")
def download_report():
    conn = get_db_connection()
    # Sortieren nach Priorität (Hoch -> Mittel -> Niedrig -> Sonstiges)
    tasks = conn.execute("""
        SELECT * FROM tasks 
        ORDER BY CASE priority 
            WHEN 'Hoch' THEN 1 
            WHEN 'Mittel' THEN 2 
            WHEN 'Niedrig' THEN 3 
            ELSE 4 END
    """).fetchall()
    conn.close()

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(190, 10, "TUEV Nord - Aufgabenstatus", ln=True, align='C')
    pdf.ln(10)

    # Header
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(90, 10, "Aufgabe", 1)
    pdf.cell(40, 10, "Verantwortlich", 1)
    pdf.cell(30, 10, "Deadline", 1)
    pdf.cell(30, 10, "Prio", 1)
    pdf.ln()

    # Body
    pdf.set_font("Arial", '', 9)
    for t in tasks:
        def clean_str(s):
            return str(s).encode('latin-1', 'replace').decode('latin-1')

        txt = t['description']
        txt = (txt[:45] + '...') if len(txt) > 45 else txt

        pdf.cell(90, 10, clean_str(txt), 1)
        pdf.cell(40, 10, clean_str(t['assignee']), 1)
        pdf.cell(30, 10, clean_str(t['deadline']), 1)
        pdf.cell(30, 10, clean_str(t['priority']), 1)
        pdf.ln()

    filename = "TUEV_Report.pdf"
    pdf.output(filename)
    return FileResponse(path=filename, filename=filename, media_type='application/pdf')


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)