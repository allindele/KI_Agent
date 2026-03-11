import uuid
import csv
import io
import os
import uvicorn
from fastapi import FastAPI, Request, UploadFile, File, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from fpdf import FPDF

from config import logger, PENDING_BATCHES, PUBLIC_URL, SENDER_EMAIL
from database import get_db_connection, setup_database, clear_tasks_table
from email_service import send_email_smtp
from ai_engine import intelligent_split_and_process
from pdf_utils import read_pdf_content

app = FastAPI()
setup_database()

class UserInput(BaseModel):
    text: str

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
                <span> Deadline: <strong>{item.get('deadline')}</strong></span>
                <span> Status: <strong>{item.get('status')}</strong></span>
                <span style="color:{prio_color}"> Prio: <strong>{item.get('priority')}</strong></span>
            </div>
            <p style="margin-top:10px;"> <em>Vorschlag: {suggestion}</em></p>
        </div>
        """
    link = f"{PUBLIC_URL}/batch_validation?batch_id={batch_id}"
    color = "#003366"
    if is_general_management: color = "#B22222"
    html_body = f"""
    <html><body style="font-family: Arial;">
        <div style="background:{color}; color:white; padding:10px;"><h2>Validierung: {dept}</h2></div>
        <p>Hallo {head['name']},</p>
        <p>Es liegen <strong>{len(task_items)} neue Vorgänge</strong> vor:</p>
        {tasks_html}
        <br>
        <a href="{link}" style="background-color: {color}; color: white; padding: 12px 20px; text-decoration: none; border-radius: 5px;">
            Zum Entscheidungs-Cockpit öffnen
        </a>
    </body></html>
    """
    send_email_smtp(head['email'], f"Batch ({len(task_items)}) - {dept}", html_body, is_html=True)
    return f"{dept}: {len(task_items)} Aufgaben gesendet."

@app.post("/submit_task")
async def submit_task(input: UserInput):
    try:
        clear_tasks_table()
        raw_input = input.text.strip()
        text_to_process = raw_input
        if raw_input.lower().endswith(".pdf"):
            if os.path.exists(raw_input):
                logger.info(f"Lokale PDF erkannt: {raw_input}")
                pdf_content = read_pdf_content(raw_input)
                if pdf_content:
                    text_to_process = pdf_content
                else:
                    return {"status": "Error", "msg": "PDF ist leer oder nicht lesbar."}
            else:
                return {"status": "Error", "msg": f"Datei '{raw_input}' wurde nicht gefunden!"}
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
        clear_tasks_table()
        filename = file.filename
        full_text = ""
        if filename.lower().endswith(".pdf"):
            full_text = read_pdf_content(file.file)
        else:
            content = await file.read()
            text_content = content.decode("utf-8")
            csv_file = io.StringIO(text_content)
            reader = csv.reader(csv_file)
            for row in reader:
                row_text = " ".join([cell.strip() for cell in row if cell and cell.strip()])
                if row_text:
                    full_text += row_text + ". "
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
                    Alle freigeben & Verteilen
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
            dispatch_department_batch(target_dept_name,
                                      [{"text": item['text'], "person": None, "deadline": meta['deadline'],
                                        "status": meta['status'], "priority": meta['priority']}])
            pm_report += f" WEITERGELEITET an Abteilung: {target_dept_name}\n"
        else:
            target_email = ""
            if "SELF" in selected_target:
                target_email = data['head_email']
            else:
                cursor.execute("SELECT email FROM employees WHERE name=?", (selected_target,))
                res = cursor.fetchone()
                target_email = res['email'] if res else "tervicellium@gmail.com"
            cursor.execute("""
                INSERT INTO tasks (description, assignee, department, deadline, status, priority, original_text, rephrased_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (item['text'], selected_target, data['dept'], meta.get('deadline'), meta.get('status'),
                  meta.get('priority'), meta.get('summary', item['text']), item['text']))
            body = f"""
            Hallo {selected_target},
            Ihnen wurde folgende Aufgabe zugewiesen:
            "{item['text']}"
            Deadline: {meta['deadline']} | Prio: {meta['priority']}
            Genehmigt von: {data['head_name']}
            """
            send_email_smtp(target_email, "ARBEITSAUFTRAG", body)
            pm_report += f" '{item['text'][:30]}...' -> {selected_target} (Prio: {meta['priority']})\n"
    conn.commit()
    conn.close()
    send_email_smtp(SENDER_EMAIL, f"Report: {data['dept']} erledigt", pm_report)
    del PENDING_BATCHES[batch_id]
    if not PENDING_BATCHES:
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
        send_email_smtp(gm_email, "Freigabe Abschlussbericht erforderlich", body, is_html=False)
        if data['head_email'] == gm_email:
            return approve_pdf_ui()
        return HTMLResponse("<h1 style='color:green; text-align:center;'>Erledigt. General Manager wurde zur Bericht-Freigabe benachrichtigt.</h1>")
    return HTMLResponse("<h1 style='color:green; text-align:center;'>Batch erledigt. Warte auf andere Abteilungen...</h1>")

@app.get("/approve_pdf_ui", response_class=HTMLResponse)
def approve_pdf_ui():
    return """
        <html>
        <head>
            <script>
                function handleProcess(btn) {
                    btn.innerHTML = " Wird verarbeitet...";
                    btn.style.opacity = "0.7";
                    btn.style.cursor = "not-allowed";
                }
            </script>
        </head>
        <body style="font-family:sans-serif; background:#f0f2f5; display:flex; justify-content:center; align-items:center; height:100vh;">
            <div style="background:white; padding:40px; border-radius:10px; box-shadow:0 4px 15px rgba(0,0,0,0.1); text-align:center; max-width:500px;">
                <h1 style="color:#28a745;">Aufgaben erfolgreich verteilt!</h1>
                <p style="font-size:1.1em; color:#555;">Der Prozess für diesen Batch ist abgeschlossen.</p>
                <hr style="margin:20px 0; border:0; border-top:1px solid #eee;">
                <p style="font-weight:bold; margin-bottom:20px;">Möchten Sie jetzt den offiziellen Abschlussbericht per E-Mail generieren und verteilen?</p>
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

def background_report_process():
    conn = get_db_connection()
    tasks = conn.execute("""
        SELECT * FROM tasks 
        ORDER BY CASE priority 
            WHEN 'Hoch' THEN 1 
            WHEN 'Mittel' THEN 2 
            WHEN 'Niedrig' THEN 3 
            ELSE 4 END
    """).fetchall()
    cursor = conn.cursor()
    cursor.execute("SELECT email FROM employees WHERE role='Head' AND department='General Management'")
    gm_res = cursor.fetchone()
    gm_email = gm_res['email'] if gm_res else "gamescomlg2024@gmail.com"
    conn.close()
    filename = "TechCorp_Abschlussbericht.pdf"
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", 'B', 16)
        pdf.cell(190, 10, "TechCorp - Abschlussbericht", ln=True, align='C')
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
    except Exception as e:
        logger.error(f"Fehler bei PDF Erstellung: {e}")
        return
    recipients = [gm_email, "hervicellium9@gmail.com"]
    download_link = f"{PUBLIC_URL}/download_final_report"
    for email in recipients:
        role = "General Manager" if email == gm_email else "Projektmanager"
        full_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">
                <h2 style="color: #003366;">TechCorp - Abschlussbericht</h2>
                <p>Sehr geehrte Damen und Herren,</p>
                <p>der offizielle Abschlussbericht wurde erfolgreich generiert und auf dem Sicherheitsserver hinterlegt.</p>
                <div style="text-align: center; margin: 30px 0;">
                    <a href="{download_link}" style="background-color: #003366; color: white; padding: 15px 25px; text-decoration: none; font-weight: bold; border-radius: 5px; font-size: 16px;">
                         PDF Bericht herunterladen
                    </a>
                </div>
                <hr style="margin-top: 30px; border: 0; border-top: 1px solid #eee;">
                <p style="font-size: 0.8em; color: #999;">TechCorp AI System | Automatisch generierte Nachricht</p>
            </div>
        </body>
        </html>
        """
        send_email_smtp(email, f"TechCorp Abschlussbericht: Download bereit ({role})", full_body, is_html=True)

@app.post("/trigger_pdf_report")
async def trigger_pdf_report(background_tasks: BackgroundTasks):
    background_tasks.add_task(background_report_process)
    return HTMLResponse(f"""
        <div style="font-family:sans-serif; text-align:center; padding:50px;">
            <h1 style="color:green;"> Prozess gestartet!</h1>
            <p>Die Generierung des Berichts und der Versand der E-Mails laufen im Hintergrund.</p>
            <p>Sie erhalten in Kürze eine E-Mail mit dem Download-Link.</p>
        </div>
    """)

@app.get("/download_final_report")
def download_final_report():
    file_path = "TechCorp_Abschlussbericht.pdf"
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=file_path, media_type='application/pdf')
    return HTMLResponse("<h1>Bericht wurde noch nicht generiert oder gelöscht.</h1>")

@app.get("/download_report")
def download_report():
    conn = get_db_connection()
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
    pdf.cell(190, 10, "TechCorp - Aufgabenstatus", ln=True, align='C')
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
    filename = "TechCorp_Report.pdf"
    pdf.output(filename)
    return FileResponse(path=filename, filename=filename, media_type='application/pdf')

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
