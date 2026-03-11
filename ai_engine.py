import json
import re
import threading
from config import logger, GEMINI_API_KEY
from database import get_db_connection

def run_with_timeout(func, timeout=2):
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
    logger.error(f"KI Timeout nach {timeout}s.")
    return None

def get_system_prompt(available_depts, sentences):
    return f"""
        Du bist ein intelligenter Dispatcher für TechCorp.
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
            }}
        ]
        """

def call_gemini_engine(prompt):
    try:
        import google.generativeai as genai
        if not GEMINI_API_KEY:
            logger.error("FEHLER: Variable GEMINI_API_KEY ist leer!")
            return None
        genai.configure(api_key=GEMINI_API_KEY)
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
                response = model.generate_content(prompt)
                if not response.text:
                    continue
                clean_json = response.text.replace("```json", "").replace("```", "").strip()
                logger.info(f"Gemini Antwort erhalten ({model_name}).")
                return json.loads(clean_json)
            except Exception as api_error:
                err_msg = str(api_error)
                if "429" in err_msg:
                    continue
                elif "404" in err_msg or "not found" in err_msg.lower():
                    continue
                elif "401" in err_msg:
                    logger.critical("API Key ungültig! Abbruch.")
                    return None
                else:
                    logger.warning(f"Fehler bei {model_name}: {err_msg}")
                    continue
        logger.error("Alle KI-Modelle sind beschäftigt.")
        return None
    except Exception as e:
        logger.error(f"Kritischer KI-Fehler: {e}")
        return None

def ask_ai_batch(sentences, available_depts):
    prompt = get_system_prompt(available_depts, sentences)
    return run_with_timeout(lambda: call_gemini_engine(prompt), timeout=180)

def local_text_cleanup(text):
    text = text.replace("Probblem", "Problem").replace("probblem", "Problem")
    text = text.replace("Energ ", "Energie ")
    text = text.replace("Energiequelle", "Energiequelle")
    text = text.strip()
    return text

def local_keyword_classifier(text):
    text_lower = text.lower()
    scores = {"IT Security": 0, "Renewable Energy": 0, "Elektrotechnik": 0}
    if re.search(r'\bit\b', text_lower): scores["IT Security"] += 10
    if any(w in text_lower for w in ["server", "cyber", "firewall", "patch", "sicherheitslücke", "software", "hack", "code"]):
        scores["IT Security"] += 5
    if any(w in text_lower for w in ["wind", "solar", "energie", "energy", "energ", "renewable", "quelle", "kraftwerk"]):
        scores["Renewable Energy"] += 5
    if any(w in text_lower for w in ["kabel", "spannung", "volt", "elektro", "auto", "fahrzeug", "startet", "akku", "batterie"]):
        scores["Elektrotechnik"] += 5
    best_dept = max(scores, key=scores.get)
    if scores[best_dept] == 0: return "General Management"
    return best_dept

def intelligent_split_and_process(full_text):
    clean_text = full_text.replace(" und ", ". ").replace(" sowie ", ". ").replace(" außerdem ", ". ")
    raw_sentences = [s.strip() for s in re.split(r'[.?!]+|\n', clean_text) if len(s.strip()) > 2]
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT department FROM employees")
    depts_db = [row[0] for row in cursor.fetchall()]
    conn.close()
    tasks = []
    ai_results = ask_ai_batch(raw_sentences, depts_db)
    if ai_results:
        logger.info(f"Gemini erfolgreich.")
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
        logger.info("Lokaler Fallback.")
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
