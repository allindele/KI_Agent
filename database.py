import sqlite3
import os
from config import logger, SENDER_EMAIL

def get_db_connection():
    db_path = os.path.expanduser("~/techcorp.db")
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
    employees.append(("Mathias Lennart", "mervicellium@gmail.com", "IT Security", "Head"))
    employees.append(("Franck Effa", "loiceffa9@gmail.com", "IT Security", "Staff"))
    employees.append(("Florian Wirtz", "tervicellium@gmail.com", "IT Security", "Staff"))
    employees.append(("Dr. Green", "franckloiceffaawoulbe@gmail.com", "Renewable Energy", "Head"))
    employees.append(("Derick Tage", "pervicellium@gmail.com", "Renewable Energy", "Staff"))
    employees.append(("Kevin Opa", "effaawoulbefranckloic@gmail.com", "Renewable Energy", "Staff"))
    employees.append(("Lars Fischer", "mervicellium@gmail.com", "Elektrotechnik", "Head"))
    employees.append(("Hans Kabel", SENDER_EMAIL, "Elektrotechnik", "Staff"))
    employees.append(("General Manager", "gamescomlg2024@gmail.com", "General Management", "Head"))

    cursor.executemany('INSERT INTO employees VALUES (?, ?, ?, ?)', employees)
    conn.commit()
    conn.close()
    logger.info("Datenbank initialisiert.")

def clear_tasks_table():
    try:
        conn = get_db_connection()
        conn.execute("DELETE FROM tasks")
        conn.execute("DELETE FROM sqlite_sequence WHERE name='tasks'")
        conn.commit()
        conn.close()
        logger.info("Datenbank bereinigt.")
    except Exception as e:
        logger.error(f"Fehler beim Bereinigen der Datenbank: {e}")
