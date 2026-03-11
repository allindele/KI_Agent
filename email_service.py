import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import logger, SENDER_EMAIL, SENDER_PASSWORD

def send_email_smtp(to_email, subject, body, is_html=False):
    logger.info(f"Vorbereitung E-Mail an {to_email}...")
    msg = MIMEMultipart()
    msg['From'] = f"TechCorp AI <{SENDER_EMAIL}>"
    msg['To'] = to_email
    msg['Subject'] = subject
    type = 'html' if is_html else 'plain'
    msg.attach(MIMEText(body, type))
    try:
        logger.info(f"Verbinde zu smtp.gmail.com:587...")
        with smtplib.SMTP('smtp.gmail.com', 587, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, to_email, msg.as_string())
        logger.info(f"Mail erfolgreich gesendet an {to_email}")
        return True
    except socket.timeout:
        logger.error(f"ZEITÜBERSCHREITUNG bei {to_email}.")
        return False
    except Exception as e:
        logger.error(f"Fehler bei {to_email}: {e}")
        return False
