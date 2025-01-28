import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import sqlite3
import time  # Import time for delay

# Constants
BATCH_SIZE = 3  # Emails to send per batch
DATABASE_URL = "email_state.db"  # SQLite database for storing state

# Environment variables for sensitive information
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")  # Set in Railway environment
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Set in Railway environment

def initialize_database():
    """Initialize the database to store state."""
    conn = sqlite3.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_row INTEGER NOT NULL,
            last_run_date TEXT NOT NULL
        )
    """)
    # Insert initial state if the table is empty
    cursor.execute("SELECT COUNT(*) FROM email_state")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO email_state (last_row, last_run_date) VALUES (0, ?)", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
    conn.commit()
    conn.close()

def get_state():
    """Retrieve the last processed state from the database."""
    conn = sqlite3.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT last_row, last_run_date FROM email_state ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    conn.close()
    return result if result else (0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def update_state(last_row, last_run_date):
    """Update the state in the database."""
    conn = sqlite3.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO email_state (last_row, last_run_date) VALUES (?, ?)", (last_row, last_run_date.strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

def send_emails(email_list):
    """Send emails to a list of recipients."""
    try:
        EMAIL_ADDRESS = "universitypoornima08@gmail.com"
        EMAIL_PASSWORD = "ilbd nmjo uefe wokz"
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

        from_ = EMAIL_ADDRESS
        for recipient in email_list:
            msg = MIMEMultipart()
            msg['From'] = from_
            msg['To'] = recipient
            msg['Subject'] = "Testing Email from Sikander"

            # HTML content
            html = f'''
            <html>
            <head></head>
            <body>
                <h1>Hello {recipient.split("@")[0].capitalize()},</h1>
                <p>This email is sent by Sikander as a test.</p>
            </body>
            </html>
            '''
            text = MIMEText(html, "html")
            msg.attach(text)

            server.sendmail(from_, recipient, msg.as_string())
            print(f"Email sent to {recipient}")

    except smtplib.SMTPException as e:
        print(f"SMTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        try:
            server.quit()
        except Exception:
            pass

def main():
    while True:
        # Initialize database if not already set up
        initialize_database()

        # Load email data from Excel
        data = pd.read_excel("data.xlsx")
        email_addresses = list(data["email"].dropna())  # Ensure no NaN values

        # Get the last state
        last_row, last_run_date = get_state()

        # Parse last_run_date without fractional seconds
        last_run_date = datetime.strptime(last_run_date, "%Y-%m-%d %H:%M:%S")

        # Process the next batch of emails
        start_index = last_row
        end_index = min(last_row + BATCH_SIZE, len(email_addresses))
        batch = email_addresses[start_index:end_index]

        if batch:
            print(f"Sending emails to rows {start_index + 1} to {end_index}...")
            send_emails(batch)

            # Update the state after sending emails
            update_state(end_index, datetime.now())
            print("Batch completed. State updated.")

            # Calculate remaining time for 15 hours delay
            remaining_time = timedelta(seconds=54000)  # 15 hours
            print(f"Waiting for 15 hours before sending the next batch...")

            # Countdown until the next batch is ready to send
            for remaining in range(remaining_time.seconds, 0, -60):  # Countdown in minutes
                remaining_hours = remaining // 3600
                remaining_minutes = (remaining % 3600) // 60
                print(f"Remaining time: {remaining_hours} hours, {remaining_minutes} minutes")
                time.sleep(60)  # Wait for 1 minute before updating the countdown

            # Wait for the full 24 hours (86000 seconds)
            time.sleep(86400)  # Freeze for 24 hours

        else:
            # All emails have been sent, restart from the first row
            print("All emails have been sent. Restarting from the first row...")
            update_state(0, datetime.now())  # Reset to the first row

if __name__ == "__main__":
    main()
