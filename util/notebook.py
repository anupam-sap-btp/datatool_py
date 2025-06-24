import os
from dotenv import load_dotenv
import requests
import json
import smtplib
from email.message import EmailMessage


load_dotenv()

databricks_instance = os.getenv("DATABRICKS_INSTANCE")
databricks_token = os.getenv("DATABRICKS_TOKEN")
cluster_id = os.getenv("CLUSTER_ID")
webhook_id = os.getenv("WEBHOOK_ID")
email_address = os.getenv("EMAIL_ADDRESS")
email_password = os.getenv("EMAIL_PASSWORD")



def run_notebook_job( job_id: int, job_step_next: tuple ):
    # Create a job that will be run immediately (one-time)
    run_name = f"Execute One-Time Job {job_id}"
    one_time_job_config = {
    "run_name": run_name,
    "existing_cluster_id": cluster_id,
    "notebook_task": {
        "notebook_path": job_step_next[10],
        "base_parameters": {
            "config_file": "Project_Config.xlsx",
            "mapping_file": "Project_Mapping.xlsx",
            "data_file": "Project_Data.xlsx",
            "job_id": job_id,
            "step_folder": f'Step-{job_step_next[0]}-{job_step_next[1]}-{job_step_next[3]}'
        }
    },
    "webhook_notifications": {
        "on_start": [
            {
                "id": webhook_id
            }
        ],
        "on_failure": [
            {
                "id": webhook_id
            }
        ],
        "on_success": [
            {
                "id": webhook_id
            }
        ]
    }
}

        
    # Run the job immediately
    run_response = requests.post(
        f"{databricks_instance}/api/2.2/jobs/runs/submit",
        headers={"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"},
        json=one_time_job_config
    )
    
    if run_response.status_code == 200:
        run_id = run_response.json()["run_id"]
        print(f"Job run started with ID: {run_id}")
        return run_id
    else:
        print(f"Error running job: {run_response.text}")


def send_email(data: str):
    # Email content
    subject = "Step Completion Notification"
    body = data
    sender_email = email_address
    receiver_email = "anupam.duttaroy@innovervglobal.com"
    password = email_password  

    # Create email message
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg.set_content(body)

    # Send email via Gmail SMTP
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender_email, password)
        smtp.send_message(msg)

    print("Email sent successfully!")
