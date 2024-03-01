import boto3
import json
from jinja2 import Environment, FileSystemLoader
import os

region = os.environ.get('Region')
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

env = os.environ.get('Environment')


def lambda_handler(event, context):
    print("Received Event: \n", str(event))
    message = event['Records'][0]['Sns']['Message']
    message = json.loads(message)
    sns_data = message['data']
    secret_name = f"smtp-filecentral-{env}"
    smtp_credentials = get_smtp_credentials(secret_name)
    sender_email = os.environ.get('Sender_id')
    if sns_data['record_type'] == 'JOB_ONBOARD_EMAIL_NOTIFICATION':
        print("Preparing Job onboard email notification : ", sns_data.get('job_name', ''))
        email_body = prepare_template(sns_data, 'job_onboard.html')
        print("email body checking", email_body)
        recipient_email = sns_data['email_recipient_list']
        subject = f"{sns_data.get('job_name', '')} Job setup and configuration successful"
        send_email_with_custom_template(sender_email, recipient_email, subject, email_body, smtp_credentials)
    elif sns_data['record_type'] == 'JOB_STATUS_EMAIL_NOTIFICATION':
        email_body = prepare_template(sns_data, 'job_status.html')
        print("Preparing Job status change email notification")
        recipient_email = sns_data['email_recipient_list']
        subject = f"{sns_data.get('job_name', '')} Job status changed !"
        send_email_with_custom_template(sender_email, recipient_email, subject, email_body, smtp_credentials)
    elif sns_data['record_type'] == 'JIRA_NOTIFICATION':
        print("ignoring JIRA notification record : " + sns_data)
    else:
        print("Probable errors in job : ", sns_data)
        return
        # data = {
        #     "name": "file_ops",
        #     "job_id": sns_data["job_id"],
        #     "job_run_id": sns_data['job_run_id'],
        #     "log_level": "warn",
        #     "error_code_id": json.dumps(sns_data['error_code_id'])
        # }
        # if sns_data['mailing_list'] is not None:
        #     data = json.dumps(data)
        #     send_email(source=sender_cc_email, recipients=sns_data['mailing_list'],
        #                template_name="file_ops_email_template", template_data=data)


def send_email_with_custom_template(sender_email, recipient_email, subject, email_body, smtp_credentials):
    try:
        # ses = boto3.client('ses', region_name=region)
        print(f"Sender : {sender_email} , Recipient : {recipient_email} , Subject : {subject}")
        # Parse SMTP credentials from the retrieved secret
        smtp_host = smtp_credentials['smtp_host']
        smtp_port = smtp_credentials['smtp_port']
        smtp_user = smtp_credentials['smtp_user']
        smtp_password = smtp_credentials['smtp_password']

        # Create MIMEText object to represent the email body
        message = MIMEMultipart()
        # message = MIMEText(email_body)
        message.attach(MIMEText(email_body, 'html'))
        message['From'] = sender_email
        message['To'] = ", ".join(recipient_email)
        message['Subject'] = subject

        print(message.as_string(), "message from mimetext")

        # Connect to the SMTP server
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()  # Secure the connection

        # Login to the SMTP server
        server.login(smtp_user, smtp_password)

        # Send the email
        response = server.sendmail(sender_email, recipient_email,
                                   message.as_string())

        # Close the connection
        server.quit()

        print("Email sent! ", response)

    except Exception as e:
        print("Sending Email failed due to Exception : \n", e)


def get_smtp_credentials(secret_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')
    # Retrieve the secret value
    response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in response:
        secret = response['SecretString']
        return json.loads(secret)
    else:
        raise ValueError("Secret value is not in string format")


def prepare_template(sns_data, html_template):
    # Create a Jinja2 environment

    env1 = Environment(loader=FileSystemLoader('./'))
    template = env1.get_template(html_template)

    # Render the template with user data

    rendered_template = template.render(sns_data=sns_data)
    return rendered_template
