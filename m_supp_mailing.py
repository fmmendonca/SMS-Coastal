# ###########################################################################
#
# File    : m_supp_mailing.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : 2018.12.01
#
# Updated : 2025.06.26
#
# Descrp. : Send a report email to more than one recipient with attachments.
#           - initmail: reads email parameters from an input file.
#           - sendmail: receives email paramters as arguments.
#      
#           - input file (initmail.json) setup:
#            {  
#                "mailto": ["somewhere@overtherainbow.com", "elsewhere@nowhere.com"],
#                "mail": "sender@highwaytohell.com",
#                "password": "my_super_secret_password",
#                "host": "smtp.office365.com",
#                "port": 587,
#                "subject": "Subject is not mandatory."
#            }
#
# ###########################################################################

from email.message import EmailMessage
from json import load
from os import path
from smtplib import SMTP
from typing import Optional, Sequence


def initmail(
        subject: Optional[str]="", body: Optional[str]="",
        attachs: Optional[Sequence]=()) -> int:
    """Sends an email to more than one recipient with attachments.
    Email parameters (addresses, host, etc.) are read from the input
    file 'initmail.json'. Returs a status code: 0 for success and 1 for
    failure.
    
    Keyword arguments:
    - subject: message subject;
    - body: message text;
    - attachs: name and path of the files to be attached.
    """
    
    # Read inputs from a json file:
    if not path.isfile("initmail.json"):
        print("[ERROR] m_supp_mailing.initmail: FileNotFoundError")
        print("\tModule initialization file 'initmail.json' not found.")
        return 1
    
    with open("initmail.json", "rb") as dat:
        inpts = load(dat)
        inpts: dict

    # Check inputs:
    adds = inpts.pop("mailto", [])
    mail = inpts.pop("mail", "")
    pswd = inpts.pop("password", "")
    host = inpts.pop("host", "")
    port = inpts.pop("port", 0)
    subject = inpts.pop("subject", "") + subject

    errtxt = "[ERROR] m_supp_mailing.initmail: ValueError"

    if not isinstance(adds, list) or len(adds) < 1:
        print(errtxt)
        print("\tMissing/invalid recipient(s) addresses.")
        return 1
    if not isinstance(mail, str) or mail=="":
        print(errtxt)
        print("\tMissing/invalid sender address.")
        return 1
    if not isinstance(pswd, str) or pswd=="":
        print(errtxt)
        print("\tMissing/invalid sender password.")
        return 1
    if not isinstance(host, str) or host=="":
        print(errtxt)
        print("\tMissing/invalid hostname.")
        return 1
    if not isinstance(port, int) or port < 1:
        print(errtxt)
        print("\tMissing/invalid host port.")
        return 1

    return sendmail(adds, mail, pswd, host, port, subject, body, attachs)


def sendmail(
        adds: Sequence, mail: str, pswd: str, host: str, port: int,
        subject: str, body: Optional[str]="",
        attachs: Optional[Sequence]=()) -> int:
    """Sends an email to more than one recipient with attachments.
    Returs a status code: 0 for success and 1 for failure.
    
    Keyword arguments:
    - adds: list with recipients addresses;
    - mail: sender address;
    - pswd: sender password;
    - host: email service host;
    - port: email service port;
    - subject: message subject;
    - body: message text;
    - attachs: name and path of the files to be attached.
    """

    # Build message:
    print("Sending an email...", end=" ")
    
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail
    msg["To"] = adds
    msg.set_content(body)
    
    # Attach files (skips empty sequences):
    for val in attachs:
        if not path.isfile(val): continue
        
        with open(val, "rb") as attach:
            data = attach.read()
        
        msg.add_attachment(
            data, maintype="application",
            subtype="octet-stream",
            filename=path.basename(val),
        )

    # Send mail:
    smtp = SMTP(host, port)

    try:
        # Sey hello to server and login:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()  # do it again!
        smtp.login(mail, pswd)

        smtp.send_message(msg)
        print("[SENT]")
        status =  0
    except Exception as err:
        print("[FAILED]")
        print(err)
        status =  1

    smtp.close()
    return status
