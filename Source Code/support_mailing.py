# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2018-12-01
#
# Updated : 2021-10-07


from os import path
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def mailreport(email_send, reportsubject, body, filename):
    if not email_send or email_send == '':
        return
    mails = email_send.split()
    
    print("Sending e-mail report...")
    for addrs in mails:
        if "@" not in addrs:
            return
        print(addrs)
        
        email_user = 'email_address@somewhere.com'
        email_password = 'my_password'
        
        try:
            server = smtplib.SMTP('smtp.office365.com', 587)
            # gmail server = smtplib.SMTP('smtp.gmail.com',587)
    
            msg = MIMEMultipart()
            msg['From'] = email_user
            msg['To'] = addrs
            msg['Subject'] = reportsubject
    
            msg.attach(MIMEText(body, 'plain'))
    
            for file in filename:
                if not path.isfile(file):
                    filename = ()
    
            for file in filename:
                attachment = open(file, 'rb')
    
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 
                                "attachment; filename= " + file)
                msg.attach(part)
    
            text = msg.as_string()
    
            server.starttls()
            server.login(email_user, email_password)
    
            server.sendmail(email_user, addrs, text)
            server.quit()
    
        except Exception as err:
            print(f'Failed to send e-mail: {err}')
