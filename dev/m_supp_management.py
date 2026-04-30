# ###########################################################################
#
# File    : m_supp_management.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : 2025.10.05
#
# Updated : 2025.10.05
#
# Descrp. : Contains the support class for dealing with user inputs and
#           running common tasks of SMS-Coastal operations, such as log
#           data and reporting.
#
# ###########################################################################

from datetime import date, datetime, timedelta
from email.message import EmailMessage
from os import makedirs, path
from smtplib import SMTP
from typing import Any, Optional, Sequence


class SmscManager:
    def __init__(self, prms: dict):
        """Management and control class for an SMS-Coastal operation.
        Takes all user input in a dictionary, which correspond to
        the data of a single operation defined in the SMS-Coastal
        initialization file: 'initsmsc.yml'.
        
        Keyword argument:
        - prms: dictionary containing user inputs.
        """

        self.prms = prms .copy()                     # inputs/parameters
        self.rootdir = self.prms.pop("ROOTDIR", "")  # operation root dir
        self.logfile = None                          # log file name and path
        self.status = 0                              # error status
        self.mailing = {}                            # mailing parameters

        # Auxiliary attrs.:
        self.txt = "[ERROR] " + __name__ + ":\n\t"
        self.stop = "\n\tSMS-Coastal STOPPED."

    def logentry(self, txt: str) -> None:
        """Writes an entry to the SMS-Coastal log for the current
        operation. The method always adds the date and time of the
        record and the newline character at the end of the input text.
        The current operation must already be initialized with the 
        'initialize_operation' method for the log file to be defined.
        
        Keyword argument:
        - txt: text to be written in the log.
        """

        with open(self.logfile, "a") as dat:
            dat.write(f"[{datetime.today().isoformat()}] ")
            dat.write(txt + "\n")

    def sendreport(
            self, subject: Optional[str]="",
            attachs: Optional[Sequence]=()) -> None:
        """Sends an email to more than one recipient with attachments.
        
        Keyword arguments:
        - subject: message subject;
        - attachs: name and path of the files to be attached.
        """

        if not self.mailing.get("ACTIVE", False): return
        
        # Build message:
        #
        print("Mailing report...", end=" ")
        self.logentry("Mailing report.")  # Good for debugging.
        
        msg = EmailMessage()
        msg["Subject"] = self.mailing.get("SUBJECT", "") + subject
        msg["From"]    = self.mailing.get("SENDER", "")
        msg["To"]      = self.mailing.get("RECEIVERS", "")
        msg.set_content("Sent by Fernando's awesome Python code.")
        
        # Attach files (skips empty sequences):
        #
        for val in attachs:
            if not path.isfile(val): continue
            
            with open(val, "rb") as attach:
                data = attach.read()
            
            msg.add_attachment(
                data, maintype="application",
                subtype="octet-stream",
                filename=path.basename(val),
            )

        # Send the message:
        #
        smtp = SMTP(
            self.mailing.get("HOST", ""),
            self.mailing.get("PORT", 587),
        )

        try:
            # Sey hello to server and login:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()  # do it again!
            smtp.login(
                self.mailing.get("SENDER", ""),
                self.mailing.get("PASSWORD", ""),
            )

            smtp.send_message(msg)
            print("[SENT]")
            self.logentry("Report sent by email.")
        except Exception as err:
            print("[FAILED]")
            self.logentry("Failed to mail the report.")
            print(err)

        smtp.close()

    def initialize_operation(self) -> None:
        """Initializes a single SMS-Coastal operation. Verifies, from
        the inputs saved in 'self.prms', the operation's settings
        for root directory, date, and email reporting parameters.
        """

        # Check root directory:
        #
        if not path.isdir(self.rootdir):
            print(f"[ERROR]", __name__ + ":")
            print(f"\tMissing root directory '{self.rootdir}'.")
            self.status += 1
            return
        
        self.rootdir = path.abspath(self.rootdir)
        
        # Set log file:
        #
        self.logfile = path.join(
            self.rootdir, "logs", date.today().isoformat() + ".dat",
        )
        makedirs(path.dirname(self.logfile), exist_ok=True)
        self.logentry("SMS-Coastal STARTED.")
        self.logentry("Checking user inputs.")
        
        # Check operation date:
        #
        val = self.prms.pop("OPDATE", "").lower()

        # The date is imported as a string and
        # stored again as a datetime.date object:
        if val == "today":
            self.prms["OPDATE"] = date.today()
        elif not val:
            txt = self.txt + "'OPDATE' operation date is not defined."
            print(txt)
            self.logentry(txt.replace("\n\t", " ") + self.stop)
            self.status += 1
            return
        else:
            try:
                val = datetime.fromisoformat(val)
                self.prms["OPDATE"] = val.date()
            except ValueError:
                txt = "Invalid format of operation date in 'OPDATE'."
                txt = self.txt + txt
                print(txt)
                self.logentry(txt.replace("\n\t", " ") + self.stop)
                self.status += 1
                return
            
        # The following method is separated for better organization:
        self.setopdates()
        if self.status > 0: return
            
        # Check email reporting parameters:
        #
        mail = self.prms.pop("MAILING", {"ACTIVE": False})

        if not mail.get("ACTIVE", False):
            # mailing disabled.
            self.mailing = mail
            return
        
        # Test strings:
        keys = ("SUBJECT", "SENDER", "PASSWORD", "HOST")
        vals = [mail.get(key) for key in keys]
        self.testlist(vals, str)

        # Test integer:
        if not isinstance(mail.get("PORT"), int):
            self.status += 1

        # Test receivers:
        self.testlist(mail.get("RECEIVERS"), str)
        
        if self.status > 0:
            txt = self.txt + "Invalid 'MAILIING' value(s)."
            print(txt)
            self.logentry(txt.replace("\n\t", " ") + self.stop)
            self.status += 1
            return
        
        self.mailing = mail
    
    def setopdates(self) -> None:
        """Defines the operation dates based on the 'OPDATE'
        and 'DTDAYS' parameters. This method should be used
        after testing 'OPDATE' with 'initialize_operation'.
        """
        
        # Check 'DTDAYS' parameter:
        vals = self.prms.pop("DTDAYS", [])

        if not vals:
            txt = self.txt + "'DTDAYS' parameter is not defined."
            print(txt)
            self.logentry(txt.replace("\n\t", " ") + self.stop)
            self.status += 1
            return
        elif not isinstance(vals, list):
            vals = [vals,]  # force to be a list.

        self.testlist(vals, int)
        
        if self.status > 0:
            txt = self.txt + "Invalid 'DTDAYS' value(s)."
            print(txt)
            self.logentry(txt.replace("\n\t", " ") + self.stop)
            return
        
        # Create initial/end days range for single 'DTDAYS':
        if len(vals) == 1 and vals[0] >= 0:
            vals = [0, vals[0]]
        elif len(vals) == 1 and vals[0] < 0:
            vals = [vals[0], 0]

        # Define the dates assuming 'OPDATE' is alreay in date format:
        opdate = self.prms.get("OPDATE")

        if not isinstance(opdate, date):
            txt = self.txt + "'OPDATE' operation date is not "
            txt+= "defined or with invalid type."
            print(txt)
            self.logentry(txt.replace("\n\t", " ") + self.stop)
            self.status += 1
            return

        opdates = [opdate + timedelta(val) for val in vals]
        self.prms["INI"] = opdates[:-1]
        self.prms["FIN"] = opdates[1:]

        txt = " ".join([val.isoformat() for val in opdates[:-1]])
        print("START DATES : " + txt)
        self.logentry("START DATES : " + txt)
        txt = " ".join([val.isoformat() for val in opdates[1:]])
        print("END DATES   : " + txt)
        self.logentry("END DATES   : " + txt)

    def testlist(
            self, vals: list, ptype: Sequence[Any],
            lsize: Optional[int]=0) ->  None:
        """Tests a parameter read from the initialization file to determine
        if it is of the list type and also the type of its elements.
        
        Keyword arguments:
        - vals: value of the parameter read from the initialization file;
        - ptype: list elements type.
        - lsize: Number of elements the list must have (0 for any number).
        """

        if not isinstance(vals, list):
            self.status += 1
            return
        
        for val in vals:
            if isinstance(val, ptype): continue
            self.status += 1
        
        if lsize > 0 and len(vals) != lsize:
            self.status += 1

    def forcgrid(self) -> None:
        """Tests the parameter that defines the grid limits
        in operations with boundary condition data.
        """

        self.testlist(self.prms.get("GRID"), (int, float), 4)
        if self.status < 1: return

        txt = self.txt + "'GRID' parameter is not defined "
        txt+= "or with invalid type(s)."
        print(txt)
        self.logentry(txt.replace("\n\t", " ") + self.stop)
        self.status += 1
    
    def forccred(self) -> None:
        """Tests the parameter that defines the credentials
        for downloading boundary condition data.
        """

        self.testlist(self.prms.get("CREDENTIALS"), str, 2)
        if self.status < 1: return

        txt = self.txt + "'CREDENTIALS' parameter is not "
        txt+= "defined or with invalid type(s)."
        print(txt)
        self.logentry(txt.replace("\n\t", " ") + self.stop)
        self.status += 1


# if __name__ == "__main__":
#     # Test case.
#     inpts = {
#         "MAILING": {
#             "ACTIVE": False,
#             "SUBJECT": "SOMA CMEMS ",
#             "RECEIVERS": ["someone@somewhere.com",],
#             "SENDER": "master@ofpuppets.com",
#             "PASSWORD": "nooneknows",
#             "HOST": "smtp.office365.com",
#             "PORT": 587,
#         },

#         "ROOTDIR": "D:/smsc/cmems",
#         "OPDATE": "today",
#         # "OPDATE": "2025-11-16",
#         "DTDAYS": 2,

#         "GRID": [35.5, 40, -12, -5],
#     }
    
#     smsc = SmscManage(inpts)
#     smsc.initialize_operation()
#     smsc.sendreport("Test")
#     smsc.forcgrid()
#     #smsc.forccred()
#     smsc.logentry("SMS-Coastal COMPLETED.\n")
    