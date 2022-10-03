#
# File:    m_public.py
#
# Created: October 4th, 2021
#
# Author:  Fernando Mendonça (fmmendonca@ualg.pt)
#
# Purpose: Read and test inputs from the public section of the initialization
#           file. Also contais common operations between all SMS-Coastal
#           layers.
#
# Updates: Sep. 30th, 2022 - Fernando Mendonca - Module built from
#           m_init_reader.py, which only read the initialization file.
#

import json
import paramiko
import pickle
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from os import getcwd, path, environ
from subprocess import run
from time import time
from typing import Optional, Sequence


class Public:
    def __init__(self) -> None:
        """Class with the methods to run operations in all layers of
        SMS-Coastal. It also reads the inputs of the public section
        inside the initialization file.
        """
        
        # Define public attributes, read from initialization file:
        self.optype = None
        self.opdate = None
        self.email = None
        self.forecast = None
        self.restart = None
        self.levels = None
        self.modtype = None
        
        # The variables in the initialization file has the same name
        # of the attributes above.
        
        # Other attributes:
        self.rston = False
        self.root = getcwd()
        self.base = path.basename(self.root)
        self.timer = time()
        self.log = None

    def rdpublic(self) -> int:
        """Read user inputs from the public section of the
        initialization file.
        """

        # Check initializatioin file and read inputs:
        fname = "init.json"
        if not path.isfile(fname): 
            print("ERROR: initialization file 'init.json' not found.")
            return 1
        inpts = json.load(open(fname, "rb")).get("public", {})
        inpts: dict

        # Every variable must be tested for None value because
        # they can be defined as null in json file.
        
        #
        # Check operation type in 'optype':
        val = inpts.pop("optype", None)
        if val is None:
            print("ERROR: operation type not defined.")
            return 1
        vals = ("forc", "sim", "simr", "data")
        if not isinstance(val, str) or val.lower() not in vals:
            print("ERROR: invalid operation type.")
            return 1
        self.optype = val.lower()

        #
        # Check operation date in 'opdate':
        val = inpts.pop("opdate", None)
        if val is None:
            val = datetime.today().strftime("%Y %m %d")
        elif not isinstance(val, str):
            print("ERROR: invalid type of operation date.")
            return 1
        try:
            val = datetime.strptime(val, "%Y %m %d")
        except ValueError:
            print("ERROR: invalid format of operation date.")
            return 1
        if val.date() > datetime.today().date():
            print("ERROR: operation date can't be greater than today.")
            return 1
        self.opdate = val

        #
        # Get email addresses for report:
        vals = inpts.pop("email", None)
        if vals is None: vals = []
        if not isinstance(vals, list):
            print("ERROR: invalid type of 'email'.")
            return 1
        # Check each value:
        for val in vals:
            if isinstance(val, str): continue
            print("ERROR: invalid type in 'email'.")
            return 1
        self.email = vals

        #
        # Check forecast range in 'forecast':
        val = inpts.pop("forecast", None)
        if val is None: val = 0
        if not isinstance(val, int) or val < 0:
            print("ERROR: invalid forecast range value.")
            return 1
        self.forecast = val

        #
        # Check restart ranges in 'restart':
        vals = inpts.pop("restart", None)
        if vals is None: vals = [0,]
        if not isinstance(vals, list):
            print("ERROR: invalid type of 'restart'.")
            return 1
        for val in vals:
            if isinstance(val, int) and val >= 0: continue
            print("ERROR: invalid type in 'restart'.")
            return 1
        self.restart = vals

        #
        # Check model number of levels in 'levels'.
        # Same as 'forecast' but different error message:
        val = inpts.pop("levels", None)
        if val is None: val = 0
        if not isinstance(val, int) or val < 0:
            print("ERROR: invalid number of model levels.")
            return 1
        self.levels = val

        #
        # Check type of the model in 'modtype':
        val = inpts.pop("modtype", None)
        if val is None:
            val = "1"
        elif val not in ("1", "2"):
            print("ERROR: invalid type of model.")
            return 1
        self.modtype = val

        #
        # Check restart operation in 'rstday':
        vals = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
        val = inpts.pop("rstday", None)
        if self.optype == "simr":
            val = self.opdate.strftime("%a").lower()
        elif val is None:
            val = (self.opdate - timedelta(1)).strftime("%a").lower()
        if val not in vals:
            print("ERROR: invalid value of restart day.")
            return 1
        # Update 'rston' attribute:
        if self.opdate.strftime("%a").lower() == val: self.rston = True

        #
        # Build pickle dictionary and save it:
        inpts = {
            "optype": self.optype, "opdate": self.opdate,
            "email": self.email, "forecast": self.forecast,
            "restart": self.restart, "levels": self.levels,
            "modtype": self.modtype, "rston": self.rston,
            "root": self.root, "base": self.base,
            "timer": self.timer,
        }
        pickle.dump(inpts, open("public.pkl", "wb"))
        return 0

    def ldpublic(self) -> None:
        """Load inputs from public.pkl"""
        inpts = pickle.load(open("public.pkl", "rb"))
        inpts: dict
        self.optype = inpts.pop("optype")
        self.opdate = inpts.pop("opdate")
        self.email = inpts.pop("email")
        self.forecast = inpts.pop("forecast")
        self.restart = inpts.pop("restart")
        self.levels = inpts.pop("levels")
        self.modtype = inpts.pop("modtype")
        self.rston = inpts.pop("rston")
        self.root = inpts.pop("root")
        self.base = inpts.pop("base")
        self.timer = inpts.pop("timer")

    def pstop(self) -> None:
        """Finish program's execution."""
        runtime = round((time() - self.timer)/60, 2)
        print("*"*72)
        print("SYSTEM TIME  :", datetime.today())
        print("ELAPSED TIME :", runtime, "min")
        print("*"*72)
        run("pause", shell=True)
        raise SystemExit

    def sendmail(
            self, subj: str, body: Optional[str]="",
            files: Optional[Sequence]=()) -> None:
        """Send a report e-mail to more than one receiver.
        Sender information is imported from the following
        OS System Variables: smsc_host, smsc_mail, smsc_pass,
        and sms_port. Make sure to add these variables before
        running the code.
        
        Keywords arguments:
        subj -- message subject;
        body -- message text;
        files -- path and name of the menssge attachments.
        """

        # Check receiver address:
        adds = []
        for val in self.email:
            if "@" in val: adds.append(val)
        if len(adds) < 1: return

        # Get sender info from system environment variables:
        sndr = environ.get("smsc_mail")
        pasw = environ.get("smsc_pass")
        host = environ.get("smsc_host")
        port = environ.get("smsc_port")

        # For MS Office 365:
        # smcs_host = smtp.office365.com
        # smsc_port = 587

        if None in (sndr, pasw, host, port):
            print(
                "WARNING: unable to send email reports.",
                "Host is not defined in system environment variables.",
            )
            return
        
        # Check port:
        try:
            port = int(port)
        except ValueError:
            print(
                "WARNING: unable to send email reports.",
                "Host port is not an integer.",
            )
            return

        # Build message:
        msg = EmailMessage()
        msg["Subject"] = subj
        msg["From"] = sndr
        msg["To"] = adds
        msg.set_content(body)
        
        # Attach files:
        for file in files:
            if not path.isfile(file): continue
            data = open(file, "rb").read()
            
            msg.add_attachment(
                data, maintype="application", subtype="octet-stream",
                filename=path.basename(file),
            )

        with smtplib.SMTP(host, port) as smtp:
            # Sey hello to server and login:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(sndr, pasw)

            # Send message:
            try:
                smtp.send_message(msg)
            except Exception as err:
                print(f"WARNING: Failed to send e-mail.\n{err}")

    def logfile(self, entry: str) -> None:
        """"Open and write to the log file.
        
        Keyword argument:
        entry -- text to be written to the log.
        """

        # Check log file:
        if self.log is None:
            print("WARNING: log file is not defined.")
            return

        # Create log file:
        header = "Operation Date;End Time;Status\n"
        if not path.isfile(self.log): open(self.log, "w").write(header)

        # Write text in file:
        open(self.log, "a").write(entry)

    def upsftp(
            self, serv: str, user: str, pwrd: str,
            files: Sequence, servdir: str) -> None:
        """Upload a sequence of files to an SFTP server in a
        designated folder.
        
        Keywords arguments:
        serv -- SFTP server;
        user -- SFTP user;
        pwrd -- SFTP user password;
        files -- files to be uploaded;
        servdir -- directory inside SFTP server to upload the files.
        """
        
        print("Upload files to SFTP")
        print("SERVER:", serv)
        print("OUTPUT DIRECTORY:", servdir)
        
        with paramiko.SSHClient() as client:
            # Set up connection:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(serv, username=user, password=pwrd)

            # Open SFTP:
            sftp = client.open_sftp()

            # Create remote output directory:
            try:
                sftp.mkdir(servdir)
            except OSError:
                print(
                    "WARNING: output directory already exists.",
                    "Data will be overwritten.",
                )

            # Upload files and close connection:
            for file in files:
                print(path.basename(file))
                sftp.put(file, servdir + "/" + path.basename(file))
                # Overwrites files with same name!!

            sftp.close()
