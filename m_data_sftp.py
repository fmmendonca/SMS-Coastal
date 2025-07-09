# ###########################################################################
#
# File    : m_data_sftp.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Oct. 19th, 2023.
#
# Updated : Mar. 19th, 2024.
#
# Descrp. : Object to operate files in an SFTP session.
#
# ###########################################################################

from ftplib import FTP
from os import path

import paramiko


class SftpOps:
    def __init__(self) -> None:
        """Performs file operations in an SFTP session."""

        self.pterr = "[ERROR] m_data_sftp:"
        self.ssh = None
        self.sftp = None

    def open(self, serv: str, user: str, pswd: str) -> int:
        """Opens an SFTP session through SSH.
        
        Keyword arguments:
        - serv: host address;
        - user: username;
        - pswd: user password.
        """

        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.ssh.connect(serv, username=user, password=pswd)
        except:
            # Unexpected error.
            print(self.pterr, "SSH connection failed")
            return 1
        
        self.sftp = self.ssh.open_sftp()
        return 0

    def close(self) -> None:
        """Closes the SFTP session."""
        self.sftp.close()
        self.ssh.close()

    def makedir(self, servdir: str) -> int:
        """Creates the directory given in 'servdir'.
        Father path must already exists.
        
        Keyword argument:
        - servdir: path to directory to be created.
        """
        
        status = 0
        fatherdir = path.dirname(servdir)

        # Check if father directory exists:
        try:
            self.sftp.listdir(fatherdir)
        except Exception as err:
            print(err)
            print(self.pterr, "missing directory:")
            print("\t" + fatherdir)
            return 1
        
        # Check if ouput directory exists:
        try:
            self.sftp.listdir(servdir)
        except FileNotFoundError:
            try:
                # Create direcotry:
                self.sftp.mkdir(servdir)
            except Exception as err:
                print(err)
                status = 1
        except Exception as err:
            print(err)
            status = 1

        # Trying to make a dir that already exists will raise OSError,
        # which can be raised due to other causes. Better check if dir
        # exists with listdir.

        if status < 1: return 0
        print(self.pterr, "unable to create directory:")
        print("\t" + servdir)
        return 1
                
    def upfile(self, fipt: str, fout: str) -> int:         
        """Uploads a file to the SFTP session.
        The file is overwritten if already exists.
        
        Keyword arguments:
        - fipt: name and path of the input file (local file);
        - fout: name and path of the output file (server file).
        """

        status = 0
        try:
            self.sftp.put(fipt, fout)
        except Exception as err:
            print(err)
            status = 1

        if status < 1: return 0
        print(self.pterr, "unable to upload file to SFTP:", fout)
        return 1


class FtpOps:
    def __init__(self) -> None:
        """Performs file operations in an FTP session."""

        self.pterr = "[ERROR] m_data_sftp:"
        self.ftp = None

    def open(self, serv: str, user: str, pswd: str) -> None:
        """Opens an FTP session. Don't forget to exit the
        session with self.ftp.quit().
        
        Keyword arguments:
        - serv: host address;
        - user: username;
        - pswd: user password.
        """

        try:
            self.ftp = FTP(serv)
            self.ftp.login(user, pswd)
        except Exception as err:
            print(self.pterr, err)
            return 1
        # DON'T FORGET TO EXIT THE SESSION WITH self.ftp.quit().
        return 0

    def chservdir(self, servdir: str) -> int:
        """Changes the working directory in the FTP server.
        
        Keyword argument:
        - servdir: path to the sirectory in the FTP server.
        """

        try:
            self.ftp.cwd(servdir)
        except Exception as err:
            print(self.pterr, err)
            return 1
        return 0
    
    def getftpfile(self, servfile: str, fout: str) -> int:
        """Downloads a file from the FTP session.
        
        Keyword arguments:
        - servfile: name and path of the file on the server;
        - fout: name and path of the output file.
        """

        status = 0
        dat = open(fout, "wb")
        
        try:
            self.ftp.retrbinary("RETR " + servfile, dat.write)
        except Exception as err:
            print(self.pterr, err)
            status =  1
        
        dat.close()
        return status