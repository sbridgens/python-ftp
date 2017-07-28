#!/usr/bin/python
'''
Created: 27th July 2015
@author: Simon Bridgens
'''
from datetime import datetime
import ftplib
import argparse
import logging
import uuid
import time
import os
import sys


"""
 CONSTANTS:
 Place any script Constants here.
"""


LOG_DIRECTORY = "/opt/scripts/logs/"


""" END CONSTANTS """


"""
 FtpUploadTracker Class:
 Calculates and reports the progress for the upload process
"""
class FtpUploadTracker:
    sizeWritten = 0
    totalSize = 0
    lastShownPercent = 0

    def __init__(self, totalSize, filename):
        self.totalSize = totalSize
        self.filename = filename

    def ftp_callback(self, block):
        self.sizeWritten += len(block)
        percentComplete = round((float(self.sizeWritten) / float(self.totalSize)) * 100)

        #Print percent to stdout 
        #allows orchestrator to update ui step progress
        outpercent = "\rFile: {0} Upload Progress:{1}".format(self.filename,"%2d%%" % percentComplete)
        sys.stdout.write(outpercent)
        sys.stdout.flush()

        if (self.lastShownPercent != percentComplete):
            self.lastShownPercent = percentComplete


""" END FtpUploadTracker Class """


""" SETUP LOGGING. """

# Generate a UUID for the Job id reference.
job_ID = str(uuid.uuid1())

log_file_name = datetime.now().strftime('ftp_upload_%H_%M_%d_%m_%Y.log')

LOG_FILE = os.path.join(LOG_DIRECTORY, log_file_name)

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)-8s JOB: {0} - %(message)s".format(job_ID),
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=LOG_FILE,
                    filemode='w')

""" END LOGGING """


""" MAIN METHODS """


''' Initalise FTP Connection '''
def initiate_ftp_connection(ftp_host, user, passwd, ftp_dir):
    ftp_session = ftplib.FTP()
    ftp_session.connect(ftp_host, 21)

    #uncomment for debugging.
    #ftp_session.set_debuglevel(2)

    ftp_session.login(user=user, 
                      passwd=passwd)

    #get and log the welcome message
    logging.info(str(ftp_session.getwelcome()))

    #cd to correct remote directory
    ftp_session.cwd(ftp_dir)
    
    return ftp_session



'''
 Upload the required deliverable to Brightcove
 Using the correct session data.
'''
def upload_deliverables(session, file_and_path):

    working_dir = os.path.dirname(file_and_path)

    #strip to remove any newlines
    filename = os.path.basename(file_and_path).strip()

    totalSize = os.path.getsize(file_and_path)

    logging.info("Working directory: {0}".format(str(working_dir)))
    logging.info("Base Filename: {0}".format(str(filename)))
    logging.info("File Total Size: {0}".format(str(totalSize)))

    #instantiate progress tracker for status updates
    uploadTracker = FtpUploadTracker(int(totalSize),filename)

    logging.info("Change Working dir to: {0}".format(str(working_dir)))

    #change dir to working_dir
    os.chdir(working_dir)

    logging.info("Upload of Filename: {0} Started".format(filename))

    '''
     Trigger the ftp upload (storbinary) for the deliverable.
      Args:
       1: FTP KEYWORD and FILE
       2: File IO
       3: Blocksize
       4: Async Callback
    '''
    session.storbinary('STOR ' + filename, 
                        open(filename,'r'), 
                        8192, 
                        uploadTracker.ftp_callback)



''' Parse Args and execute FTP Methods. '''
def main():
    try:
        parser = argparse.ArgumentParser(description="FTP Delivery Script.",
        epilog="USAGE:  pyFTPuploader.py --ftp_user \"someusername\"" + \
        " --ftp_pass \"somepass\"" + \
        " --assets \"asset_n\" \"asset_n\"")

        requiredArgs = parser.add_argument_group('Required FTP Arguments')
        requiredArgs.add_argument('--ftp_host', help="FTP Host", type=str, required=True)
        requiredArgs.add_argument('--ftp_user', help="FTP Username", type=str, required=True)
        requiredArgs.add_argument('--ftp_pass', help="FTP Password", type=str, required=True)
        requiredArgs.add_argument('--ftp_dir', help="FTP Directory", type=str, required=True)
        requiredArgs.add_argument('--asset', help="Asset to Deliver", type=str, required=True)
        args = parser.parse_args()

        # Start initial logging process and output args to log.
        logging.info(" *** NEW FTP REQUEST ACCEPTED *** ")
        logging.info("Arguments passed to pyFTPuploader.py: {0}, {1}, {2}".format(args.ftp_host,
                                                                                      args.ftp_user,                
                                                                                      args.ftp_pass,
                                                                                      args.ftp_dir,
                                                                                      args.asset))


        logging.info("Initiating FTP Connection")
        
        #connect to server
        ftp_session = initiate_ftp_connection(args.ftp_host, 
                                              args.ftp_user, 
                                              args.ftp_pass,
                                              args.ftp_dir)


        logging.info("Connection to FTP Server Successful")

        logging.info("Starting upload of Asset: {0}".format(str(args.asset)))

        #start delivery of asset
        upload_deliverables(ftp_session, args.asset)

        logging.info("FTP Upload of Asset: {0} Completed Successfully".format(str(os.path.basename(args.asset))))

        #quit the ftp session
        ftp_session.quit()
        
        #close any file handles.
        ftp_session.close()

        logging.info("**** COMPLETE: FTP Delivery of asset successful ****")

        exit(0)
                                                                               
    except Exception as ftpEx:
        logging.error("FTP Delivery has encountered an error: DEBUG = {0}".format(str(ftpEx)))
        exit(1)


if __name__ == '__main__':
    main()
    
