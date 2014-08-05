#!/usr/bin/env python

import os
import sys
import time
from daemon import runner
import errno
import signal
from zipfile import ZipFile, BadZipfile
from pymongo import MongoClient, errors
import ConfigParser as parser
import logging

running = True

def sigint_handler(signum, frame):
    global running
    print("Caught signal %d." % signum)
    running = False

class BfidDaemon():

    def __init__(self, configfile = '/etc/dcache/container.conf'):
        self.configfile = configfile
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/var/run/mydaemon.pid'
        self.pidfile_timeout = 5

    def run():
        global running
        logging.basicConfig(filename = '/var/log/dcache/writebfids.log',
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

        try:
            while running:
                configuration = parser.RawConfigParser(defaults = { 'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5, 'logLevel': 'ERROR' })
                configuration.read(self.configfile)

                global archiveUser
                global archiveMode
                global mountPoint
                global dataRoot
                global mongoUri
                global mongoDb
                archiveUser = configuration.get('DEFAULT', 'archiveUser')
                archiveMode = configuration.get('DEFAULT', 'archiveMode')
                mountPoint = configuration.get('DEFAULT', 'mountPoint')
                dataRoot = configuration.get('DEFAULT', 'dataRoot')
                mongoUri = configuration.get('DEFAULT', 'mongoUri')
                mongoDb  = configuration.get('DEFAULT', 'mongodb')
                logLevelStr = configuration.get('DEFAULT', 'logLevel')
                logLevel = getattr(logging, logLevelStr.upper(), None)

                loopDelay = configuration.getint('DEFAULT', 'loopDelay')

                logging.getLogger().setLevel(logLevel)

                logging.info('Successfully read configuration from file %s.' % self.configfile)

                try:
                    client = MongoClient(mongoUri)
                    db = client[mongoDb]
                    logging.info("Established db connection")

                    with db.archives.find() as archives:
                        for archive in archives:
                            if not running:
                                logging.info("Exiting.")
                                sys.exit(1)
                            try:
                                localpath = archive['path'].replace(dataRoot, mountPoint)
                                archivePnfsid = archive['pnfsid']
                                zf = ZipFile(localpath, mode='r', allowZip64 = True)
                                for f in zf.filelist:
                                    logging.debug("Entering bfid into record for file %s" % f.filename)
                                    filerecord = db.files.find_one( { 'pnfsid': f.filename, 'state': 'archived: %s' % archive['path'] }, await_data=True )
                                    if filerecord:
                                        url = "dcache://dcache/?store=%s&group=%s&bfid=%s:%s" % (filerecord['store'], filerecord['group'], f.filename, archivePnfsid)
                                        filerecord['archiveUrl'] = url
                                        filerecord['state'] = 'verified: %s' % archive['path']
                                        db.files.save(filerecord)
                                        logging.debug("Updated record with URL %s in archive %s" % (url,archive['path']))
                                    else:
                                        logging.warn("File %s in archive %s has no entry in DB. This could be caused by a previous forced interrupt. Creating failure entry." % (f.filename, archive['path']) )
                                        db.failures.insert( { 'archiveId': archivePnfsid, 'pnfsid': f.filename } )

                                logging.debug("stat(%s): %s" % (localpath, os.stat(localpath)))

                                db.archives.remove( { 'pnfsid': archive['pnfsid'] } )
                                logging.debug("Removed entry for archive %s[%s]" % ( archive['path'], archive['pnfsid'] ) )

                                pnfsname = os.path.join(os.path.dirname(localpath), archive['pnfsid'])
                                logging.debug("Renaming archive %s to %s" % (localpath, pnfsname) )
                                os.rename(localpath, pnfsname)

                            except BadZipfile as e:
                                logging.warn("Archive %s is not yet ready. Will try later." % localpath)

                            except IOError as e:
                                if e.errno != errno.EINTR:
                                    logging.error("IOError: %s" % e.strerror)
                                else:
                                    logging.info("User interrupt.")


                except errors.ConnectionFailure as e:
                    logging.warn("Connection to DB failed: %s" % e.message)
                except errors.OperationFailure as e:
                    logging.warn("Could not create cursor: %s" % e.message)
                except Exception as e:
                    logging.error("Unexpected error: %s" % e.message)

                logging.info("Processed all archive entries. Sleeping 60 seconds.")
                time.sleep(60)

        except parser.NoOptionError as e:
            print("Missing option: %s" % e.message)
            logging.error("Missing option: %s" % e.message)
        except parser.Error as e:
            print("Error reading configfile %s: %s" % (self.configfile, e.message))
            logging.error("Error reading configfile %s: %s" % (self.configfile, e.message))
            sys.exit(2)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    if not os.getuid() == 0:
        print("writebfsids.py must run as root!")
        sys.exit(2)

    daemon = BfidDaemon()
    daemon_runner = runner.DaemonRunner(daemon)
    daemon_runner.do_action()

