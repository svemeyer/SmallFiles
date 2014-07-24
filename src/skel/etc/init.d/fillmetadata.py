#!/usr/bin/env python

import os
import sys
import time
import signal
from daemon import runner
import ConfigParser as parser
from pymongo import MongoClient, Connection, errors
import logging

running = True

def sigint_handler(signum, frame):
    global running
    logging.info("Caught signal %d." % signum)
    print("Caught signal %d.'" % signum)
    running = False

mongoUri = "mongodb://localhost/"
mongoDb  = "smallfiles"
mountPoint = ""
dataRoot = ""

def dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), ".(%s)(%s)" % (tag, os.path.basename(filepath))), mode='r') as dotfile:
        result = dotfile.readline().strip()
    return result

class MetaDataDaemon():
    
    def __init__(self, configfile = '/etc/dcache/container.conf'):
        self.configfile = configfile
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/var/run/fillmetadata.pid'
        self.pidfile_timeout = 5

    def run():
        global running
        logging.basicConfig(filename='/var/log/dcache/fillmetadata.log',
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

        try:
            while running:
                configuration = parser.RawConfigParser(defaults = { 'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5, 'logLevel': 'ERROR' })
                configuration.read(self.configfile)

                global mountPoint
                global dataRoot
                global mongoUri
                global mongoDb
                mountPoint = configuration.get('DEFAULT', 'mountPoint')
                dataRoot = configuration.get('DEFAULT', 'dataRoot')
                mongoUri = configuration.get('DEFAULT', 'mongoUri')
                mongoDb  = configuration.get('DEFAULT', 'mongodb')
                loopDelay = configuration.getint('DEFAULT', 'loopDelay')
                logLevelStr = configuration.get('DEFAULT', 'logLevel')
                logLevel = getattr(logging, logLevelStr.upper(), None)

                logging.getLogger().setLevel(logLevel)

                logging.info('Successfully read configuration from file %s.' % self.configfile)

                try:
                    client = MongoClient(mongoUri)
                    db = client[mongoDb]

                    with db.files.find( { 'state': { '$exists': False } }, snaphot=True ) as newFilesCursor:
                        logging.info("found %d new files" % (newFilesCursor.count()))
                        for record in newFilesCursor:
                            if not running:
                                sys.exit(1)
                            try:
                                pathof = dotfile(os.path.join(mountPoint, record['pnfsid']), 'pathof')
                                localpath = pathof.replace(dataRoot, mountPoint)
                                stats = os.stat(localpath)

                                record['path'] = pathof
                                record['parent'] = os.path.dirname(pathof)
                                record['size'] = stats.st_size
                                record['ctime'] = stats.st_ctime
                                record['state'] = 'new'

                                newFilesCursor.collection.save(record)
                                logging.debug("Updated record: %s" % str(record))
                            except KeyError as e:
                                logging.warn("KeyError: %s: %s" % (str(record), e.message))
                            except IOError as e:
                                logging.warn("IOError: %s: %s" % (str(record), e.message))
                                logging.info("Removing entry for file %s" % record['pnfsid'])
                                db.files.remove( { 'pnfsid': record['pnfsid'] } )

                except errors.ConnectionFailure as e:
                    logging.warn("Connection failure: %s" % e.message)
                except errors.OperationFailure as e:
                    logging.warn("Could not create cursor: %s" % e.message)

                logging.info("Sleeping for 60 seconds")
                time.sleep(60)

        except parser.NoOptionError as e:
            print("Missing option: %s" % e.message)
            logging.ERROR("Missing option: %s" % e.message)
            sys.exit(2)
        except parser.Error as e:
            print("Error reading configfile %s: %s" % (self.configfile, e.message))
            logging.ERROR("Error reading configfile %s" % (self.configfile, e.message))
            sys.exit(2)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    if not os.getuid() == 0:
        print("pack-files must run as root!")
        sys.exit(2)

    daemon = MetaDataDaemon()
    daemon_runner = runner.DaemonRunner(daemon)
    daemon_runner.do_action()
