#!/usr/bin/env python

import os
import sys
import time
import signal
from zipfile import ZipFile
from pymongo import MongoClient, Connection
import ConfigParser as parser
import logging

running = True

def sigint_handler(signum, frame):
    global running
    print("Caught signal %d." % signum)
    running = False

def main(configfile = '/etc/dcache/container.conf'):
    global running
    logging.basicConfig(filename = '/var/log/dcache/writebfids.log',
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    try:
        while running:
            configuration = parser.RawConfigParser(defaults = { 'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5, 'logLevel': 'ERROR' })
            configuration.read(configfile)

            global mountPoint
            global dataRoot
            global mongoUri
            global mongoDb
            mountPoint = configuration.get('DEFAULT', 'mountPoint')
            dataRoot = configuration.get('DEFAULT', 'dataRoot')
            mongoUri = configuration.get('DEFAULT', 'mongoUri')
            mongoDb  = configuration.get('DEFAULT', 'mongodb')
            logLevelStr = configuration.get('DEFAULT', 'logLevel')
            logLevel = getattr(logging, logLevelStr.upper(), None)

            loopDelay = configuration.getint('DEFAULT', 'loopDelay')

            logging.getLogger().setLevel(logLevel)

            logging.info('Successfully read configuration from file %s.' % configfile)

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
                            filerecord = db.files.find_one( { 'pnfsid': f.filename }, await_data=True )
                            if filerecord:
                                url = "dcache://dcache/?store=%s&group=%s&bfid=%s:%s" % (filerecord['store'], filerecord['group'], f.filename, archivePnfsid)
                                filerecord['archiveUrl'] = url
                                db.files.save(filerecord)
                                logging.debug("Updated record with URL %s in archive %s" % (url,archive['path']))
                            else:
                                logging.warn("File %s in archive %s has no entry in DB. Will retry later." % (f.filename, archive['path']) )
                                db.failures.insert( { 'archiveId': archivePnfsid, 'pnfsid': f.filename } )

                    except Exception as e:
                        logging.error("Unexpected error: %s" % e.message)
                        # db.archives.remove( { 'id': archive['pnfsid'] } )
                    
                    db.archives.remove( { 'pnfsid': archive['pnfsid'] } )

            time.sleep(60)

    except parser.NoOptionError as e:
        print("Missing option: %s" % e.message)
        logging.error("Missing option: %s" % e.message)
    except parser.Error as e:
        print("Error reading configfile %s: %s" % (configfile, e.message))
        logging.error("Error reading configfile %s: %s" % (configfile, e.message))
        sys.exit(2)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    if not os.getuid() == 0:
        print("writebfsids.py must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: writebfids.py <configfile>")
        sys.exit(2)

