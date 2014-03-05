#!/usr/bin/env/python

import os
import sys
import time
import signal
from zipfile import ZipFile
from pymongo import MongoClient, Connection
import ConfigParser as parser

def sigint_handler(signum, frame):
    print("Caught signal %d." % signum)
    sys.exit(1)


def dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), ".(%s)(%s)" % (tag, os.path.basename(filepath))), mode='r') as dotfile:
       result = dotfile.readline().strip()
    return result


def main(configfile = '/etc/dcache/container.conf'):
    try:
        while True:
            print "reading configuration"
            configuration = parser.RawConfigParser(defaults = { 'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5 })
            configuration.read(configfile)

            global mountPoint
            global dataRoot
            global mongoUri
            global mongoDb
            mountPoint = configuration.get('DEFAULT', 'mountPoint')
            dataRoot = configuration.get('DEFAULT', 'dataRoot')
            mongoUri = configuration.get('DEFAULT', 'mongoUri')
            mongoDb  = configuration.get('DEFAULT', 'mongodb')
            loopDelay = configuration.getint('DEFAULT', 'loopDelay')
            print "done"

            print "establishing db connection"
            client = MongoClient(mongoUri)
            db = client[mongoDb]
            print "done"

            with db.archives.find() as archives:
                for archive in archives:
                    try:
                        localpath = archive['path'].replace(dataRoot, mountPoint)
                        archivePnfsid = dotfile(localpath, 'id')
                        zf = ZipFile(localpath, mode='r', allowZip64 = True)
                        for f in zf.filelist:
                            print("find_one( { pnfsid: %s } )" % f.filename)
                            filerecord = db.files.find_one( { 'pnfsid': f.filename } )
                            print(filerecord)
                            if filerecord:
                                url = "dcache://dcache/?store=%s&group=%s&bfid=%s:%s" % (filerecord['store'], filerecord['group'], f.filename, archivePnfsid)
                                filerecord['archiveUrl'] = url
                                db.files.save(filerecord)
                            else:
                                print("WARN: File %s in archive %s has no entry in DB. Assuming it was deleted on disk." % (f.filename, localpath) )
                    except Exception as e:
                        print("ERROR: %s" % e.message)
                        # db.archives.remove( { 'id': archive['pnfsid'] } )
                    
                    db.archives.remove( { 'pnfsid': archive['pnfsid'] } )


            time.sleep(60)

    except parser.NoOptionError:
        print("Missing option")
    except parser.Error:
        print("Error reading configfile", configfile)
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
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)

