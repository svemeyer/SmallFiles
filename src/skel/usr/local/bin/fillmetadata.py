#!/usr/bin/env python

import os
import sys
import time
import signal
import ConfigParser as parser
from pymongo import MongoClient, Connection

running = True

def sigint_handler(signum, frame):
    global running
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

def main(configfile = '/etc/dcache/container.conf'):
    global running
    try:
        while running:
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

            try:
                client = MongoClient(mongoUri)
                db = client[mongoDb]
            except ConnectionFailure as e:
                print("Connection failure: %s" % e.strerror)

            with db.files.find( { 'path': None }, snaphot=True ) as newFilesCursor:
                print "found %d new files" % (newFilesCursor.count())
                for record in newFilesCursor:
                    if not running:
                        sys.exit(1)
                    try:
                        pathof = dotfile(os.path.join(mountPoint, record['pnfsid']), 'pathof')
                        localpath = pathof.replace(dataRoot, mountPoint)
                        stats = os.stat(localpath)

                        record['path'] = pathof
                        record['size'] = stats.st_size
                        record['ctime'] = stats.st_ctime

                        newFilesCursor.collection.save(record)
                    except KeyError as e:
                        print "KeyError: " + str(record) + ":" + e.message
                    except IOError as e:
                        print "IOError: " + str(record) + ":" + e.strerror
                        db.files.remove( { 'pnfsid': record['pnfsid'] } )

            time.sleep(60)

    except parser.NoOptionError:
        print("Missing option")
    except parser.Error:
        print("Error reading configfile", configfile)
        sys.exit(2)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    if not os.getuid() == 0:
        print("fillmetadata.py must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)

