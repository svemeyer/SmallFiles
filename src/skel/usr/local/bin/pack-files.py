#!/usr/bin/env python

import os
import sys
import time
from datetime import datetime, timedelta
import hashlib
import signal
import re
import ConfigParser as parser
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from pymongo import MongoClient, Connection

running = True

def sigint_handler(signum, frame):
    global running
    print("Caught signal %d." % signum)
    running = False

mongoUri = "mongodb://localhost/"
mongoDb  = "smallfiles"
mountPoint = ""
dataRoot = ""

class Container:

    def __init__(self, targetdir):
        tmpfile = NamedTemporaryFile(suffix = '.darc', dir=targetdir, delete=False)
        self.arcfile = ZipFile(tmpfile.name, mode = 'w', allowZip64 = True)
        self.size = 0
        self.filecount = 0

    def close(self):
        self.arcfile.close()

    def add(self, pnfsid, filepath, localpath, size):
        self.arcfile.write(localpath, arcname=pnfsid)
        self.arcfile.comment += "%s:%15d %s" % (pnfsid, size, filepath)
        self.size += size
        self.filecount += 1

    def getFilelist(self):
        return self.arcfile.filelist

    def verifyFilelist(self):
        return (len(self.arcfile.filelist) == self.filecount)

    def verifyChecksum(self, chksum):
        print("WARN: Checksum verification not implemented, yet")
        return True


class GroupPackager:

    def __init__(self, path, filePattern, sGroup, storeName, archivePath, archiveSize, minAge, maxAge, verify):
        self.path = path
        self.pathPattern = re.compile(os.path.join(path, filePattern))
        self.sGroup = re.compile(sGroup)
        self.storeName = re.compile("StoreName %s" % storeName)
        self.archivePath=os.path.join(mountPoint, archivePath)
        if not os.path.exists(self.archivePath):
            os.makedirs(self.archivePath, mode = 0770)
        self.archiveSize = int(archiveSize.replace('G','000000000').replace('M','000000').replace('K','000'))
        self.minAge = int(minAge)
        self.maxAge = int(maxAge)
        self.verify = verify
        self.client = MongoClient(mongoUri)
        self.db = self.client[mongoDb]

    def __del__(self):
        pass

    def verifyContainer(self, container):
        verified = False
        if self.verify == 'filelist':
            verified = container.verifyFilelist()
        elif self.verify == 'chksum':
            verified = container.verifyChecksum(0)
        elif self.verify == 'off':
            verified = True
        else:
            print("WARN: Unknown verification method %s. Assuming failure!" % self.verify)
            verified = False

        return verified


    def createArchiveEntry(self, container):
        try:
            containerLocalPath = container.arcfile.filename
            containerChimeraPath = containerLocalPath.replace(mountPoint, dataRoot)
            containerPnfsid = dotfile(containerLocalPath, 'id')

            self.db.archives.insert( { 'pnfsid': containerPnfsid, 'path': containerChimeraPath } )
        except IOError as e:
            print("CRITICAL: Could not find archive file %s, referred to by file entries in database! This needs immediate attention or you will lose data!" % arcPath)


    def run(self):
        global running
        now = int(datetime.now().strftime("%s"))
        ctime_threshold = (now - self.minAge*60)
        with self.db.files.find( { 'archiveUrl': { '$exists': False }, 'path': self.pathPattern, 'group': self.sGroup, 'store': self.storeName, 'ctime': { '$lt': ctime_threshold } }, snapshot=True ) as files:
            print "found %d files" % (files.count())
            container = None
            try:
                for f in files:
                    if not running:
                        break

                    if container == None:
                        container = Container(os.path.join(self.archivePath))
                        print os.path.join(self.archivePath, container.arcfile.filename)

                    try:
                        localfile = f['path'].replace(dataRoot, mountPoint)
                        container.add(f['pnfsid'], f['path'], localfile, f['size'])
                    except OSError as e:
                        print("WARN: Could not add file %s to archive %s, %s" % (f['path'], container.arcfile.filename, e.strerror) )
                        self.db.files.remove( { 'pnfsid': f['pnfsid'] } )

                    if container.size >= self.archiveSize:
                        container.close()

                        if self.verifyContainer(container):
                            self.createArchiveEntry(container)
                        else:
                            os.remove(container.arcfile.filename)

                        container = None
            except OperationFailure as e:
                print('ERROR: %s' % e.strerror)

            # if we have a partly filled container after processing all files, close and delete it.
            if container:
                isOld = False
                ctime_oldfile_threshold = (now - self.maxAge*60)
                for archived in container.getFilelist():
                    if self.db.files.find( { 'pnfsid': archived.filename, 'ctime': { '$lt': ctime_oldfile_threshold } } ).count() > 0:
                        isOld = True

                container.arcfile.close()

                if not isOld:
                    os.remove(container.arcfile.filename)
                else:
                    if self.verifyContainer(container):
                        self.createArchiveEntry(container)
                    else:
                        os.remove(container.arcfile.filename)


def dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), ".(%s)(%s)" % (tag, os.path.basename(filepath))), mode='r') as dotfile:
       result = dotfile.readline().strip()
    return result


def main(configfile = '/etc/dcache/container.conf'):
    global running
    while running:
        try:
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

            print "creating group packagers"
            groups = configuration.sections()
            groupPackagers = {}
            for group in groups:
                print(group)
                filePattern = configuration.get(group, 'fileExpression') 
                sGroup = configuration.get(group, 'sGroup')
                storeName = configuration.get(group, 'storeName')
                archivePath = configuration.get(group, 'archivePath')
                archiveSize = configuration.get(group, 'archiveSize')
                minAge = configuration.get(group, 'minAge')
                maxAge = configuration.get(group, 'maxAge')
                verify = configuration.get(group, 'verify') 
                pathre = re.compile(configuration.get(group, 'pathExpression'))
                print(pathre.pattern)
                paths = db.files.find( { 'parent': pathre } ).distinct( 'parent')
                print(paths)
                pathset = set()
                for path in paths:
                    print(path)
                    pathmatch = re.match("(?P<sfpath>%s)" % pathre.pattern, path).group('sfpath')
                    print(pathmatch)
                    pathset.add(pathmatch)

                for path in pathset:
                    groupPackagers[group] = GroupPackager(
                        path,
                        filePattern,
                        sGroup,
                        storeName,
                        archivePath,
                        archiveSize,
                        minAge,
                        maxAge,
                        verify)
                    print "added packager %s for paths matching %s" % (group, (groupPackagers[group].path))
            print "done"

            print "running packagers..."
            for packager in groupPackagers.values():
                if not running:
                    sys.exit(1)
                packager.run()
            print "done"

            time.sleep(loopDelay)

        except parser.NoOptionError:
            print("Missing option")
        except parser.Error:
            print("Error reading configfile", configfile)
            sys.exit(2)
        except Exception as e:
            print("Unexpected exception: %s" % e.message)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    if not os.getuid() == 0:
        print("pack-files must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)

