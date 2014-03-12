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
import logging

running = True

def sigint_handler(signum, frame):
    global running
    logging.info("Caught signal %d." % signum)
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
        self.logger = logging.getLogger(name = "Container[%s]" % tmpfile)

    def close(self):
        self.arcfile.close()

    def add(self, pnfsid, filepath, localpath, size):
        self.arcfile.write(localpath, arcname=pnfsid)
        self.arcfile.comment += "%s:%15d %s\n" % (pnfsid, size, filepath)
        self.size += size
        self.filecount += 1

    def getFilelist(self):
        return self.arcfile.filelist

    def verifyFilelist(self):
        return (len(self.arcfile.filelist) == self.filecount)

    def verifyChecksum(self, chksum):
        self.logger.warn("Checksum verification not implemented, yet")
        return True


class GroupPackager:

    def __init__(self, path, filePattern, sGroup, storeName, archivePath, archiveSize, minAge, maxAge, verify):
        self.path = path
        self.pathPattern = re.compile(os.path.join(path, filePattern))
        self.sGroup = re.compile(sGroup)
        self.storeName = re.compile(storeName)
        self.archivePath=os.path.join(mountPoint, archivePath)
        if not os.path.exists(self.archivePath):
            os.makedirs(self.archivePath, mode = 0770)
        self.archiveSize = int(archiveSize.replace('G','000000000').replace('M','000000').replace('K','000'))
        self.minAge = int(minAge)
        self.maxAge = int(maxAge)
        self.verify = verify
        self.client = MongoClient(mongoUri)
        self.db = self.client[mongoDb]
        self.logger = logging.getLogger(name = "GroupPackager[%s]" % self.pathPattern.pattern)

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
            self.logger.warn("Unknown verification method %s. Assuming failure!" % self.verify)
            verified = False

        return verified


    def createArchiveEntry(self, container):
        try:
            containerLocalPath = container.arcfile.filename
            containerChimeraPath = containerLocalPath.replace(mountPoint, dataRoot)
            containerPnfsid = dotfile(containerLocalPath, 'id')

            self.db.archives.insert( { 'pnfsid': containerPnfsid, 'path': containerChimeraPath } )
        except IOError as e:
            self.logger.critical("Could not find archive file %s, referred to by file entries in database! This needs immediate attention or you will lose data!" % arcPath)


    def run(self):
        global running
        now = int(datetime.now().strftime("%s"))
        ctime_threshold = (now - self.minAge*60)
        self.logger.debug("Looking for files matching { path: %s, group: %s, store: %s, ctime: { $lt: %d } }" % (self.pathPattern.pattern, self.sGroup.pattern, self.storeName.pattern, ctime_threshold) )
        with self.db.files.find( { 'archiveUrl': { '$exists': False }, 'path': self.pathPattern, 'group': self.sGroup, 'store': self.storeName, 'ctime': { '$lt': ctime_threshold } }, snapshot=True ) as files:
            self.logger.info("found %d files" % files.count())
            container = None
            try:
                for f in files:
                    if not running:
                        break

                    if container == None:
                        container = Container(os.path.join(self.archivePath))
                        self.logger.info("Creating new container %s" % os.path.join(self.archivePath, container.arcfile.filename))

                    try:
                        localfile = f['path'].replace(dataRoot, mountPoint)
                        container.add(f['pnfsid'], f['path'], localfile, f['size'])
                    except OSError as e:
                        self.logger.warn("Could not add file %s to archive %s, %s" % (f['path'], container.arcfile.filename, e.strerror) )
                        self.logger.debug("Removing entry for file %s" % f['pnfsid'])
                        self.db.files.remove( { 'pnfsid': f['pnfsid'] } )

                    if container.size >= self.archiveSize:
                        container.close()

                        if self.verifyContainer(container):
                            self.logger.info("Container %s successfully stored" % container.arcfile.filename)
                            self.createArchiveEntry(container)
                        else:
                            self.logger.warn("Removing container %s due to verification error" % container.arcfile.filename)
                            os.remove(container.arcfile.filename)

                        container = None
            except OperationFailure as e:
                self.logger.error('%s' % e.strerror)

            # if we have a partly filled container after processing all files, close and delete it.
            if container:
                isOld = False
                ctime_oldfile_threshold = (now - self.maxAge*60)
                self.logger.debug("Checking container %s for old files (ctime < %d)" % (container.arcfile.filename,ctime_oldfile_threshold))
                for archived in container.getFilelist():
                    if self.db.files.find( { 'pnfsid': archived.filename, 'ctime': { '$lt': ctime_oldfile_threshold } } ).count() > 0:
                        isOld = True

                container.arcfile.close()

                if not isOld:
                    self.logger.info("Removing unfull container %s" % container.arcfile.filename)
                    os.remove(container.arcfile.filename)
                else:
                    if self.verifyContainer(container):
                        self.logger.info("Container %s with old files successfully stored" % container.arcfile.filename)
                        self.createArchiveEntry(container)
                    else:
                        self.logger.warn("Removing container %s with old files due to verification error" % container.arcfile.filename)
                        os.remove(container.arcfile.filename)


def dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), ".(%s)(%s)" % (tag, os.path.basename(filepath))), mode='r') as dotfile:
       result = dotfile.readline().strip()
    return result


def main(configfile = '/etc/dcache/container.conf'):
    global running
    logging.basicConfig(filename = '/var/log/dcache/pack-files.log')

    while running:
        try:
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
            logging.debug('mountPoint = %s' % mountPoint)
            logging.debug('dataRoot = %s' % dataRoot)
            logging.debug('mongoUri = %s' % mongoUri)
            logging.debug('mongoDb = %s' % mongoDb)
            logging.debug('logLevel = %s' % logLevel)
            logging.debug('loopDelay = %s' % loopDelay)

            client = MongoClient(mongoUri)
            db = client[mongoDb]
            logging.info("Established db connection")

            logging.info("Creating group packagers")
            groups = configuration.sections()
            groupPackagers = {}
            for group in groups:
                logging.debug("Group: %s" % group)
                filePattern = configuration.get(group, 'fileExpression') 
                logging.debug("filePattern: %s" % filePattern)
                sGroup = configuration.get(group, 'sGroup')
                logging.debug("sGroup: %s" % sGroup)
                storeName = configuration.get(group, 'storeName')
                logging.debug("storeName: %s" % storeName)
                archivePath = configuration.get(group, 'archivePath')
                logging.debug("archivePath: %s" % archivePath)
                archiveSize = configuration.get(group, 'archiveSize')
                logging.debug("archiveSize: %s" % archiveSize)
                minAge = configuration.get(group, 'minAge')
                logging.debug("minAge: %s" % minAge)
                maxAge = configuration.get(group, 'maxAge')
                logging.debug("maxAge: %s" % maxAge)
                verify = configuration.get(group, 'verify') 
                logging.debug("verify: %s" % verify)
                pathre = re.compile(configuration.get(group, 'pathExpression'))
                logging.debug("pathExpression: %s" % pathre.pattern)
                paths = db.files.find( { 'parent': pathre } ).distinct( 'parent')
                pathset = set()
                for path in paths:
                    pathmatch = re.match("(?P<sfpath>%s)" % pathre.pattern, path).group('sfpath')
                    pathset.add(pathmatch)

                logging.debug("Creating a packager for each path in: %s" % pathset)
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
                    logging.info("Added packager %s for paths matching %s" % (group, (groupPackagers[group].path)))

            logging.info("Running packagers")
            for packager in groupPackagers.values():
                if not running:
                    sys.exit(1)
                packager.run()
            logging.info("all packagers finished. Sleeping for %d seconds" % loopDelay)

            time.sleep(loopDelay)

        except parser.NoOptionError as e:
            print("Missing option: %s" % e.strerror)
            logging.error("Missing option: %s" % e.strerror)
        except parser.Error as e:
            print("Error reading configfile %s: %s" % (configfile, e.strerror))
            logging.error("Error reading configfile %s: %s" % (configfile, e.message))
            sys.exit(2)

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

