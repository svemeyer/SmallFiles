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
from pymongo import MongoClient, errors, ASCENDING, DESCENDING
from pwd import getpwnam
import logging

running = True

def sigint_handler(signum, frame):
    global running
    logging.info("Caught signal %d." % signum)
    print("Caught signal %d." % signum)
    running = False

scriptId = 'pack'
archiveUser = 'root'
archiveMode = '0644'
mongoUri = "mongodb://localhost/"
mongoDb  = "smallfiles"
mountPoint = ""
dataRoot = ""

class Container:

    def __init__(self, targetdir):
        tmpfile = NamedTemporaryFile(suffix = '.darc', dir=targetdir, delete=False)
        self.arcfile = ZipFile(tmpfile.name, mode = 'w', allowZip64 = True)
        global archiveUser
        global archiveMode
        self.archiveUid = getpwnam(archiveUser).pw_uid
        self.archiveMod = int(archiveMode, 8)
        self.size = 0
        self.filecount = 0
        self.logger = logging.getLogger(name = "Container[%s]" % tmpfile.name)

    def close(self):
        self.arcfile.close()
        os.chown(self.arcfile.filename, self.archiveUid, os.getgid())
        os.chmod(self.arcfile.filename, self.archiveMod)

    def add(self, pnfsid, filepath, localpath, size):
        self.arcfile.write(localpath, arcname=pnfsid)
        self.arcfile.comment += "%s:%15d %s\n" % (pnfsid, size, filepath)
        self.size += size
        self.filecount += 1
        self.logger.debug("Added file %s with pnfsid %s" % (filepath, pnfsid))

    def getFilelist(self):
        return self.arcfile.filelist

    def verifyFilelist(self):
        return (len(self.arcfile.filelist) == self.filecount)

    def verifyChecksum(self, chksum):
        self.logger.warn("Checksum verification not implemented, yet")
        return True

class UserInterruptException(Exception):
    def __init__(self, arcfile):
        self.arcfile = arcfile

    def __str__(self):
        return repr(arcfile)

class GroupPackager:

    def __init__(self, path, filePattern, sGroup, storeName, archivePath, archiveSize, minAge, maxAge, verify):
        self.path = path
        self.pathPattern = re.compile(os.path.join(path, filePattern))
        self.sGroup = re.compile(sGroup)
        self.storeName = re.compile(storeName)
        self.archivePath=os.path.join(mountPoint, archivePath)
        if not os.path.exists(self.archivePath):
            os.makedirs(self.archivePath, mode = 0777)
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
            self.logger.critical("Could not find archive file %s, referred to by file entries in database! This needs immediate attention or you will lose data!" % containerChimeraPath)

    def run(self):
        global scriptId
        global running
        now = int(datetime.now().strftime("%s"))
        ctime_threshold = (now - self.minAge*60)
        self.logger.debug("Looking for files matching { path: %s, group: %s, store: %s, ctime: { $lt: %d } }" % (self.pathPattern.pattern, self.sGroup.pattern, self.storeName.pattern, ctime_threshold) )
        with self.db.files.find( { 'state': 'new', 'path': self.pathPattern, 'group': self.sGroup, 'store': self.storeName, 'ctime': { '$lt': ctime_threshold } } ).batch_size(1024) as files:
            files.sort('ctime', ASCENDING)
            sumsize = 0
            old_file_mode = False
            ctime_oldfile_threshold = (now - self.maxAge*60)
            for f in files:
                if f['ctime'] < ctime_oldfile_threshold:
                    old_file_mode = True
                # else:
                #    self.logger.debug("%s needs %d more seconds to become old" % (f['pnfsid'], f['ctime']-ctime_oldfile_threshold))
                sumsize += f['size']

            filecount = files.count()

            self.logger.info("found %d files with a combined size of %d bytes" % (filecount, sumsize))
            if old_file_mode:
                self.logger.debug("containing old files: ctime < %d" % ctime_oldfile_threshold)
            else:
                self.logger.debug("containing no old files: ctime < %d" % ctime_oldfile_threshold)

            if old_file_mode:
                if sumsize < self.archiveSize:
                   self.logger.info("combined size of old files not big enough for a regular archive, packing in old-file-mode")

                else:
                   old_file_mode = False
                   self.logger.info("combined size of old files big enough for regular archive, packing in normal mode")
            elif sumsize < self.archiveSize:
                self.logger.info("no old files found and %d bytes missing to create regular archive of size %d, leaving packager" % (self.archiveSize-sumsize, self.archiveSize))
                return

            files.rewind()
            container = None
            containerChimeraPath = "unset"
            try:
                for f in files:
                    self.logger.debug("Next file %s [%s], remaining %d [%d bytes]" % (f['path'], f['pnfsid'], filecount, sumsize) )
                    if not running:
                        if container:
                            raise UserInterruptException(container.arcfile.filename)
                        else:
                            raise UserInterruptException(None)

                    if container == None:
                        if sumsize >= self.archiveSize or old_file_mode:
                            container = Container(os.path.join(self.archivePath))
                            self.logger.info("Creating new container %s . %d files [%d bytes] remaining." % (os.path.join(self.archivePath, container.arcfile.filename), filecount, sumsize))
                        else:
                            self.logger.info("remaining combined size %d < %d, leaving packager" % (sumsize, self.archiveSize))
                            return

                    if old_file_mode:
                        self.logger.debug("%d bytes remaining for this archive" % sumsize)
                    else:
                        self.logger.debug("%d bytes remaining for this archive" % (self.archiveSize-container.size))

                    try:
                        localfile = f['path'].replace(dataRoot, mountPoint)
                        self.logger.debug("before container.add")
                        container.add(f['pnfsid'], f['path'], localfile, f['size'])
                        self.logger.debug("before collection.save")
                        f['state'] = "added: %s" % container.arcfile.filename.replace(mountPoint, dataRoot)
                        f['lock'] = scriptId
                        files.collection.save(f)
                        self.logger.debug("Added file %s [%s], size: %d" % (f['path'], f['pnfsid'], f['size']))
                    except IOError as e:
                        self.logger.warn("Could not add file %s to archive %s [%s], %s" % (f['path'], f['pnfsid'], container.arcfile.filename, e.message) )
                        self.logger.debug("Removing entry for file %s" % f['pnfsid'])
                        self.db.files.remove( { 'pnfsid': f['pnfsid'] } )
                    except OSError as e:
                        self.logger.warn("Could not add file %s to archive %s [%s], %s" % (f['path'], f['pnfsid'], container.arcfile.filename, e.message) )
                        self.logger.debug("Removing entry for file %s" % f['pnfsid'])
                        self.db.files.remove( { 'pnfsid': f['pnfsid'] } )
                    except errors.OperationFailure as e:
                        self.logger.error("Removing container %s due to OperationalFailure. See below for details." % container.arcfile.filename)
                        container.close()
                        os.remove(container.arcfile.filename)
                        raise e
                    except errors.ConnectionFailure as e:
                        self.logger.error("Removing container %s due to ConnectionFailure. See below for details." % container.arcfile.filename)
                        container.close()
                        os.remove(container.arcfile.filename)
                        raise e

                    sumsize -= f['size']
                    filecount -= 1

                    if container.size >= self.archiveSize:
                        self.logger.debug("Closing full container %s" % container.arcfile.filename)
                        containerChimeraPath = container.arcfile.filename.replace(mountPoint, dataRoot)
                        container.close()

                        if self.verifyContainer(container):
                            self.logger.info("Container %s successfully stored" % container.arcfile.filename)
                            self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'archived: %s' % containerChimeraPath }, '$unset': { 'lock': "" } }, multi = True )
                            self.createArchiveEntry(container)
                        else:
                            self.logger.warn("Removing container %s due to verification error" % container.arcfile.filename)
                            self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi = True )
                            os.remove(container.arcfile.filename)

                        container = None


                if container:
                    if not old_file_mode:
                        self.logger.warn("Removing unful container %s . Maybe a file was deleted during packaging." % container.arcfile.filename)
                        container.close()
                        os.remove(container.arcfile.filename)
                        return

                    self.logger.debug("Closing container %s containing remaining old files", container.arcfile.filename)
                    containerChimeraPath = container.arcfile.filename.replace(mountPoint, dataRoot)
                    container.close()

                    if self.verifyContainer(container):
                        self.logger.info("Container %s with old files successfully stored" % container.arcfile.filename)
                        self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'archived: %s' % containerChimeraPath }, '$unset': { 'lock': "" } }, multi = True )
                        self.createArchiveEntry(container)
                    else:
                        self.logger.warn("Removing container %s with old files due to verification error" % container.arcfile.filename)
                        self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi = True )
                        os.remove(container.arcfile.filename)

            except IOError as e:
                self.logger.error("%s closing file %s . Trying to clean up files in state: 'added'. This might need additional manual fixing!" % (e.strerror, containerChimeraPath))
                self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi = True )
            except errors.OperationFailure as e:
                self.logger.error("Operation Exception in database communication while creating container %s . Please check!" % containerChimeraPath )
                self.logger.error('%s' % e.message)
            except errors.ConnectionFailure as e:
                self.logger.error("Connection Exception in database communication while creating container %s . Please check!" % containerChimeraPath)
                self.logger.error('%s' % e.message)




def dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), ".(%s)(%s)" % (tag, os.path.basename(filepath))), mode='r') as dotfile:
       result = dotfile.readline().strip()
    return result


def main(configfile = '/etc/dcache/container.conf'):
    global running
    logging.basicConfig(filename = '/var/log/dcache/pack-files.log',
                        format='%(asctime)s %(name)-80s %(levelname)-8s %(message)s')

    while running:
        try:
            configuration = parser.RawConfigParser(defaults = { 'scriptId': 'pack', 'archiveUser': 'root', 'archiveMode': '0644', 'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5, 'logLevel': 'ERROR' })
            configuration.read(configfile)

            global scriptId
            global archiveUser
            global archiveMode
            global mountPoint
            global dataRoot
            global mongoUri
            global mongoDb
            scriptId = configuration.get('DEFAULT', 'scriptId')
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

            logging.info('Successfully read configuration from file %s.' % configfile)
            logging.debug('scriptId = %s' % scriptId)
            logging.debug('archiveUser = %s' % archiveUser)
            logging.debug('archiveMode = %s' % archiveMode)
            logging.debug('mountPoint = %s' % mountPoint)
            logging.debug('dataRoot = %s' % dataRoot)
            logging.debug('mongoUri = %s' % mongoUri)
            logging.debug('mongoDb = %s' % mongoDb)
            logging.debug('logLevel = %s' % logLevel)
            logging.debug('loopDelay = %s' % loopDelay)

            try:
                client = MongoClient(mongoUri)
                db = client[mongoDb]
                logging.info("Established db connection")
                
                logging.info("Sanitizing database")
                db.files.update( { 'lock': scriptId }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi = True )

                logging.info("Creating group packagers")
                groups = configuration.sections()
                groupPackagers = []
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
                        packager = GroupPackager(
                            path,
                            filePattern,
                            sGroup,
                            storeName,
                            archivePath,
                            archiveSize,
                            minAge,
                            maxAge,
                            verify)
                        groupPackagers.append(packager)
                        logging.info("Added packager %s for paths matching %s" % (group, (packager.path)))

                logging.info("Running packagers")
                for packager in groupPackagers:
                    packager.run()

            except errors.ConnectionFailure as e:
                logging.error("Connection to DB failed: %s" % e.message)

            logging.info("Sleeping for %d seconds" % loopDelay)
            time.sleep(loopDelay)

        except UserInterruptException as e:
            if e.arcfile:
                logging.info("Cleaning up unfinished container %s." % e.arcfile)
                os.remove(e.arcfile)
                logging.info("Cleaning up modified file entries.")
                containerChimeraPath = e.arcfile.replace(mountPoint, dataRoot)
                db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' } }, multi = True )
            logging.info("Finished cleaning up. Exiting.")
            sys.exit(1)
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
        print("pack-files must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)

