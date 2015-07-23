#!/usr/bin/env python

import os
import sys
import signal
import time
import logging
from datetime import datetime
import re
import ConfigParser as Parser
import uuid
from zipfile import ZipFile
from pymongo import MongoClient, errors, ASCENDING
from pwd import getpwnam
from dcap import Dcap

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
dcapUrl = ""

class Container:

    def __init__(self, localtargetdir, dcap):
        self.filename = str(uuid.uuid1())
        self.localfilepath = os.path.join(localtargetdir, self.filename)
        pnfstargetdir = localtargetdir.replace(mountPoint, dataRoot, 1)
        self.pnfsfilepath = os.path.join(pnfstargetdir, self.filename)

        self.logger = logging.getLogger(name="Container[%s]" % self.pnfsfilepath)
        self.logger.debug("Initializing")

        self.dcaparc = dcap.open_file(self.pnfsfilepath, 'w')
        self.arcfile = ZipFile(self.dcaparc, mode='w', allowZip64=True)
        global archiveUser
        global archiveMode
        self.archiveUid = getpwnam(archiveUser).pw_uid
        self.archiveMod = int(archiveMode, 8)
        self.size = 0
        self.filecount = 0

    def close(self):
        self.logger.debug("Closing")
        self.arcfile.close()
        self.dcaparc.close()
        os.chown(self.localfilepath, self.archiveUid, os.getgid())
        os.chmod(self.localfilepath, self.archiveMod)

    def add(self, pnfsid, filepath, localpath, size):
        self.arcfile.write(localpath, arcname=pnfsid)
        self.size += size
        self.filecount += 1
        self.logger.debug("Added file %s with pnfsid %s" % (filepath, pnfsid))

    def getFilelist(self):
        return self.arcfile.filelist

    def verifyFilelist(self):
        return len(self.arcfile.filelist) == self.filecount

    def verifyChecksum(self, chksum):
        self.logger.warn("Checksum verification not implemented, yet")
        return True

class UserInterruptException(Exception):
    def __init__(self, arcfile):
        self.arcfile = arcfile

    def __str__(self):
        return repr(self.arcfile)

class GroupPackager:

    def __init__(self, path, filePattern, sGroup, storeName, archivePath, archiveSize, minAge, maxAge, verify):
        self.path = path
        self.pathPattern = re.compile(os.path.join(path, filePattern))
        self.sGroup = re.compile(sGroup)
        self.storeName = re.compile(storeName)
        self.archivePath = os.path.join(mountPoint, archivePath)
        if not os.path.exists(self.archivePath):
            os.makedirs(self.archivePath, mode=0777)
            os.chmod(self.archivePath, 0777)
        self.archiveSize = int(archiveSize.replace('G', '000000000').replace('M', '000000').replace('K', '000'))
        self.minAge = int(minAge)
        self.maxAge = int(maxAge)
        self.verify = verify
        self.client = MongoClient(mongoUri)
        self.db = self.client[mongoDb]
        self.logger = logging.getLogger(name="GroupPackager[%s]" % self.pathPattern.pattern)

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
            containerLocalPath = container.localfilepath
            containerChimeraPath = container.pnfsfilepath
            containerPnfsid = dotfile(containerLocalPath, 'id')

            self.db.archives.insert( { 'pnfsid': containerPnfsid, 'path': containerChimeraPath } )
        except IOError as e:
            self.logger.critical("Could not find archive file %s, referred to by file entries in database! This needs immediate attention or you will lose data!" % containerChimeraPath)

    def writeStatus(self, arcfile, currentSize, nextFile):
        with open("/var/log/dcache/pack-files-%s.status", 'w') as statusFile:
            statusFile.write("Container: %s\n" % arcfile)
            statusFile.write("Size: %d/%d\n" % ( currentSize, self.archiveSize ))
            statusFile.write("Next: %s\n" % nextFile)

    def run(self):
        global scriptId
        global running
        global dcapUrl
        dcap = Dcap(dcapUrl)
        try:
            now = int(datetime.now().strftime("%s"))
            ctime_threshold = (now - self.minAge*60)
            self.logger.debug("Looking for files matching { path: %s, group: %s, store: %s, ctime: { $lt: %d } }" % (self.pathPattern.pattern, self.sGroup.pattern, self.storeName.pattern, ctime_threshold) )
            with self.db.files.find( { 'state': 'new', 'path': self.pathPattern, 'group': self.sGroup, 'store': self.storeName, 'ctime': { '$lt': ctime_threshold } } ).batch_size(512) as cursor:
                cursor.sort('ctime', ASCENDING)
                sumsize = 0
                old_file_mode = False
                ctime_oldfile_threshold = (now - self.maxAge*60)
                for f in cursor:
                    if f['ctime'] < ctime_oldfile_threshold:
                        old_file_mode = True
                    sumsize += f['size']

                filecount = cursor.count()

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

                cursor.rewind()
                container = None
                containerChimeraPath = None
                try:
                    for f in cursor:
                        if filecount <= 0 or sumsize <= 0:
                            self.logger.info("Actual number of files exceeds precalculated number, will collect new files in next run.")
                            break

                        self.logger.debug("Next file %s [%s], remaining %d [%d bytes]" % (f['path'], f['pnfsid'], filecount, sumsize) )
                        if not running:
                            if container:
                                raise UserInterruptException(container.localfilepath)
                            else:
                                raise UserInterruptException(None)

                        if container is None:
                            if sumsize >= self.archiveSize or old_file_mode:
                                container = Container(self.archivePath, dcap)
                                self.logger.info("Creating new container %s . %d files [%d bytes] remaining." % (container.pnfsfilepath, filecount, sumsize))
                            else:
                                self.logger.info("remaining combined size %d < %d, leaving packager" % (sumsize, self.archiveSize))
                                return

                        if old_file_mode:
                            self.logger.debug("%d bytes remaining for this archive" % sumsize)
                            self.writeStatus(container.arcfile, sumsize, "%s [%s]" % ( f['path'], f['pnfsid'] ))
                        else:
                            self.logger.debug("%d bytes remaining for this archive" % (self.archiveSize-container.size))
                            self.writeStatus(container.arcfile, self.archiveSize-container.size, "%s [%s]" % ( f['path'], f['pnfsid'] ))

                        try:
                            localfile = f['path'].replace(dataRoot, mountPoint,1)
                            self.logger.debug("before container.add(%s[%s], %s)" % (f['path'], f['pnfsid'], f['size']))
                            container.add(f['pnfsid'], f['path'], localfile, f['size'])
                            self.logger.debug("before collection.save")
                            f['state'] = "added: %s" % container.pnfsfilepath
                            f['lock'] = scriptId
                            cursor.collection.save(f)
                            self.logger.debug("Added file %s [%s]" % (f['path'], f['pnfsid']))
                        except IOError as e:
                            self.logger.exception("IOError while adding file %s to archive %s [%s], %s" % (f['path'], container.pnfsfilepath, f['pnfsid'], e.message))
                            self.logger.debug("Removing entry for file %s" % f['pnfsid'])
                            self.db.files.remove( { 'pnfsid': f['pnfsid'] } )
                        except OSError as e:
                            self.logger.exception("OSError while adding file %s to archive %s [%s], %s" % (f['path'], f['pnfsid'], container.pnfsfilepath, e.message))
                            self.logger.debug("Removing entry for file %s" % f['pnfsid'])
                            self.db.files.remove( { 'pnfsid': f['pnfsid'] } )
                        except errors.OperationFailure as e:
                            self.logger.error("Removing container %s due to OperationalFailure. See below for details." % container.localfilepath)
                            container.close()
                            os.remove(container.localfilepath)
                            raise e
                        except errors.ConnectionFailure as e:
                            self.logger.error("Removing container %s due to ConnectionFailure. See below for details." % container.localfilepath)
                            container.close()
                            os.remove(container.localfilepath)
                            raise e

                        sumsize -= f['size']
                        filecount -= 1

                        if container.size >= self.archiveSize:
                            self.logger.debug("Closing full container %s" % container.pnfsfilepath)
                            containerChimeraPath = container.pnfsfilepath
                            container.close()

                            if self.verifyContainer(container):
                                self.logger.info("Container %s successfully stored" % container.pnfsfilepath)
                                self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'archived: %s' % containerChimeraPath }, '$unset': { 'lock': "" } }, multi=True )
                                self.createArchiveEntry(container)
                            else:
                                self.logger.warn("Removing container %s due to verification error" % container.localfilepath)
                                self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi=True )
                                os.remove(container.localfilepath)

                            container = None

                    if container:
                        if not old_file_mode:
                            self.logger.warn("Removing unful container %s . Maybe a file was deleted during packaging." % container.localfilepath)
                            container.close()
                            os.remove(container.localfilepath)
                            return

                        self.logger.debug("Closing container %s containing remaining old files", container.pnfsfilepath)
                        containerChimeraPath = container.pnfsfilepath
                        container.close()

                        if self.verifyContainer(container):
                            self.logger.info("Container %s with old files successfully stored" % container.pnfsfilepath)
                            self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'archived: %s' % containerChimeraPath }, '$unset': { 'lock': "" } }, multi=True )
                            self.createArchiveEntry(container)
                        else:
                            self.logger.warn("Removing container %s with old files due to verification error" % container.localfilepath)
                            self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi=True )
                            os.remove(container.localfilepath)

                except IOError as e:
                    self.logger.error("%s closing file %s. Trying to clean up files in state: 'added'. This might need additional manual fixing!" % (e.strerror, containerChimeraPath))
                    self.db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi=True )
                except errors.OperationFailure as e:
                    self.logger.error("Operation Exception in database communication while creating container %s . Please check!" % containerChimeraPath )
                    self.logger.error('%s' % e.message)
                except errors.ConnectionFailure as e:
                    self.logger.error("Connection Exception in database communication. Removing incomplete container %s ." % containerChimeraPath)
                    self.logger.error('%s' % e.message)

        finally:
            dcap.close()

def dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), ".(%s)(%s)" % (tag, os.path.basename(filepath))), mode='r') as dotfile:
       result = dotfile.readline().strip()
    return result


def main(configfile = '/etc/dcache/container.conf'):
    global running

    while running:
        try:
            configuration = Parser.RawConfigParser(defaults = { 'scriptId': 'pack', 'archiveUser': 'root', 'archiveMode': '0644', 'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5, 'logLevel': 'ERROR' })
            configuration.read(configfile)

            global scriptId
            global archiveUser
            global archiveMode
            global mountPoint
            global dataRoot
            global mongoUri
            global mongoDb
            global dcapUrl
            scriptId = configuration.get('DEFAULT', 'scriptId')
            
            logging.basicConfig(filename = '/var/log/dcache/pack-files-%s.log' % scriptId,
                        format='%(asctime)s %(name)-80s %(levelname)-8s %(message)s')

            archiveUser = configuration.get('DEFAULT', 'archiveUser')
            archiveMode = configuration.get('DEFAULT', 'archiveMode')
            mountPoint = configuration.get('DEFAULT', 'mountPoint')
            dataRoot = configuration.get('DEFAULT', 'dataRoot')
            mongoUri = configuration.get('DEFAULT', 'mongoUri')
            mongoDb  = configuration.get('DEFAULT', 'mongodb')
            dcapUrl = configuration.get('DEFAULT', 'dcapUrl')
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
            logging.debug('dcapUrl = %s' % dcapUrl)
            logging.debug('logLevel = %s' % logLevel)
            logging.debug('loopDelay = %s' % loopDelay)

            try:
                client = MongoClient(mongoUri)
                db = client[mongoDb]
                logging.info("Established db connection")
                
                logging.info("Sanitizing database")
                db.files.update( { 'lock': scriptId }, { '$set': { 'state': 'new' }, '$unset': { 'lock': "" } }, multi=True )

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
                    paths = db.files.find( { 'parent': pathre } ).distinct('parent')
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

                client.close()

            except errors.ConnectionFailure as e:
                logging.error("Connection to DB failed: %s" % e.message)

            logging.info("Sleeping for %d seconds" % loopDelay)
            time.sleep(loopDelay)

        except UserInterruptException as e:
            if e.arcfile:
                logging.info("Cleaning up unfinished container %s." % e.arcfile)
                os.remove(e.arcfile)
                logging.info("Cleaning up modified file entries.")
                containerChimeraPath = e.arcfile.replace(mountPoint, dataRoot, 1)
                db.files.update( { 'state': 'added: %s' % containerChimeraPath }, { '$set': { 'state': 'new' } }, multi=True )

            logging.info("Finished cleaning up. Exiting.")
            sys.exit(1)
        except Parser.NoOptionError as e:
            print("Missing option: %s" % e.message)
            logging.error("Missing option: %s" % e.message)
        except Parser.Error as e:
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

