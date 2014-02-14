#!/usr/bin/env python

import os
import sys
from datetime import datetime, timedelta
import signal
import re
import ConfigParser as parser
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from pymongo import MongoClient, Connection

mongoUri = "mongodb://localhost/"
mongoDb  = "smallfiles"
mountPoint = ""
dataRoot = ""

class Container:

    def __init__(self, targetdir):
        tmpfile = NamedTemporaryFile(suffix = '.darc', dir=targetdir, delete=False)
        self.arcfile = ZipFile(tmpfile.name, mode = 'w', allowZip64 = True)
        self.size = 0

    def close(self):
        self.arcfile.close()

    def add(self, pnfsid, filepath, localpath, size):
        self.arcfile.write(localpath, arcname=pnfsid)
        self.arcfile.comment += "%s:%15d %s" % (pnfsid, size, filepath)
        self.size += size

    def getFilelist(self):
        return self.arcfile.filelist


class GroupPackager:

    def __init__(self, pathExpression, archivePath, archiveSize, minAge, maxAge, verify):
        self.pathExpression = re.compile(pathExpression)
        self.archivePath=os.path.join(mountPoint, archivePath)
        self.archiveSize = int(archiveSize.replace('G','000000000').replace('M','000000').replace('K','000'))
        self.minAge = int(minAge)
        self.maxAge = int(maxAge)
        self.verify = verify
        self.client = MongoClient(mongoUri)
        self.db = self.client[mongoDb]

    def __del__(self):
        pass

    def run(self):
        now = int(datetime.now().strftime("%s"))
        with self.db.files.find( { 'archivePath': { '$exists': False }, 'path': self.pathExpression }, snapshot=True, exhaust=True) as files: #  , 'ctime': { '$lt': now-self.minAge*60 } } ) as files:
            print "found %d files" % (files.count())
            container = None
            for f in files:
                if container == None:
                    container = Container(os.path.join(self.archivePath))
                    print os.path.join(self.archivePath, container.arcfile.filename)

                container.add(f['pnfsid'], f['path'], os.path.join(f['parent'], f['filename']), f['size'])
                if container.size >= self.archiveSize:
                    container.close()

                    for archived in container.getFilelist():
                        self.db.files.update( { 'pnfsid': archived.filename },
                            { '$set' :
                                { 'archivePath': container.arcfile.filename } } )

                    container = None

            if container:
                container.arcfile.close()
                os.remove(container.arcfile.filename)


def dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), ".(%s)(%s)" % (tag, os.path.basename(filepath))), mode='r') as dotfile:
       result = dotfile.readline().strip()
    return result


def main(configfile = '/etc/dcache/container.conf'):
    try:
        print "reading configuration"
        configuration = parser.RawConfigParser(defaults = { 'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles' })
        configuration.read(configfile)

        global mountPoint
        global dataRoot
        global mongoUri
        global mongoDb
        mountPoint = configuration.get('DEFAULT', 'mountPoint')
        dataRoot = configuration.get('DEFAULT', 'dataRoot')
        mongoUri = configuration.get('DEFAULT', 'mongoUri')
        mongoDb  = configuration.get('DEFAULT', 'mongodb')
        print "done"

        print "establishing db connection"
        client = MongoClient(mongoUri)
        db = client[mongoDb]
        print "done"

        print "creating group packagers"
        groups = configuration.sections()
        groupPackagers = {}
        for group in groups:
            groupPackagers[group] = GroupPackager(
                configuration.get(group, 'pathExpression'),
                configuration.get(group, 'archivePath'),
                configuration.get(group, 'archiveSize'),
                configuration.get(group, 'minAge'),
                configuration.get(group, 'maxAge'),
                configuration.get(group, 'verify') )
            print "added packager %s for paths matching %s" % (group, (groupPackagers[group].pathExpression.pattern))
        print "done"

    #    while True:
        print "getting new files"
        with db.files.find( { 'path': None }, snaphot=True, exhaust=True) as newFilesCursor:
            print "found %d new files" % (newFilesCursor.count())
            for record in newFilesCursor:
                try:
                    pathof = dotfile(os.path.join(mountPoint, record['pnfsid']), 'pathof')
                    localpath = re.sub(dataRoot, mountPoint, pathof)
                    parentpath = os.path.dirname(localpath)
                    filename = os.path.basename(localpath)
                    stats = os.stat(localpath)

                    record['path'] = pathof
                    record['parent'] = parentpath
                    record['filename'] = filename
                    record['size'] = stats.st_size
                    record['ctime'] = stats.st_ctime

                    newFilesCursor.collection.save(record)
                except KeyError as e:
                    print "KeyError: " + str(record) + ":" + e.message
                except IOError as e:
                    print "IOError: " + str(record) + ":" + e.strerror
                    db.files.remove( { 'pnfsid': record['pnfsid'] } )
        print "done"

        print "running packagers..."
        for packager in groupPackagers.values():
            packager.run()
        print "done"

        print "writing urls into archived file records"
        archives = {}
        for arcPath in db.files.distinct( 'archivePath' ):
            archives[arcPath] = dotfile(arcPath, 'id')

        with db.files.find( { 'archivePath': { '$exists': True }, 'archiveUrl': { '$exists': False } }, snapshot=True, exhaust=True ) as archivedFilesCursor:
            for record in archivedFilesCursor:
                record['archiveUrl'] = "dcache://dcache/?store=%s&group=%s&bfid=%s:%s" % (record['store'],record['group'],record['pnfsid'],archives[record['archivePath']])

                archivedFilesCursor.collection.save(record)
        print "done"

    except parser.NoOptionError:
        print("Missing option")
    except parser.Error:
        print("Error reading configfile", configfile)
        sys.exit(2)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)

