#!/usr/bin/env python3
# coding=utf-8

import os
import sys
import signal
import time
import logging
import logging.handlers
from datetime import datetime
import re
import configparser as parser
import uuid
from zipfile import ZipFile
from pymongo import MongoClient, errors, ASCENDING
from pwd import getpwnam
from dcap import Dcap

running = True


def sigint_handler(signum, frame):
    global running
    logging.info(f"Caught signal {signum}.")
    print(f"Caught signal {signum}.")
    running = False
    raise InterruptedError


script_id = 'pack'
archive_user = 'root'
archive_mode = '0644'
mongo_uri = "mongodb://localhost/"
mongo_db = "smallfiles"
mount_point = ""
data_root = ""
dcap_url = ""


class Container:

    def __init__(self, localtargetdir, dcap):
        self.filename = str(uuid.uuid1())
        self.localfilepath = os.path.join(localtargetdir, self.filename)
        pnfstargetdir = localtargetdir.replace(mount_point, data_root, 1)
        self.pnfsfilepath = os.path.join(pnfstargetdir, self.filename)

        self.logger = logging.getLogger(name=f"Container[{self.pnfsfilepath}]")
        self.logger.debug("Initializing")

        self.dcaparc = dcap.open_file(self.pnfsfilepath, 'w')
        self.arcfile = ZipFile(self.dcaparc, "w")
        global archive_user
        global archive_mode
        self.archiveUid = getpwnam(archive_user).pw_uid
        self.archiveMod = int(archive_mode, 8)
        self.size = 0
        self.filecount = 0

    def close(self):
        self.logger.debug("Closing")
        try:
            self.arcfile.close()
            self.dcaparc.close()
            os.chown(self.localfilepath, self.archiveUid, os.getgid())
            os.chmod(self.localfilepath, self.archiveMod)
        except InterruptedError:
            self.logger.error("Caught interuption signal. Cancelled closing.")
            raise InterruptedError

    def add(self, pnfsid, filepath, localpath, size):
        self.arcfile.write(localpath, arcname=pnfsid)
        self.size += size
        self.filecount += 1
        self.logger.debug(f"Added file {filepath} with pnfsid {pnfsid}")

    def get_filelist(self):
        return self.arcfile.filelist

    def verify_filelist(self):
        return len(self.arcfile.filelist) == self.filecount

    def verify_checksum(self, chksum):
        self.logger.warning("Checksum verification not implemented, yet")
        return True


class UserInterruptException(Exception):
    def __init__(self, arcfile):
        self.arcfile = arcfile

    def __str__(self):
        return repr(self.arcfile)


class GroupPackager:

    def __init__(self, path, file_pattern, s_group, store_name, archive_path, archive_size, min_age, max_age, verify):
        self.path = path
        self.path_pattern = re.compile(os.path.join(path, file_pattern))
        self.s_group = re.compile(s_group)
        self.store_name = re.compile(store_name)
        self.archive_path = os.path.join(mount_point, archive_path)
        if not os.path.exists(self.archive_path):
            os.makedirs(self.archive_path, mode=0o777)
            os.chmod(self.archive_path, 0o777)
        self.archive_size = int(archive_size.replace('G', '000000000').replace('M', '000000').replace('K', '000'))
        self.min_age = int(min_age)
        self.max_age = int(max_age)
        self.verify = verify
        self.client = MongoClient(mongo_uri)
        self.db = self.client[mongo_db]
        self.logger = logging.getLogger(name=f"GroupPackager[{self.path_pattern.pattern}]")

    def __del__(self):
        self.client.close()
        logging.debug("Connection to MongoDB closed, container-object deleted.")

    def verify_container(self, container):
        verified = False
        if self.verify == 'filelist':
            verified = container.verify_filelist()
        elif self.verify == 'chksum':
            verified = container.verify_checksum(0)
        elif self.verify == 'off':
            verified = True
        else:
            self.logger.warning(f"Unknown verification method {self.verify}. Assuming failure!")
            verified = False

        return verified

    def create_archive_entry(self, container):
        container_local_path = container.localfilepath
        container_chimera_path = container.pnfsfilepath
        try:
            container_pnfsid = read_dotfile(container_local_path, 'id')

            self.db.archives.insert({'pnfsid': container_pnfsid, 'path': container_chimera_path})
        except InterruptedError:
            self.logger.error("Got interruption signal. Remove entry.")
            if container_pnfsid:
                self.db.archives.remove({'pnfsid': container_pnfsid, 'path': container_chimera_path})
        except IOError as e:
            self.logger.critical(
                f"Could not find archive file {container_chimera_path}, referred to by file entries in database! This "
                f"needs immediate attention or you will lose data!")

    def write_status(self, arcfile, current_size, next_file):
        global script_id
        with open(f"/var/log/dcache/pack-files-{script_id}.status", 'w') as statusFile:
            statusFile.write(f"Container: {arcfile}\n")
            statusFile.write(f"Size: {current_size}/{self.archive_size}\n")
            statusFile.write(f"Next: {next_file.encode('ascii', 'ignore')}\n")

    def run(self):
        global script_id
        global running
        global dcap_url
        dcap = Dcap(dcap_url)
        try:
            now = int(datetime.now().strftime("%s"))
            ctime_threshold = (now - self.min_age * 60)
            self.logger.debug(
                f"Looking for files matching {{ path: {self.path_pattern.pattern}, group: {self.s_group.pattern}, "
                f"store: {self.store_name.pattern}, ctime: {{ $lt: {ctime_threshold} }} }}")
            with self.db.files.find(
                    {'state': 'new', 'path': self.path_pattern, 'group': self.s_group, 'store': self.store_name,
                     'ctime': {'$lt': ctime_threshold}}, timeout=False).batch_size(512) as cursor:
                cursor.sort('ctime', ASCENDING)
                sumsize = 0
                old_file_mode = False
                ctime_oldfile_threshold = (now - self.max_age * 60)
                for f in cursor:
                    if f['ctime'] < ctime_oldfile_threshold:
                        old_file_mode = True
                    sumsize += f['size']

                filecount = cursor.count()

                self.logger.info(f"found {filecount} files with a combined size of {sumsize} bytes")
                if old_file_mode:
                    self.logger.debug(f"containing old files: ctime < {ctime_oldfile_threshold}")
                else:
                    self.logger.debug(f"containing no old files: ctime < {ctime_oldfile_threshold}")

                if old_file_mode:
                    if sumsize < self.archive_size:
                        self.logger.info(
                            "combined size of old files not big enough for a regular archive, packing in old-file-mode")

                    else:
                        old_file_mode = False
                        self.logger.info(
                            "combined size of old files big enough for regular archive, packing in normal mode")
                elif sumsize < self.archive_size:
                    self.logger.info(
                        f"no old files found and {self.archive_size - sumsize} bytes missing to create regular "
                        f"archive of size {self.archive_size}, leaving packager")
                    return

                cursor.rewind()
                container = None
                container_chimera_path = None
                try:
                    for f in cursor:
                        if filecount <= 0 or sumsize <= 0:
                            self.logger.info(
                                "Actual number of files exceeds precalculated number, will collect new files in next "
                                "run.")
                            break

                        self.logger.debug(
                            f"Next file {f['path']} [{f['pnfsid']}], remaining {filecount} [{sumsize} bytes]")
                        if not running:
                            if container:
                                raise UserInterruptException(container.localfilepath)
                            else:
                                raise UserInterruptException(None)

                        if container is None:
                            if sumsize >= self.archive_size or old_file_mode:
                                container = Container(self.archive_path, dcap)
                                self.logger.info(
                                    f"Creating new container {container.pnfsfilepath} . {filecount} files "
                                    f"[{sumsize} bytes] remaining.")
                            else:
                                self.logger.info(
                                    f"remaining combined size {sumsize} < {self.archive_size}, leaving packager")
                                return

                        if old_file_mode:
                            self.logger.debug(f"{sumsize} bytes remaining for this archive")
                            self.write_status(container.pnfsfilepath, sumsize, f"{f['path']} [{f['pnfsid']}]")
                        else:
                            self.logger.debug(f"{self.archive_size - container.size} bytes remaining for this archive")
                            self.write_status(container.pnfsfilepath, self.archive_size - container.size,
                                              f"{f['path']} [{f['pnfsid']}]")

                        try:
                            localfile = f['path'].replace(data_root, mount_point, 1)
                            self.logger.debug(f"before container.add({f['path']}[{f['pnfsid']}], {f['size']})")
                            container.add(f['pnfsid'], f['path'], localfile, f['size'])
                            self.logger.debug("before collection.save")
                            f['state'] = f"added: {container.pnfsfilepath}"
                            f['lock'] = script_id
                            cursor.collection.save(f)
                            self.logger.debug(f"Added file {f['path']} [{f['pnfsid']}]")
                        except IOError as e:
                            self.logger.exception(
                                f"IOError while adding file {f['path']} to archive {container.pnfsfilepath} [{f['pnfsid']}], {str(e)}")
                            self.logger.debug(f"Removing entry for file {f['pnfsid']}")
                            self.db.files.remove({'pnfsid': f['pnfsid']})
                        except OSError as e:
                            self.logger.exception(
                                f"OSError while adding file {f['path']} to archive {f['pnfsid']} [{container.pnfsfilepath}], {str(e)}")
                            self.logger.debug(f"Removing entry for file {f['pnfsid']}")
                            self.db.files.remove({'pnfsid': f['pnfsid']})
                        except errors.OperationFailure as e:
                            self.logger.error(
                                f"Removing container {container.localfilepath} due to OperationalFailure. See below for details.")
                            container.close()
                            os.remove(container.localfilepath)
                            raise e
                        except errors.ConnectionFailure as e:
                            self.logger.error(
                                f"Removing container {container.localfilepath} due to ConnectionFailure. See below for details.")
                            container.close()
                            os.remove(container.localfilepath)
                            raise e

                        sumsize -= f['size']
                        filecount -= 1

                        if container.size >= self.archive_size:
                            self.logger.debug(f"Closing full container {container.pnfsfilepath}")
                            container_chimera_path = container.pnfsfilepath
                            container.close()

                            if self.verify_container(container):
                                self.logger.info(f"Container {container.pnfsfilepath} successfully stored")
                                self.db.files.update({'state': f'added: {container_chimera_path}'},
                                                     {'$set': {'state': f'archived: {container_chimera_path}'},
                                                      '$unset': {'lock': ""}}, multi=True)
                                self.create_archive_entry(container)
                            else:
                                self.logger.warning(
                                    f"Removing container {container.localfilepath} due to verification error")
                                self.db.files.update({'state': f'added: {container_chimera_path}'},
                                                     {'$set': {'state': 'new'}, '$unset': {'lock': ""}}, multi=True)
                                os.remove(container.localfilepath)

                            container = None

                    if container:
                        if not old_file_mode:
                            self.logger.warning(
                                f"Removing unful container {container.localfilepath} . Maybe a file was deleted "
                                f"during packaging.")
                            container.close()
                            os.remove(container.localfilepath)
                            return

                        self.logger.debug(f"Closing container {container.pnfsfilepath} containing remaining old files")
                        container_chimera_path = container.pnfsfilepath
                        container.close()

                        if self.verify_container(container):
                            self.logger.info(f"Container {container.pnfsfilepath} with old files successfully stored")
                            self.db.files.update({'state': f'added: {container_chimera_path}'},
                                                 {'$set': {'state': f'archived: {container_chimera_path}'},
                                                  '$unset': {'lock': ""}}, multi=True)
                            self.create_archive_entry(container)
                        else:
                            self.logger.warning(
                                f"Removing container {container.localfilepath} with old files due to verification error")
                            self.db.files.update({'state': f'added: {container_chimera_path}'},
                                                 {'$set': {'state': 'new'}, '$unset': {'lock': ""}}, multi=True)
                            os.remove(container.localfilepath)
                except InterruptedError:
                    self.logger.info(f"Caught interruption. Cleanup")
                    os.remove(container.localfilepath)
                    # if lock is script_id, the state is set to new and lock removed when while-loop is entered
                    # in main
                    dcap.close()
                    sys.exit("Interruption signal")
                except IOError as e:
                    self.logger.error(
                        f"{e.strerror} closing file {container_chimera_path}. Trying to clean up files in state: "
                        f"'added'. This might need additional manual fixing!")
                    self.db.files.update({'state': f'added: {container_chimera_path}'},
                                         {'$set': {'state': 'new'}, '$unset': {'lock': ""}}, multi=True)
                except errors.OperationFailure as e:
                    self.logger.error(
                        f"Operation Exception in database communication while creating container "
                        f"{container_chimera_path} . Please check!")
                    self.logger.error(f'{str(e)}')
                    os.remove(container.localfilepath)
                except errors.ConnectionFailure as e:
                    self.logger.error(
                        f"Connection Exception in database communication. Removing incomplete container "
                        f"{container_chimera_path} .")
                    self.logger.error(f'{str(e)}')
                    os.remove(container.localfilepath)

        finally:
            dcap.close()


def read_dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), f".({tag})({os.path.basename(filepath)})"), mode='r') as dotfile:
        result = dotfile.readline().strip()
    return result


def main(configfile='/etc/dcache/container.conf'):
    global running

    # initialize logging
    logger = logging.getLogger()
    log_handler = None

    while running:
        global script_id
        global archive_user
        global archive_mode
        global mount_point
        global data_root
        global mongo_uri
        global mongo_db
        global dcap_url

        try:
            configuration = parser.RawConfigParser(
                defaults={'scriptId': 'pack', 'archiveUser': 'root', 'archiveMode': '0644',
                          'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5,
                          'logLevel': 'ERROR'})
            configuration.read(configfile)

            script_id = configuration.get('DEFAULT', 'scriptId')

            log_level_str = configuration.get('DEFAULT', 'logLevel')
            log_level = getattr(logging, log_level_str.upper(), None)
            logger.setLevel(log_level)

            if log_handler is not None:
                log_handler.close()
                logger.removeHandler(log_handler)

            log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/pack-files-{script_id}.log')
            formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
            log_handler.setFormatter(formatter)
            logger.addHandler(log_handler)

            archive_user = configuration.get('DEFAULT', 'archiveUser')
            archive_mode = configuration.get('DEFAULT', 'archiveMode')
            mount_point = configuration.get('DEFAULT', 'mountPoint')
            data_root = configuration.get('DEFAULT', 'dataRoot')
            mongo_uri = configuration.get('DEFAULT', 'mongoUri')
            mongo_db = configuration.get('DEFAULT', 'mongodb')
            dcap_url = configuration.get('DEFAULT', 'dcapUrl')

            loop_delay = configuration.getint('DEFAULT', 'loopDelay')

            logging.info(f'Successfully read configuration from file {configfile}.')
            logging.debug(f'scriptId = {script_id}')
            logging.debug(f'archiveUser = {archive_user}')
            logging.debug(f'archiveMode = {archive_mode}')
            logging.debug(f'mountPoint = {mount_point}')
            logging.debug(f'dataRoot = {data_root}')
            logging.debug(f'mongoUri = {mongo_uri}')
            logging.debug(f'mongoDb = {mongo_db}')
            logging.debug(f'dcapUrl = {dcap_url}')
            logging.debug(f'logLevel = {log_level}')
            logging.debug(f'loopDelay = {loop_delay}')

            try:
                client = MongoClient(mongo_uri)
                db = client[mongo_db]
                logging.info("Established db connection")

                logging.info("Sanitizing database")
                db.files.update({'lock': script_id}, {'$set': {'state': 'new'}, '$unset': {'lock': ""}}, multi=True)

                logging.info("Creating group packagers")
                groups = configuration.sections()
                group_packagers = []
                for group in groups:
                    logging.debug(f"Group: {group}")
                    file_pattern = configuration.get(group, 'fileExpression')
                    logging.debug(f"filePattern: {file_pattern}")
                    s_group = configuration.get(group, 'sGroup')
                    logging.debug(f"sGroup: {s_group}")
                    store_name = configuration.get(group, 'storeName')
                    logging.debug(f"storeName: {store_name}")
                    archive_path = configuration.get(group, 'archivePath')
                    logging.debug(f"archivePath: {archive_path}")
                    archive_size = configuration.get(group, 'archiveSize')
                    logging.debug(f"archiveSize: {archive_size}")
                    min_age = configuration.get(group, 'minAge')
                    logging.debug(f"minAge: {min_age}")
                    max_age = configuration.get(group, 'maxAge')
                    logging.debug(f"maxAge: {max_age}")
                    verify = configuration.get(group, 'verify')
                    logging.debug(f"verify: {verify}")
                    pathre = re.compile(configuration.get(group, 'pathExpression'))
                    logging.debug(f"pathExpression: {pathre.pattern}")
                    paths = db.files.find({'parent': pathre}).distinct('parent')
                    pathset = set()
                    for path in paths:
                        pathmatch = re.match(f"(?P<sfpath>{pathre.pattern})", path).group('sfpath')
                        pathset.add(pathmatch)

                    logging.debug(f"Creating a packager for each path in: {pathset}")
                    for path in pathset:
                        packager = GroupPackager(
                            path,
                            file_pattern,
                            s_group,
                            store_name,
                            archive_path,
                            archive_size,
                            min_age,
                            max_age,
                            verify)
                        group_packagers.append(packager)
                        logging.info(f"Added packager {group} for paths matching {packager.path}")

                logging.info("Running packagers")
                for packager in group_packagers:
                    packager.run()

                client.close()

            except errors.ConnectionFailure as e:
                logging.error(f"Connection to DB failed: {str(e)}")

            logging.info(f"Sleeping for {loop_delay} seconds")
            time.sleep(loop_delay)

        except UserInterruptException as e:
            if e.arcfile:
                logging.info(f"Cleaning up unfinished container {e.arcfile}.")
                os.remove(e.arcfile)
                logging.info("Cleaning up modified file entries.")
                container_chimera_path = e.arcfile.replace(mount_point, data_root, 1)
                db.files.update({'state': f'added: {container_chimera_path}'}, {'$set': {'state': 'new'}}, multi=True)

            logging.info("Finished cleaning up. Exiting.")
            sys.exit(1)
        except parser.NoOptionError as e:
            print(f"Missing option: {str(e)}")
            logging.error(f"Missing option: {str(e)}")
        except parser.Error as e:
            print(f"Error reading configfile {configfile}: {str(e)}")
            logging.error(f"Error reading configfile {configfile}: {str(e)}")
            sys.exit(2)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)  # Keyboard interrupt
    signal.signal(signal.SIGTERM, sigint_handler)  # Service stopped
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
