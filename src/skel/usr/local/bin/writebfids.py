#!/usr/bin/env python3
# coding=utf-8

import os
import sys
import time
import errno
import signal
from zipfile import ZipFile, BadZipfile
from pymongo import MongoClient, errors
import configparser as parser
import logging
import logging.handlers

running = True

# script_id = 'pack'
archive_user = 'root'
archive_mode = '0644'
mount_point = ""
data_root = ""
mongo_uri = "mongodb://localhost/"
mongo_db = "smallfiles"
# dcap_url = ""


def sigint_handler(signum, frame):
    global running
    print(f"Caught signal {signum}.")
    running = False


def main(configfile='/etc/dcache/container.conf'):
    global running

    # initialize logging
    logger = logging.getLogger()
    log_handler = None

    try:
        while running:
            configuration = parser.RawConfigParser(defaults={'scriptId': 'pack', 'mongoUri': 'mongodb://localhost/',
                                                             'mongoDb': 'smallfiles', 'loopDelay': 5,
                                                             'logLevel': 'ERROR'})
            configuration.read(configfile)

            global archive_user
            global archive_mode
            global mount_point
            global data_root
            global mongo_uri
            global mongo_db

            script_id = configuration.get('DEFAULT', 'scriptId')

            log_level_str = configuration.get('DEFAULT', 'logLevel')
            log_level = getattr(logging, log_level_str.upper(), None)
            logger.setLevel(log_level)

            if log_handler is not None:
                log_handler.close()
                logger.removeHandler(log_handler)

            log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/writebfids-{script_id}.log')
            formatter = logging.Formatter('%(asctime)s %(name)-80s %(levelname)-8s %(message)s')
            log_handler.setFormatter(formatter)
            logger.addHandler(log_handler)

            archive_user = configuration.get('DEFAULT', 'archiveUser')
            archive_mode = configuration.get('DEFAULT', 'archiveMode')
            mount_point = configuration.get('DEFAULT', 'mountPoint')
            data_root = configuration.get('DEFAULT', 'dataRoot')
            mongo_uri = configuration.get('DEFAULT', 'mongoUri')
            mongo_db = configuration.get('DEFAULT', 'mongodb')

            loop_delay = configuration.getint('DEFAULT', 'loopDelay')

            logging.info(f'Successfully read configuration from file {configfile}.')

            try:
                client = MongoClient(mongo_uri)
                db = client[mongo_db]
                logging.info("Established db connection")

                with db.archives.find() as archives:
                    for archive in archives:
                        if not running:
                            logging.info("Exiting.")
                            sys.exit(1)
                        try:
                            localpath = archive['path'].replace(data_root, mount_point, 1)
                            archive_pnfsid = archive['pnfsid']
                            zf = ZipFile(localpath, mode='r', allowZip64=True)
                            for f in zf.filelist:
                                logging.debug(f"Entering bfid into record for file {f.filename}")
                                filerecord = db.files.find_one(
                                    {'pnfsid': f.filename, 'state': f'archived: {archive["path"]}'}, await_data=True)
                                if filerecord:
                                    url = f"dcache://dcache/?store={filerecord['store']}&group={filerecord['group']}" \
                                          f"&bfid={f.filename}:{archive_pnfsid} "
                                    filerecord['archiveUrl'] = url
                                    filerecord['state'] = f'verified: {archive["path"]}'
                                    db.files.save(filerecord)
                                    logging.debug(f"Updated record with URL {url} in archive {archive['path']}")
                                else:
                                    logging.warning(
                                        f"File {f.filename} in archive {archive['path']} has no entry in DB. This "
                                        f"could be caused by a previous forced interrupt. Creating failure entry.")
                                    db.failures.insert({'archiveId': archive_pnfsid, 'pnfsid': f.filename})

                            logging.debug(f"stat({localpath}): {os.stat(localpath)}")

                            db.archives.remove({'pnfsid': archive['pnfsid']})
                            logging.debug(f"Removed entry for archive {archive['path']}[{archive['pnfsid']}]")

                            pnfsname = os.path.join(os.path.dirname(localpath), archive['pnfsid'])
                            logging.debug(f"Renaming archive {localpath} to {pnfsname}")
                            os.rename(localpath, pnfsname)

                        except BadZipfile as e:
                            logging.warning(f"Archive {localpath} is not yet ready. Will try later.")

                        except IOError as e:
                            if e.errno != errno.EINTR:
                                logging.error(f"IOError: {e.strerror}")
                            else:
                                logging.info("User interrupt.")

                client.close()

            except errors.ConnectionFailure as e:
                logging.warning(f"Connection to DB failed: {str(e)}")
            except errors.OperationFailure as e:
                logging.warning(f"Could not create cursor: {str(e)}")
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")

            logging.info(f"Processed all archive entries. Sleeping {loop_delay} seconds.")
            time.sleep(loop_delay)

    except parser.NoOptionError as e:
        print(f"Missing option: {str(e)}")
        logging.error(f"Missing option: {str(e)}")
    except parser.Error as e:
        print(f"Error reading configfile {configfile}: {str(e)}")
        logging.error(f"Error reading configfile {configfile}: {str(e)}")
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
