#!/usr/bin/env python3
# coding=utf-8

import os
import sys
import time
import signal
import configparser as parser
from pymongo import MongoClient, errors
import logging
import logging.handlers

running = True


def sigint_handler(signum, frame):
    global running
    logging.info(f"Caught signal {signum}")
    print(f"Caught signal {signum}'")
    running = False


mongo_uri = "mongodb://localhost/"
mongo_db = "smallfiles"
mount_point = ""
data_root = ""


def read_dotfile(filepath, tag):
    with open(os.path.join(os.path.dirname(filepath), f".({tag})({os.path.basename(filepath)})"), mode='r') \
            as dotfile:
        result = dotfile.readline().strip()
    return result


def main(configfile='/etc/dcache/container.conf'):
    global running

    # initialize logging
    logger = logging.getLogger()
    log_handler = None

    try:
        while running:
            configuration = parser.RawConfigParser(
                defaults={'scriptId': 'pack', 'archiveUser': 'root', 'archiveMode': '0644',
                          'mongoUri': 'mongodb://localhost/', 'mongoDb': 'smallfiles', 'loopDelay': 5,
                          'logLevel': 'ERROR'})
            configuration.read(configfile)
            # if type(configuration) == FileNotFoundError:
            #     logging.error(f"Configuration file {configfile} not found.")
            #     return

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

            log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/fillmetadata-{script_id}.log')
            formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
            log_handler.setFormatter(formatter)
            logger.addHandler(log_handler)

            mount_point = configuration.get('DEFAULT', 'mountPoint')
            data_root = configuration.get('DEFAULT', 'dataRoot')
            mongo_uri = configuration.get('DEFAULT', 'mongoUri')
            mongo_db = configuration.get('DEFAULT', 'mongodb')
            loop_delay = configuration.getint('DEFAULT', 'loopDelay')

            logging.info(f'Successfully read configuration from file {configfile}.')

            try:
                client = MongoClient(mongo_uri)
                db = client[mongo_db]

                with db.files.find({'state': {'$exists': False}}, snapshot=True) as new_files_cursor:
                    logging.info(f"found {new_files_cursor.count()} new files")
                    for record in new_files_cursor:
                        if not running:
                            sys.exit(1)
                        try:
                            pathof = read_dotfile(os.path.join(mount_point, record['pnfsid']), 'pathof')
                            localpath = pathof.replace(data_root, mount_point, 1)
                            stats = os.stat(localpath)

                            record['path'] = pathof
                            record['parent'] = os.path.dirname(pathof)
                            record['size'] = stats.st_size
                            record['ctime'] = stats.st_ctime
                            record['state'] = 'new'

                            new_files_cursor.collection.save(record)
                            logging.debug(f"Updated record: {str(record)}")
                        except KeyError as e:
                            logging.warning(f"KeyError: {str(record)}: {str(e)}")
                        except IOError as e:
                            logging.warning(f"IOError: {str(record)}: {str(e)}")
                            logging.info(f"Removing entry for file {record['pnfsid']}")
                            db.files.remove({'pnfsid': record['pnfsid']})
                        except OSError as e:
                            logging.warning(f"OSError: {str(record)}: {str(e)}")
                            logging.exception(e)
                            logging.info(f"Removing entry for file {record['pnfsid']}")
                            db.files.remove({'pnfsid': record['pnfsid']})

                client.close()

            except errors.ConnectionFailure as e:
                logging.warning(f"Connection failure: {str(e)}")
            except errors.OperationFailure as e:
                logging.warning(f"Could not create cursor: {str(e)}")

            logging.info(f"Sleeping for {loop_delay} seconds")
            time.sleep(loop_delay)

    except parser.NoOptionError as e:
        print(f"Missing option: {str(e)}")
        logging.error(f"Missing option: {str(e)}")
        sys.exit(2)
    except parser.Error as e:
        print(f"Error reading configfile {configfile}: {str(e)}")
        logging.error(f"Error reading configfile {configfile}: {str(e)}")
        sys.exit(2)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)  # Keyboard interrupt
    signal.signal(signal.SIGTERM, sigint_handler)  # Service stopped
    if not os.getuid() == 0:
        print("fillmetadata.py must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: fillmetadata.py <configfile>")
        sys.exit(1)
