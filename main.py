#! /usr/bin/env python
#-*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

import sys
#import argparse
import logging, logging.handlers
import mongo_synchronizer
import settings


def parse_args():
    """ Parse arguments.
    """
    parser = argparse.ArgumentParser(description='Sync data from a replica-set to another mongos/mongod instance.')
    parser.add_argument('--from', nargs='?', required=True, help='the source mongod instance in replica-set')
    parser.add_argument('--to', nargs='?', required=True, help='the destination mongos/mongod instance')
    parser.add_argument('--db', nargs='+', required=False, help='the names of databases to be synchronized')
    parser.add_argument('--oplog', action='store_true', help='enable continuous synchronization')
    parser.add_argument('-u, --username', nargs='?', required=False, help='username')
    parser.add_argument('-p, --password', nargs='?', required=False, help='password')
    #parser.add_argument('--help', nargs='?', required=False, help='help information')
    args = vars(parser.parse_args())
    src_host = args['from'].split(':', 1)[0]
    src_port = int(args['from'].split(':', 1)[1])
    dst_host = args['to'].split(':', 1)[0]
    dst_port = int(args['to'].split(':', 1)[1])
    db = args['db']
    username = args['username']
    password = args['password']
    return src_host, src_port, dst_host, dst_port, db, username, password


def logger_init():
    """ Init logger for global.
    """
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler_stdout = logging.StreamHandler(sys.stdout)
    handler_stdout.setFormatter(formatter)
    handler_stdout.setLevel(logging.INFO)
    handler_log = logging.handlers.RotatingFileHandler('log', mode='a', maxBytes=1024*1024*100, backupCount=10)
    handler_log.setFormatter(formatter)
    handler_log.setLevel(logging.INFO)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler_stdout)
    logger.addHandler(handler_log)


def main():
    logger_init()
    logger = logging.getLogger()
    #src_host, src_port, dst_host, dst_port, db, username, password = parse_args()
    #syncer = MongoSynchronizer(src_host, src_port, dst_host, dst_port, db, username=username, password=password)

    syncer = mongo_synchronizer.MongoSynchronizer(
            settings.Source.hostportstr, settings.Destination.hostportstr,
            src_username=settings.Source.username, src_password=settings.Source.password,
            dst_username=settings.Destination.username, dst_password=settings.Destination.password)
    syncer.run()

    logger.info('exit')


if __name__ == '__main__':
    main()
