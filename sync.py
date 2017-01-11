#! /usr/bin/env python
#-*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

import sys
import argparse
import logging, logging.handlers
import pymongo
import mongo_synchronizer

# global variables
g_src = ''
g_src_engine = 'mongodb'
g_src_authdb = 'admin'
g_src_username = ''
g_src_password = ''
g_dst = ''
g_dst_authdb = 'admin'
g_dst_username = ''
g_dst_password = ''
g_dbs = []
g_colls = []
g_start_optime = ''
g_write_concern = 1
g_logfilepath = ''

def parse_args():
    """ Parse arguments.
    """
    global g_src, g_src_engine, g_src_authdb, g_src_username, g_src_password, g_dst, g_dst_username, g_dst_password, g_dbs, g_colls, g_start_optime, g_write_concern, g_logfilepath

    parser = argparse.ArgumentParser(description='Sync data from a replica-set to another mongod/replica-set/sharded-cluster.')
    parser.add_argument('--from', nargs='?', required=True, help='the source must be a member of replica-set')
    parser.add_argument('--src-authdb', nargs='?', required=False, help="authentication database, default is 'admin'")
    parser.add_argument('--src-username', nargs='?', required=False, help='src username')
    parser.add_argument('--src-password', nargs='?', required=False, help='src password')
    parser.add_argument('--src-engine', nargs='?', required=False, help='src engine, the value could be mongodb or tokumx, default is mongodb')
    parser.add_argument('--to', nargs='?', required=True, help='the destionation should be a mongos or mongod instance')
    parser.add_argument('--dst-authdb', nargs='?', required=False, help="authentication database, default is 'admin'")
    parser.add_argument('--dst-username', nargs='?', required=False, help='dst username')
    parser.add_argument('--dst-password', nargs='?', required=False, help='dst password')
    parser.add_argument('--dbs', nargs='+', required=False, help='databases to sync, conflict with --colls')
    parser.add_argument('--colls', nargs='+', required=False, help='collections to sync, conflict with --dbs')
    parser.add_argument('--start-optime', nargs='?', required=False, help="start optime, a timestamp value in second for MongoDB or a 'YYYYmmddHHMMSS' value for TokuMX")
    parser.add_argument('--write-concern', nargs='?', required=False, help='write concern, default 1')
    parser.add_argument('--log', nargs='?', required=False, help='log file path')

    args = vars(parser.parse_args())
    if args['from'] != None:
        g_src = args['from']
    if args['src_engine'] != None:
        if args['src_engine'] not in ['mongodb', 'tokumx']:
            print 'invalid src_engine, terminate'
            sys.exit(1)
        g_src_engine = args['src_engine']
    if args['src_authdb'] != None:
        g_src_authdb = args['src_authdb']
    if args['src_username'] != None:
        g_src_username = args['src_username']
    if args['src_password'] != None:
        g_src_password = args['src_password']
    if args['to'] != None:
        g_dst = args['to']
    if args['dst_authdb'] != None:
        g_dst_authdb = args['dst_authdb']
    if args['dst_username'] != None:
        g_dst_username = args['dst_username']
    if args['dst_password'] != None:
        g_dst_password = args['dst_password']
    if args['dbs'] != None:
        g_dbs = args['dbs']
    if args['colls'] != None:
        g_colls = args['colls']
    if args['start_optime'] != None:
        g_start_optime = args['start_optime']
    if args['write_concern'] != None:
        g_write_concern = int(args['write_concern'])
    if args['log'] != None:
        g_logfilepath = args['log']

    if g_dbs and g_colls:
        print 'conflict options: --dbs --colls, terminate'
        sys.exit(1)

def logger_init(filepath):
    """ Init logger for global.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    if filepath:
        handler_log = logging.handlers.RotatingFileHandler(filepath, mode='a', maxBytes=1024*1024*100, backupCount=3)
        handler_log.setFormatter(formatter)
        handler_log.setLevel(logging.INFO)
        logger.addHandler(handler_log)
    else:
        handler_stdout = logging.StreamHandler(sys.stdout)
        handler_stdout.setFormatter(formatter)
        handler_stdout.setLevel(logging.INFO)
        logger.addHandler(handler_stdout)

if __name__ == '__main__':
    parse_args()

    logger_init(g_logfilepath)
    logger = logging.getLogger()
    logger.info('================================================')
    logger.info('src             :  %s' % g_src)
    logger.info('src engine      :  %s' % g_src_engine)
    if g_src_username or g_src_password:
        logger.info('src authdb      :  %s' % g_src_authdb)
    else:
        logger.info('src authdb      :  ')
    logger.info('src username    :  %s' % g_src_username)
    logger.info('src password    :  %s' % g_src_password)
    logger.info('dst             :  %s' % g_dst)
    if g_dst_username or g_dst_password:
        logger.info('dst authdb      :  %s' % g_dst_authdb)
    else:
        logger.info('dst authdb      :  ')
    logger.info('dst username    :  %s' % g_dst_username)
    logger.info('dst password    :  %s' % g_dst_password)
    logger.info('databases       :  %s' % g_dbs)
    logger.info('collections     :  %s' % g_colls)
    logger.info('start optime    :  %s' % g_start_optime)
    logger.info('write concern   :  %s' % g_write_concern)
    logger.info('log filepath    :  %s' % g_logfilepath)
    logger.info('pymongo version :  %s' % pymongo.version)
    logger.info('================================================')
 
    syncer = mongo_synchronizer.MongoSynchronizer(
            g_src,
            g_dst,
            src_authdb=g_src_authdb,
            src_username=g_src_username,
            src_password=g_src_password,
            src_engine=g_src_engine,
            dst_authdb=g_dst_authdb,
            dst_username=g_dst_username,
            dst_password=g_dst_password,
            dbs=g_dbs,
            colls=g_colls,
            ignore_indexes=False,
            start_optime=g_start_optime,
            write_concern=g_write_concern)
    syncer.run()
    logger.info('exit')
