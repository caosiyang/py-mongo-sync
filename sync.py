#! /usr/bin/env python
#-*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

import pymongo
from mongosync.mongo_synchronizer import MongoSynchronizer
from mongosync.command_options import CommandOptions
from mongosync.logger import Logger
from mongosync.mongo_helper import get_version

if __name__ == '__main__':
    opts = CommandOptions()
    opts.parse()

    Logger.init(opts.logfilepath)

    logger = Logger.get()
    logger.info('================================================')
    logger.info('src hostportstr :  %s' % opts.src_hostportstr)
    logger.info('src engine      :  %s' % opts.src_engine)
    logger.info('src db version  :  %s' % get_version(opts.src_host, opts.src_port))
    if opts.src_username or opts.src_password:
        logger.info('src authdb      :  %s' % opts.src_authdb)
    else:
        logger.info('src authdb      :  ')
    logger.info('src username    :  %s' % opts.src_username)
    logger.info('src password    :  %s' % opts.src_password)
    logger.info('dst hostportstr :  %s' % opts.dst_hostportstr)
    logger.info('dst db version  :  %s' % get_version(opts.dst_host, opts.dst_port))
    if opts.dst_username or opts.dst_password:
        logger.info('dst authdb      :  %s' % opts.dst_authdb)
    else:
        logger.info('dst authdb      :  ')
    logger.info('dst username    :  %s' % opts.dst_username)
    logger.info('dst password    :  %s' % opts.dst_password)
    logger.info('databases       :  %s' % opts.dbs)
    logger.info('collections     :  %s' % opts.colls)
    logger.info('src db          :  %s' % opts.src_db)
    logger.info('dst db          :  %s' % opts.dst_db)
    logger.info('start optime    :  %s' % opts.start_optime)
    logger.info('log filepath    :  %s' % opts.logfilepath)
    logger.info('pymongo version :  %s' % pymongo.version)
    logger.info('================================================')
 
    syncer = MongoSynchronizer(
            opts.src_hostportstr,
            opts.dst_hostportstr,
            src_authdb=opts.src_authdb,
            src_username=opts.src_username,
            src_password=opts.src_password,
            src_engine=opts.src_engine,
            dst_authdb=opts.dst_authdb,
            dst_username=opts.dst_username,
            dst_password=opts.dst_password,
            dbs=opts.dbs,
            colls=opts.colls,
            src_db=opts.src_db,
            dst_db=opts.dst_db,
            ignore_indexes=False,
            start_optime=opts.start_optime)
    syncer.run()
    logger.info('exit')

