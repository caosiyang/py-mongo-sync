#! /usr/bin/env python
#-*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

import pymongo
from mongosync.mongo_synchronizer import MongoSynchronizer
from mongosync.command_options import CommandOptions
from mongosync.logger import Logger

if __name__ == '__main__':
    opts = CommandOptions()
    opts.parse()

    Logger.init(opts.logfilepath)

    logger = Logger.get()
    logger.info('================================================')
    logger.info('src hostportstr :  %s' % opts.src_hostportstr)
    logger.info('src engine      :  %s' % opts.src_engine)
    if opts.src_username or opts.src_password:
        logger.info('src authdb      :  %s' % opts.src_authdb)
    else:
        logger.info('src authdb      :  ')
    logger.info('src username    :  %s' % opts.src_username)
    logger.info('src password    :  %s' % opts.src_password)
    logger.info('dst hostportstr :  %s' % opts.dst_hostportstr)
    if opts.dst_username or opts.dst_password:
        logger.info('dst authdb      :  %s' % opts.dst_authdb)
    else:
        logger.info('dst authdb      :  ')
    logger.info('dst username    :  %s' % opts.dst_username)
    logger.info('dst password    :  %s' % opts.dst_password)
    logger.info('databases       :  %s' % opts.dbs)
    logger.info('collections     :  %s' % opts.colls)
    logger.info('start optime    :  %s' % opts.start_optime)
    logger.info('write concern   :  %s' % opts.write_concern)
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
            ignore_indexes=False,
            start_optime=opts.start_optime,
            write_concern=opts.write_concern)
    syncer.run()
    logger.info('exit')

