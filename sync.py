#! /usr/bin/env python
#-*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

try:
    from gevent import monkey
    monkey.patch_all()
    gevent_support = True
except ImportError as e:
    gevent_support = False
from mongosync.command_options import CommandOptions
from mongosync.logger import Logger
from mongosync.mongo_synchronizer import MongoSynchronizer

if __name__ == '__main__':
    conf = CommandOptions.parse()

    Logger.init(conf.logfilepath)
    logger = Logger.get()

    conf.asyncio = gevent_support
    conf.info(logger)

    syncer = MongoSynchronizer(
            conf.src_hostportstr,
            conf.dst_hostportstr,
            src_engine=conf.src_engine,
            src_authdb=conf.src_authdb,
            src_username=conf.src_username,
            src_password=conf.src_password,
            dst_authdb=conf.dst_authdb,
            dst_username=conf.dst_username,
            dst_password=conf.dst_password,
            dbs=conf.dbs,
            colls=conf.colls,
            src_db=conf.src_db,
            dst_db=conf.dst_db,
            ignore_indexes=False,
            start_optime=conf.start_optime,
            asyncio = conf.asyncio)
    syncer.run()
    logger.info('exit')

