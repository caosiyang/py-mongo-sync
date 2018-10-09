#! /usr/bin/env python
# -*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

try:
    from gevent import monkey
    monkey.patch_all()
except ImportError:
    pass

from mongosync.command_options import CommandOptions
from mongosync.config import MongoConfig, EsConfig
from mongosync.logger import Logger
from mongosync.mongo.syncer import MongoSyncer
from mongosync.es.syncer import EsSyncer

log = Logger.get()

if __name__ == '__main__':
    conf = CommandOptions.parse()
    Logger.init(conf.logfilepath)
    conf.info(log)

    if isinstance(conf.dst_conf, MongoConfig):
        syncer = MongoSyncer(conf)
        syncer.run()
    elif isinstance(conf.dst_conf, EsConfig):
        syncer = EsSyncer(conf)
        syncer.run()
    else:
        raise Exception('invalid dst type')

    log.info('exit')
