#! /usr/bin/env python
# -*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

from gevent import monkey
monkey.patch_all()

import sys
from mongosync.command_options import CommandOptions
from mongosync.config import MongoConfig, EsConfig
from mongosync.logger import Logger

if __name__ == '__main__':
    conf = CommandOptions.parse()
    Logger.init(conf.logfilepath)
    log = Logger.get()

    conf.info(log)
    if conf.logfilepath:
        conf.info(sys.stdout)

    if isinstance(conf.dst_conf, MongoConfig):
        from mongosync.mongo.syncer import MongoSyncer
        syncer = MongoSyncer(conf)
        syncer.run()
    elif isinstance(conf.dst_conf, EsConfig):
        from mongosync.es.syncer import EsSyncer
        syncer = EsSyncer(conf)
        syncer.run()
    else:
        raise Exception('invalid config type')

    log.info('exit')
