# source
#src_host = 'servicecloud-test-dev09.qiyi.virtual'
src_host = 'lego-mongodb-online01.qiyi.virtual'
src_port = 27018
src_repl_set = 'test'
src_hosts = []
username = 'lego'
password = 'lego'

# destination
dst_host = 'passport-mongodb-online25-bjdx.qiyi.virtual'
dst_port = 27017
dst_repl_set = None
dst_hosts = []

# oplog
# update optime once apply the specified count of oplog
optime_interval = 100

# log
