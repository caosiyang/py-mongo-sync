# source
src_host = ''
src_port = 27017
src_replset = ''
username = ''
password = ''
dbnames = []

# destination
dst_host = ''
dst_port = 27017
dst_replset = ''

# oplog
# update optime once apply the specified count of oplog
optime_interval = 100

# log

# buffer-mongod to save oplog
buf_host = ''
buf_port = 27017
capped_collection_size = 1024*1024*1024*10
