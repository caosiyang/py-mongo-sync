# use python for config now,
# it may be improved later


# source config
# set a mongod instance of a replica set with the format like HOST:PORT
src = 'localhost:27017'

# username and password of source replica set
# if no authentication, set them a empty string
username = ''
password = ''


# destination config
# set a mongos or a standalone mongod or a mongod instance of a replica set with the format like HOST:PORT
dst = 'localhost:27018'

# not support authentication for destination
