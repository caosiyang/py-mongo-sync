#python main.py --from nl-mongodb-online003-bjdxt.qiyi.virtual:27018 --to 10.15.188.223:27017 --db qiyirc --coll qiyirc --query "{'uid': {'\$regex': '^tv_'}}"
python main.py --from demo-mongodb-dev001-jylt.qiyi.virtual:27018 --to 10.15.188.223:27017 --db qiyirc --coll qiyirc --query "{'uid': {'\$regex': '^tv_'}}"
