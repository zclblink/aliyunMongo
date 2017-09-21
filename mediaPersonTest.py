#coding=utf-8

'''
此程序运行在47.93.29.135机器上
构造Mdeia和Person
'''

import pymongo
from bson.objectid import ObjectId
import datetime
import time
import json
import requests
from PIL import Image
import cStringIO
import redis
import multiprocessing

flag = 1



if flag == 1:
    # client = pymongo.MongoClient(
    #     "mongodb://root:2egRPV4LaDtoNUE18ZSwB7@dds-2ze898a8f674d1741.mongodb.rds.aliyuncs.com:3717,dds-2ze898a8f674d1742.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-4375909")
    client1 = pymongo.MongoClient("mongodb://biliankeji:biliankeji@47.93.29.135:27200")
    imgUrl = "http://renrenimg0724.oss-cn-beijing.aliyuncs.com/{0}.{1}"
    feaUrl = "http://172.17.196.131:8001/local?path=/tmp/renrenimg0724/{0}.{1}"
    # pool = redis.ConnectionPool(host='47.93.29.135', port=6379, db=1)
    # rs = redis.Redis(connection_pool=pool)
    # pool = redis.ConnectionPool(host='47.93.29.135', port=6379, db=0)
    # rs0 = redis.Redis(connection_pool=pool)
elif flag == 2:
    # client = pymongo.MongoClient("mongodb://root:2egRPV4LaDtoNUE18ZSwB7@47.94.254.214:3717")
    client1 = pymongo.MongoClient("mongodb://biliankeji:biliankeji@47.93.29.135:27200")
    imgUrl = "http://renrenimg0724.oss-cn-beijing.aliyuncs.com/{0}.{1}"
    feaUrl = "http://g00.me-yun.com:8001/local?path=/tmp/renrenimg0724/{0}.{1}"
    # pool = redis.ConnectionPool(host='47.93.29.135', port=6379, db=1)
    # rs = redis.Redis(connection_pool=pool)
    # pool = redis.ConnectionPool(host='47.93.29.135', port=6379, db=0)
    # rs0 = redis.Redis(connection_pool=pool)

# db = client['honey']
db1 = client1['renren0717']


def getImage():
    result = db1.MeFile.find({'uid':'469024088','albumName':{"$regex":'头像相册'}},{'_id':1,'uid':1,'albumName':1,'type':1})
    for res in result:
        print res['_id']
        open('mediaPersonTest','a').write(str(res['_id']) + '\t' + res['uid']+ '\t' + res['type']+'\n')
        # except Exception,ex:
        #     print ex

def getSize(url):
    file = requests.get(url)
    tmpIm = cStringIO.StringIO(file.content)
    img = Image.open(tmpIm)
    return img.size

def reduceImage(res):
    print res['_id']
    furl = feaUrl.format(str(res['_id']), res['type'])
    r = requests.get(furl)
    data = json.loads(r.text)
    print data
    if 'errMsg' in data:
        print res['_id'], data['errMsg']
        return -1
    personAll = []
    user = '469024088'
    if len(data) > 0:
        for info in data:
            # info = json.loads(info)
            person = {
                # "user": "596c56b2b4b33e11caf84e92",
                # "hasUser": False,
                "possible": [{'user': user, 'score': 0.0}],
                "media": str(res['_id']),
                "rect": info['rect'],
                # "rectDLib": "",
                # "rectArc": "",
                "feature": info['feature'],
                # "featureDLib": "",
                # "featureArc": "",
                "detectScore": info["detectScore"],
                "landmarkScore": info["landmarkScore"],
                "acl": {
                    "*": {
                            "read": True,
                            "write": True
                        }
                },
                'updateAt': datetime.datetime.now()
            }
            person['createAt'] = person['updateAt']
            personAll.append(person)

        iurl = imgUrl.format(str(res['_id']), res['type'])
        size = getSize(iurl)
        pIdAll = []
        for person in personAll:
            oid = db1.Person_test.insert(person)
            pIdAll.append(str(oid))
        media = {
            "_id": res['_id'],
            "file": {
                "_type": "pointer",
                "_id": str(res['_id']),
                "_class": "MeFile"
            },
            "mediaType": 0,
            "width": size[0],
            "height": size[1],
            # "upload": "",
            "backupUser": user,
            "albumName": "头像相册",
            "price": 0,
            "persons": pIdAll,
            "acl": {
                "*": {
                    "read": True,
                    "write": True
                }
            },
            'updateAt': datetime.datetime.now()
        }
        media['createAt'] = media['updateAt']
        db1.Media_test.save(media)
    else:
        print 123


def reduceMediaPerson():
    result = db1.MeFile.find({'_id':ObjectId('599c23501b32094ab9fcc8b2'),'albumName':{"$regex":'头像相册'}},{'_id':1,'uid':1,'albumName':1,'type':1})
    for item in result:
        # items = item.strip('\n').split('\t')
        # res = {
        #     '_id':ObjectId(items[0]),
        #     'uid':items[1],
        #     'albumName':'头像相册',
        #     'type':items[2]
        # }
        if item['type'] != 'gif':
            reduceImage(item)







if __name__ == "__main__":
    # if flag == 1:
    #     cpu_count = multiprocessing.cpu_count()
    #     processList = []
    #     for i in range(cpu_count):
    #         processList.append(multiprocessing.Process(target=reduceMediaPerson, args=()))
    #     for process in processList:
    #         process.start()
    #     for process in processList:
    #         process.join()

    reduceMediaPerson()

    # updateBackupUser()

    # updateMediaPerson()

    # r = requests.get("http://n01.me-yun.com:8001/local?path=/tmp/renrenimg0724/596c5e8db4b33e151eabb400.jpg")
    # print r.text
    # reduceUesr()
    # reduceContact()

    # print getSize("http://renrenimg0724.oss-cn-beijing.aliyuncs.com/596c639ab4b33e6de7d6ed82.jpg")

    # getNo()

    # getImage()
