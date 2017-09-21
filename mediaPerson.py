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
    client = pymongo.MongoClient(
        "mongodb://root:2egRPV4LaDtoNUE18ZSwB7@dds-2ze898a8f674d1741.mongodb.rds.aliyuncs.com:3717,dds-2ze898a8f674d1742.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-4375909")
    client1 = pymongo.MongoClient("mongodb://biliankeji:biliankeji@47.93.29.135:27200")
    imgUrl = "http://renrenimg0724.oss-cn-beijing.aliyuncs.com/{0}.{1}"
    feaUrl = "http://172.17.196.131:8001/local/feature?path=/tmp/renrenimg0724/{0}.{1}"
    detUrl = "http://172.17.196.132:8001/local/detect?MeFile={0}.{1}"
    # pool = redis.ConnectionPool(host='47.93.29.135', port=6379, db=2)
    pool = redis.ConnectionPool(host='r-2zefb20ffc37a974.redis.rds.aliyuncs.com', port=6379,
                                password="2egRPV4LaDtoNUE18ZSwB7", db=2)
    rs = redis.Redis(connection_pool=pool)
    pool0 = redis.ConnectionPool(host='47.93.29.135', port=6379, db=0)
    rs0 = redis.Redis(connection_pool=pool0)
elif flag == 2:
    client = pymongo.MongoClient("mongodb://root:2egRPV4LaDtoNUE18ZSwB7@47.94.254.214:3717")
    client1 = pymongo.MongoClient("mongodb://biliankeji:biliankeji@47.93.29.135:27200")
    imgUrl = "http://renrenimg0724.oss-cn-beijing.aliyuncs.com/{0}.{1}"
    feaUrl = "http://g00.me-yun.com:8001/local/feature?path=/tmp/renrenimg0724/{0}.{1}"
    pool = redis.ConnectionPool(host='47.93.29.135', port=6379, db=2)
    rs = redis.Redis(connection_pool=pool)
    pool0 = redis.ConnectionPool(host='47.93.29.135', port=6379, db=0)
    rs0 = redis.Redis(connection_pool=pool0)

db = client['honey']
db1 = client1['renren0717']
db2 = client1['renren08301100']

'''
将用户信息提交到阿里云mongodb上
'''
media = {
    "file": "",
    "mediaType": 1,
    "width": "",
    "height": "",
    "upload": "",
    "backupUser": "",
    "albumName": "",
    "price": 0,
    "persons": ""
}

person = {
    "user": "",
    "hasUser": "",
    "possible": "",
    "media": "",
    "rect": "",
    "rectDLib": "",
    "rectArc": "",
    "feature": "",
    "featureDLib": "",
    "featureArc": "",
    "detectScore":"",
    "landmarkScore":""
}

'''
将mongo中的头像链接放入redis中
'''
def getImage():
    haveRunId = {}
    fn = open('uniqHaveRun','r').readlines()
    for line in fn:
        haveRunId[line.strip('\n')] = ''

    result = db.Media.find({},{'_id':1})
    for res in result:
        haveRunId[str(res['_id'])] = ""
    print len(haveRunId)
    #2017-08-30 10:30
    result = db1.MeFile.find({
                                'albumName':{"$regex":'头像相册'},
                                'createAt': {'$gte': datetime.datetime(2017, 8, 21, 0, 0, 0)}
                              },
                             {'_id':1,'uid':1,'albumName':1,'type':1})
    count = 0
    for res in result:
        count += 1
        if not count % 1000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), count / 1000, 'q'
        if str(res['_id']) in haveRunId:
            continue
        try:
            rs0.rpush('mediaPerson',str(res['_id']) + '\t' + res['uid'] + '\t' + res['albumName'] + '\t' + res['type'])
        except Exception,ex:
            print ex

def getImageTwo():
    haveRunId = {}
    fn = open('uniqHaveRun','r').readlines()
    for line in fn:
        haveRunId[line.strip('\n')] = ''

    result = db.Media.find({},{'_id':1})
    for res in result:
        haveRunId[str(res['_id'])] = ""
    print len(haveRunId)
    result = db2.userInfo0830.find({},{'userId':1}).batch_size(5)
    count = 0
    for ruser in result:
        print ruser['userId']
        result = db1.MeFile.find({
                                    'albumName':{"$regex":'头像相册'},
                                    'uid':ruser['userId']
                                  },
                                 {'_id':1,'uid':1,'albumName':1,'type':1})
        for res in result:
            count += 1
            if not count % 1000:
                print time.strftime("%Y-%m-%d %H:%M:%S"), count / 1000, 'q'
            if str(res['_id']) in haveRunId:
                continue
            try:
                rs0.rpush('mediaPerson',str(res['_id']) + '\t' + res['uid'] + '\t' + res['albumName'] + '\t' + res['type'])
            except Exception,ex:
                print ex

def getSize(url):
    file = requests.get(url)
    tmpIm = cStringIO.StringIO(file.content)
    img = Image.open(tmpIm)
    return img.size

def reduceImage(res):
    try:
        furl = feaUrl.format(str(res['_id']), res['type'])
        # print 'get'
        r = requests.get(furl)
        # print 'next'
        jsonText = r.text
        data = json.loads(jsonText)
        if 'errMsg' in data:
            print res['_id'], data['errMsg']
            return -1
        personAll = []

        user = rs.get(res['uid'])
        if user == None:
            user = db1['userInfo0717'].find_one({'userId': res['uid']}, {'_id': 1})
            user = str(user['_id'])
        facer = data['facer']
        if len(facer) > 0:
            for info in facer:
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
                oid = db.Face.insert(person)
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
                "faces": pIdAll,
                "acl": {
                    "*": {
                        "read": True,
                        "write": True
                    }
                },
                'updateAt': datetime.datetime.now()
            }
            media['createAt'] = media['updateAt']
            db.Media.save(media)
        else:
            open('haveRunImageNoFace', 'a').write(str(res['_id']) + '\n')
    except Exception,ex:
        print res,ex
        return -1
    return 1


def reduceMediaPerson():
    mediaRun = {}
    # result = db.Media.find({},{'_id':1})
    # for res in result:
    #     mediaRun[str(res['_id'])] = ""
    while rs0.llen('mediaPerson') > 0:
        item = rs0.lpop('mediaPerson')
        item = item.split('\t')
        print item
        if item[0] not in mediaRun:
            res = {
                '_id':ObjectId(item[0]),
                'uid':item[1],
                'albumName':item[2],
                'type':item[3]
            }
            if res['type'] != 'gif':
                flag = 0
                while flag>=0 and flag<3:
                    ret = reduceImage(res)
                    if ret > 0:
                        flag = -1
                    else:
                        time.sleep(5)
                        flag += 1
                if flag < 0:
                    open('haveRunImage','a').write(str(res['_id']) + '\n')
                else:
                    open('haveRunImageError', 'a').write(str(res['_id']) + '\n')
                # if not i % 1000:
                #     print time.strftime("%Y-%m-%d %H:%M:%S"), i / 1000, 'q'



def updateBackupUser():
    db.BackupUser.update_many({},{"$unset":{
                                "user": "",
                                "nickName": "",
                                "idCard": "",
                                "gender": "",
                                "address": "",
                                "weiboVerify": "",
                                "school": "",
                                "birthday": "",
                                "hometown": "",
                                "signature": "",
                                "weibo": "",
                                "douban": "",
                                "maimai": ""
    }})


def updateMediaPerson():
    result = db.Person.find()
    for res in result:
        res["acl"] = {
                   "*": {
                       "read": True,
                       "write": True
                   }
               }
        res['updateAt'] = datetime.datetime.now()
        res['createAt'] = res['updateAt']
        db.Person.save(res)

'''
删除在Person不在Media中的(删除不在Media中的Person)
必须在识别人脸程序停止的情况下运行
'''
def getNo():
    mediaPerson = {}
    result = db.Media.find({},{'persons':1})
    for i,res in enumerate(result):
        for person in res['persons']:
            mediaPerson[person] = ""
        if not i % 10000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), i / 10000, 'w'

    result = db.Person.find({},{'_id':1})
    for i,res in enumerate(result):
        if not i % 10000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), i / 10000, 'w'
        if str(res['_id']) not in mediaPerson:
            print res['_id']
            db.Person.remove({'_id':res['_id']})


'''
删除在Media不在Person中的(删除不在Person中的Media)
必须在识别人脸程序停止的情况下运行
'''
def getNoTwo():
    mediaPerson = {}
    result = db.Person.find({}, {'_id': 1})
    for i, res in enumerate(result):
        if not i % 10000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), i / 10000, 'w'
        mediaPerson[str(res['_id'])] = ""

    result = db.Media.find({},{'persons':1})
    for i,res in enumerate(result):
        if not i % 10000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), i / 10000, 'w'
        for person in res['persons']:
            if person not in mediaPerson:
                open('personNoMedia','a').write(str(res['_id']) + '\t' + person + '\n')

def getNoTwoTwo():
    fn = open('personNoMedia', 'r').readlines()
    for line in fn:
        lines = line.strip('\n').split('\t')
        db.Media.remove({'_id':ObjectId(lines[0])})



def testOssDow():
    print time.strftime("%Y-%m-%d %H:%M:%S")
    result = db.Media.find({},{'_id':1}).limit(1000)
    for res in result:
        print res['_id']
        name = str(res['_id']) + '.jpg'
        durl = detUrl.format(str(res['_id']),'jpg')
        r = requests.get(durl)
        jsonText = r.text
        data = json.loads(jsonText)
        item = {
            '_id':res['_id'],
            'name':name,
            'data':data
        }
        db1.faceDetect.save(item)
    print time.strftime("%Y-%m-%d %H:%M:%S")


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

    # reduceMediaPerson()

    # updateBackupUser()

    # updateMediaPerson()

    # r = requests.get("http://n01.me-yun.com:8001/local?path=/tmp/renrenimg0724/596c5e8db4b33e151eabb400.jpg")
    # print r.text
    # reduceUesr()
    # reduceContact()

    # print getSize("http://renrenimg0724.oss-cn-beijing.aliyuncs.com/596c639ab4b33e6de7d6ed82.jpg")

    # getNo()
    # getNoTwo()
    # getNoTwoTwo()

    # getImage()
    # getImageTwo()

    # result = db.Person.find()
    # for res in result:
    #     db.Face.save(res)
    #     db.Person.remove({'_id':res['_id']})
    testOssDow()

    client.close()
    client1.close()
