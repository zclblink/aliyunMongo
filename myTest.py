#coding=utf-8

'''
此程序运行在47.93.29.135机器上
'''

import pymongo
import datetime
import time
import re
from bson.objectid import ObjectId

flag = 2


if flag == 1:
    client = pymongo.MongoClient(
        "mongodb://root:2egRPV4LaDtoNUE18ZSwB7@dds-2ze898a8f674d1741.mongodb.rds.aliyuncs.com:3717,dds-2ze898a8f674d1742.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-4375909")
    client1 = pymongo.MongoClient("mongodb://biliankeji:biliankeji@127.0.0.1:27200")
elif flag == 2:
    client = pymongo.MongoClient("mongodb://root:2egRPV4LaDtoNUE18ZSwB7@47.94.254.214:3717")
    client1 = pymongo.MongoClient("mongodb://biliankeji:biliankeji@47.93.29.135:27200")

    # db_auth = client['admin']
    # db_auth.authenticate("root","2egRPV4LaDtoNUE18ZSwB7")

db = client['honey']
db1 = client1['renren0717']
db2 = client1['renren08301100']

'''
将用户信息提交到阿里云mongodb上
'''
def reduceUesr():
    result = db1.userInfo0717.find({}, {'_id': 1, 'name': 1, 'userId': 1, 'img': 1}).skip(2000000)
    for i, res in enumerate(result):
        stand = {
            "_id": res['_id'],
            # "user": "",
            "realName": res['name'],
            # "nickName": "",
            # "idCard": "",
            # "gender": "",
            # "address": "",
            # "weiboVerify": "",
            # "school": "",
            # "birthday": "",
            # "hometown": "",
            "avatar": res['img'],
            # "signature": "",
            "renren": res['userId'],
            # "weibo": "",
            # "douban": "",
            # "maimai": "",
            "acl": {
                "*": {
                    "read": True,
                    "write": True
                }
            },
            'updateAt': datetime.datetime.now()
        }
        stand['createAt'] = stand['updateAt']
        db.BackupUser.save(stand)
        if not i % 10000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), i / 10000, 'w'

'''
将用户的关系提交到阿里云mongodb
已提交的用户关系id保存在userContact文件中
'''
def reduceContact():
    # result = db1.userContact0717.find({'flag':{'$exists':False}},{'parent':1,'friends':1,'_id':1})
    result = db1.userContact0830_copy.find({},{'parent':1,'friends':1,'_id':1})
    for i,res in enumerate(result):
        item = {}
        item['_id'] = res['_id']
        item['user'] = res['parent']
        item['friend'] = res['friends']
        item["acl"] = {
                "*": {
                    "read": True,
                    "write": True
                }
            }
        item['updateAt'] = datetime.datetime.now()
        item['createAt'] = item['updateAt']
        db.RenRenFriends.save(item)
        if not i % 10000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), i / 10000, 'w'


'''
截取用户头像的id
'''
def getAvatar():
    result = db1.MeFile.find({'albumName': {"$regex": '头像相册'}},{'_id':1,'uid':1,'name':1})
    for i,res in enumerate(result):
        print res['_id']
        open('idavatar', 'a').write(str(res['_id']) + '\t' + res['uid'] + '\t' + res['name'] + '\n')
    #     xulie = re.search(r"_[a-z0-9]{8,}",res['name'])
    #     if xulie:
    #         xulie = xulie.group(0).replace('_','')
    #         open('_idavatar','a').write(str(res['_id'])+'\t'+res['uid']+'\t'+xulie+'\n')
        if not i % 1000:
            print time.strftime("%Y-%m-%d %H:%M:%S"), i / 1000, 'q'

'''
获取Media表的_id与uid的对应关系，更新user的_id使用
'''
def getMediaId():
    result = db.Media.find({'backupUser':None},{'_id':1})
    for res in result:
        print res['_id']
        item = db1.MeFile.find_one({'_id':res['_id']},{'uid':1})
        open('MediaIdUid','a').write(str(res['_id']) + '\t' + item['uid'] + '\n')
'''
根据uid获取user的_id
'''
def getMediaIdTwo():
    fn = open('MediaIdUid', 'r').readlines()
    for line in fn:
        print line.strip('\n')
        lines = line.strip('\n').split('\t')
        item = db1.userInfo0717.find_one({'userId':lines[1]},{'_id':1})
        open('MediaIdUidTwo', 'a').write(lines[0] + '\t' + lines[1] + '\t' + str(item['_id']) + '\n')

'''
更新Media中的backupUser
'''
def updateMediabackupUser():
    count = 0
    fn = open('MediaIdUidTwo', 'r').readlines()
    for line in fn:
        lines = line.strip('\n').split('\t')
        print lines[0]
        db.Media.update({'_id':ObjectId(lines[0])},{'$set':{'backupUser':lines[2]}})
        count += 1
        if count % 10000 == 0:
            print  time.strftime("%Y-%m-%d %H:%M:%S"), count / 10000, 'w'

'''
更新Person中的possible.user
'''
def updatePersonbackupUser():
    count = 0
    fn = open('MediaIdUidTwo', 'r').readlines()
    for line in fn:
        lines = line.strip('\n').split('\t')
        print lines[0]
        # db.Media.update({'_id': ObjectId(lines[0])}, {'$set': {'backupUser': lines[2]}})
        db.Person.update_many({"media": lines[0]}, {'$set': {'possible.0.user': lines[2]}})
        count += 1
        if count % 10000 == 0:
            print  time.strftime("%Y-%m-%d %H:%M:%S"), count / 10000, 'w'

'''
修改用户的关系（删除前1100个人的关系）
'''
def updateUserContact():
    needId = {}
    result = db2.userInfo0830.find({'f':{'$exists':False}},{'_id':1})
    for res in result:
        needId[str(res['_id'])] = ''
    print len(needId)
    for id in needId:
        print id
        db.RenRenFriends.remove({'user':id})



'''
查看user(parent)关系中用户是否存在
'''
def getIfUser():
    result = db.RenRenFriends.distinct('user')
    for res in result:
        print res
        item = db.BackupUser.find_one({'_id':ObjectId(res)})
        if item == None:
            open('noFriends','a').write(res + '\n')

'''
查看friend关系中用户是否存在
'''
def getIfFriend():
    haverun ={}
    fn = open('oldMyTest','r').readlines()
    for line in fn:
        haverun[line.strip('\n')] = ""
    count = 1
    haveFriend = {}
    result = db.RenRenFriends.find({},{'_id':-1,'friend':1})
    for res in result:
        haveFriend[res['friend']] = ""
        count += 1
        if count % 10000 == 0:
            print time.strftime("%Y-%m-%d %H:%M:%S"), count / 10000, 'w'
    print len(haveFriend)
    for res in haveFriend:
        print res
        if res in haverun:
            continue
        item = db.BackupUser.find_one({'_id':ObjectId(res)})
        if item == None:
            open('noFriendsTwo','a').write(res + '\n')

'''
更新关系表中的user(parent)
'''
def updateUserConPar():
    count = 1
    fn = open('noFriends','r').readlines()
    for line in fn:
        u_id = line.strip('\n')
        print u_id
        result = db1.userInfo0717_remove.find_one({"_id" : ObjectId(u_id)})
        if result != None:
            item = db.RenRenFriends.find_one({"_id" : ObjectId(str(result['needId']))})
            if item == None:#如果needId不存在用户关系则更新，否则删除
                try:
                    db.RenRenFriends.update_many({'user':u_id},{'$set':{'user':str(result['needId'])}})
                except Exception,ex:
                    print ex
            else:
                db.RenRenFriends.remove({'user': u_id})
        count += 1
        if count % 10000 == 0:
            print time.strftime("%Y-%m-%d %H:%M:%S"), count / 10000, 'w'


'''
更新关系表中的friend
'''
def updateUserConFri():
    count = 1
    fn = open('noFriendsThree', 'r').readlines()
    for line in fn:
        u_id = line.strip('\n')
        print u_id
        result = db1.userInfo0717_remove.find_one({"_id": ObjectId(u_id)})
        if result != None:
            try:
                db.RenRenFriends.update_many({'friend': u_id}, {'$set': {'friend': str(result['needId'])}})
            except Exception, ex:
                print ex
        count += 1
        if count % 10000 == 0:
            print time.strftime("%Y-%m-%d %H:%M:%S"), count / 10000, 'w'
if __name__ == "__main__":
    # reduceUesr()
    # reduceContact()
    # getAvatar()
    # getMediaId()
    # getMediaIdTwo()
    # updateMediabackupUser()
    # updatePersonbackupUser()
    # updateUserContact()
    getIfUser()
    # updateUserConPar()
    # getIfFriend()
    # print 123
    # updateUserConFri()
    # print len('2017-09-02 14:17:54 1 w')