#coding=utf-8
import random
import re
import sys, getopt

import datetime
import requests
from bs4 import BeautifulSoup
import logging
import json
from pymongo import MongoClient,ASCENDING
import time
from urllib import unquote

LEVELS={'DEBUG':logging.DEBUG,
        'INFO':logging.INFO,
        'WARNING':logging.WARNING,
        'ERROR':logging.ERROR,
        'CRITICAL':logging.CRITICAL,
        }

def setLog(strLevel='INFO',logfile='../log.txt'):
    level=LEVELS[strLevel]
    logging.basicConfig(
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                )
    logger = logging.getLogger()
    handler = logging.FileHandler(logfile)
    logger.addHandler(handler)
    console = logging.StreamHandler()
    logger.addHandler(console)
    logger.setLevel(level)
    return logger

def progressBar(strTitle,num=1, total=100):
    rate = int(float(num) / float(total)*1000)
    sys.stdout.write(strTitle+"\t=>\t"+str(rate/10.0)+"%\r")
    sys.stdout.flush()

def openTable(tableName=''):
    strConn = "mongodb://" + global_setting['host'] + ":" + global_setting['port']
    con = MongoClient(strConn)
    db = con[global_setting['database']]
    return db[tableName]

def parseCommandLine():
    para = {}
    options,args = getopt.getopt(sys.argv[1:],"h",['site=', 'level1=', 'level2=', 'level3=','style','dump', 'dealer', 'city', 'host=', 'loglevel=','port=','sleep=', 'database=','productTable=','catTable=','pagesize=', 'hasPrice','all','batchUpdate', 'hasSpec','delta', 'help','catUpdate'])
    for opt, value in options:
        if opt in ['--level1','--level2','--level3']:
            strKey = re.sub('-','',opt)
            para[strKey] = value.decode('gb2312').split(',')
        elif opt in ['--site','--database','--catTable','--productTable']:
            strKey = re.sub('-','',opt)
            para[strKey] = value.decode('gb2312')
        elif opt in ['--host','--port','--pagesize','--sleep','--loglevel']:
            strKey = re.sub('-','',opt)
            para[strKey] = value
        elif opt in ['--style','--dump', '--dealer', '--city','--all']:
            strKey = re.sub('-','',opt)
            para[strKey] = True
        if opt in ['-h','--help']:
            usage()
            sys.exit()
    return para

def usage():
    print "Usage: python getCategory.py [--help] [--site] [--dump] [--dealer] [--city] [--homeUrl]  [--host] [--sleep] [--port] [--sleep] [--database] [--productTable] [--catTable] [--level1] [--level2] [--level3] [--delta] [--batchUpdate] [--catUpdate]\n"

def getStyle(path=''):
    styleDb = openTable(tableName=global_setting['styledb'])
    retStyleList=[]
    RFILE = open(path,'r')
    for line in RFILE.readlines():
        (id,url,brand,model,style) = line.split(',')[0:5]
        if id == 'id':
            continue
        brand = re.sub(r'[\"\s]','',brand)
        model = re.sub(r';\"\s]','',model)
        style = re.sub(r'[\"\s]','',style)
        stylecode = re.findall(r'\/m(\d+)\/\"',url)
        record = {"stylecode":stylecode[0], "name":style,"brand":brand,"model":model}
        retStyleList.append(record)
        styleDb.insert(record)
    return retStyleList

def getCitiesCode():
    cityDb = openTable(tableName=global_setting['citydb'])
    nameList = ["安徽","北京","福建","甘肃","广东","广西","贵州","海南","河北","河南","黑龙江","湖北","湖南","吉林","江苏","江西","辽宁","内蒙古 ","宁夏","青海","山东","山西","陕西","上海","四川","天津","西藏","新疆","云南","浙江","重庆"]
    baseUrl = 'http://dealer.bitauto.com/zuidijia/ashx/getCitys.ashx?p=%d'
    cities = []
    for index, item in enumerate(nameList):
        index = index + 1
        r = session.get(baseUrl %(index))
        strJson = re.sub(r'name','"name"',r.text)
        strJson = re.sub(r'id','"id"',strJson)
        listCity = json.loads(strJson)
        for city in listCity:
            cities.append((city['id'],city['name'],index,item)) #城市编码、城市名称、省份编码、省份名称
            record = {"citycode":city['id'],"cityname":city['name'],"procode":index,"proname":item}
            cityDb.insert(record)
    return cities


def getDealers():
    baseUrl = 'http://dealer.bitauto.com/zuidijia/ashx/getDealers.ashx?car=%s&city=%s'
    cityDb = openTable(tableName=global_setting['citydb'])
    styleDb = openTable(tableName=global_setting['styledb'])
    dearDb = openTable(tableName=global_setting['dealerdb'])
    cityLen = cityDb.find().count()
    for style in styleDb.find():
        logger.info('begin %s:%s' %(style['model'],style['name']))
        count = 0
        for city in cityDb.find():
            progressBar("progress",count,cityLen)
            count = count + 1
            targetUrl = baseUrl %(style['stylecode'],city['citycode'])
            try:
                sleepTime = random.random()*0.3+0.1
                time.sleep(sleepTime)
                r = session.get(targetUrl)
                retStr = re.sub(r'(\w+):',r'"\1":',r.text)
                retJson = json.loads(retStr)[0]
                dealerRecord = {"styleid":retJson['carid'],"stylename":style['name'],"brand":style['brand'],"model":style['model'],"city": city['cityname'],"province":city['proname']}
                for dealer in retJson['data']:
                    record = dict(dealerRecord)
                    record.update({"dealerid":dealer['id'],"name":unquote(dealer['name'].encode('utf-8')),"addr":unquote(dealer['ad'].encode('utf-8')),"telephone":dealer['te']})
                    dearDb.insert(record)
            except Exception ,e:
                logger.exception('fail in getting style:%s, city:%s,url:%s, skip'%(style['stylecode'],city['citycode'],targetUrl))

def dumpDealerDb(path='./dealerlist.csv'):
    dealearDb = openTable(tableName=global_setting['dealerdb'])
    WFILE = open(path,'w')
    #fieldMap = {'styleid':u'车款ID','stylename':u'车款名称','brand':u'品牌名称','model':u'车型','province':u'省份','city':u'城市','dealerid':u'经销商ID','addr':u'经销商地址','telephone':u'联系电话'}
    fieldList = ('styleid','stylename','brand','model','province','city','dealerid','addr','telephone')
    fileHeader = (u'车款ID',u'车款名称',u'品牌名称',u'车型',u'省份',u'城市',u'经销商ID',u'经销商地址',u'联系电话')
    #import pdb;pdb.set_trace()
    WFILE.write('\t'.join(fileHeader).encode('utf-8')+'\n')
    for dealer in dealearDb.find().sort('styleid',ASCENDING):
        record=[]
        for field in fieldList:
            if field in [u'styleid',u'dealerid']:
                record.append(str(dealer[field]))
            else:
                record.append(dealer[field].encode('utf-8'))
        WFILE.write('\t'.join(record)+'\n')
    WFILE.close()


global_setting = {}
global session
global logger
if __name__ == '__main__':

    session = requests.Session()
    retPara = parseCommandLine()
    global_setting['loglevel'] = retPara.get('loglevel','INFO')
    logger = setLog(global_setting['loglevel'])
    logger.debug('log level, %d' %(logger.level))
    global_setting['host'] = retPara.get('host','127.0.0.1')
    global_setting['port'] = retPara.get('port','27017')
    global_setting['database'] = retPara.get('database','auto')
    global_setting['citydb'] = retPara.get('citydb','citydb')
    global_setting['styledb'] = retPara.get('styledb','styledb')
    global_setting['dealerdb'] = retPara.get('dealerdb','dealerdb')
    global_setting['city'] = retPara.get('city',False)
    global_setting['style'] = retPara.get('style',False)
    global_setting['dealer'] = retPara.get('dealer',False)
    global_setting['dump'] = retPara.get('dump',True)
    #import pdb;pdb.set_trace()
    if global_setting['style']:
        listStyle = getStyle('./bit_auto.csv')
    if global_setting['city']:
        cities = getCitiesCode()
    if global_setting['dealer']:
        cities = getDealers()
    if global_setting['dump']:
        dumpDealerDb()




