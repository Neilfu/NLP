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
    options,args = getopt.getopt(sys.argv[1:],"h",['site=', 'procode=', 'level2=', 'level3=','style','dump','geo', 'dealer', 'city', 'host=', 'loglevel=','port=','sleep=', 'database=','productTable=','catTable=','pagesize=', 'hasPrice','all','batchUpdate', 'hasSpec','delta', 'help','catUpdate'])
    for opt, value in options:
        if opt in ['--procode']:
            strKey = re.sub('-','',opt)
            retMatch = re.findall(r'(\d+)\.\.(\d+)',value)
            if retMatch:
                (left,right) = retMatch[0]
                para[strKey] = range(int(left),int(right)+1)
            else:
                strPara = value.split(',')
                para[strKey] = strPara
        elif opt in ['--site','--database','--catTable','--productTable']:
            strKey = re.sub('-','',opt)
            para[strKey] = value.decode('gb2312')
        elif opt in ['--host','--port','--pagesize','--sleep','--loglevel']:
            strKey = re.sub('-','',opt)
            para[strKey] = value
        elif opt in ['--style','--dump', '--dealer', '--city','--all','--geo']:
            strKey = re.sub('-','',opt)
            para[strKey] = True
        if opt in ['-h','--help']:
            usage()
            sys.exit()
    return para

def usage():
    print "Usage: python getCategory.py [--help] [--site] [--dump] [--geo] [--dealer] [--city] [--homeUrl]  [--host] [--sleep] [--port] [--sleep] [--database] [--productTable] [--catTable] [--level1] [--level2] [--level3] [--delta] [--batchUpdate] [--catUpdate]\n"

def getStyle(path=''):
    styleDb = openTable(tableName=global_setting['styledb'])
    retStyleList=[]
    RFILE = open(path,'r')
    for line in RFILE.readlines():
        (id,url,brand,model,style) = line.split(',')[0:5]
        if id == 'id':
            continue
        brand = re.sub(r'[\"\s]','',brand)
        model = re.sub(r'[\"\s]','',model)
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
    styleLen =styleDb.find().count()
    cityCount = 1
    for city in cityDb.find().sort('procode',ASCENDING):
        if city['procode'] not in global_setting['procode']:
            continue
        logger.info('begin %s(%s):%s(%s),(%d/%d)' %(city['proname'],city['procode'],city['cityname'],city['citycode'],cityCount,cityLen))
        cityCount = cityCount + 1
        styleCount = 1
        for style in styleDb.find():
            progressBar("progress",styleCount,styleLen)
            styleCount = styleCount + 1
            targetUrl = baseUrl %(style['stylecode'],city['citycode'])
            if dearDb.find({'styleid':int(style['stylecode']),'city':city['cityname']}).count() >0:
                logger.info('style:%s,city:%s dealers already exists,skip' %(style['stylecode'],city['cityname']))
                continue
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
    fileHeader = (u'车款ID',u'车款名称',u'品牌名称',u'车型',u'省份',u'城市',u'经销商ID',u'经销商地址',u'联系电话',u"经度",u"纬度")
    #import pdb;pdb.set_trace()
    WFILE.write('\t'.join(fileHeader).encode('utf-8')+'\n')
    mapLocation = getLocationMap()
    for dealer in dealearDb.find():
        record=[]
        for field in fieldList:
            if field in [u'styleid',u'dealerid']:
                record.append(str(dealer[field]))
            else:
                record.append(dealer[field].encode('utf-8'))
        location = mapLocation.get(dealer['dealerid'],False)
        if not location:
            (lon,lat) = ('','')
        else:
            (lon,lat) = (str(location[0]),str(location[1]))
        record.extend((lon,lat))
        WFILE.write('\t'.join(record)+'\n')
    WFILE.close()

def getLocationMap():
    retMap = {}
    dealearGeoDb = openTable(tableName=global_setting['dealerGeodb'])
    for dealer in dealearGeoDb.find():
        retMap.update({dealer['dealerid']:[dealer['lon'],dealer['lat']]})
    return retMap


def getDealerLocation():
    baseUrl = "http://dealer.bitauto.com/VendorMap/GoogleMap.aspx?dID=%d&S=S&W=498&H=385&Z=12"
    dealearDb = openTable(tableName=global_setting['dealerdb'])
    dealearGeoDb = openTable(tableName=global_setting['dealerGeodb'])
    for dealerid in dealearDb.distinct('dealerid'):
        if dealearGeoDb.find({'dealerid':dealerid}).count() > 0:
            logger.info('dealer:%d location already exists,skip' %(dealerid))
            continue
        sleepTime = random.random()*0.3+0.1
        time.sleep(sleepTime)
        r = session.get(baseUrl %(dealerid))
        if not r:
            continue
        retMatch = re.findall(r'AddData\((.*?),(.*?), \'(.*?)\'',r.text)
        if retMatch:
            (lat,lon,name) = retMatch[0]
            dealearGeoDb.insert({'dealerid':dealerid,'dealername':name,'lon':lon,'lat':lat})


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
    global_setting['dealerGeodb'] = retPara.get('dealerGeodb','dealerGeodb')
    global_setting['city'] = retPara.get('city',False)
    global_setting['style'] = retPara.get('style',False)
    global_setting['dealer'] = retPara.get('dealer',False)
    global_setting['geo'] = retPara.get('geo',False)
    global_setting['dump'] = retPara.get('dump',True)
    global_setting['procode'] = retPara.get('procode',range(1,32))
    #import pdb;pdb.set_trace()
    if global_setting['style']:
        listStyle = getStyle('./bit_auto.csv')
    if global_setting['city']:
        cities = getCitiesCode()
    if global_setting['dealer']:
        cities = getDealers()
    #if global_setting['geo']:
    #    getDealerLocation()
    if global_setting['dump']:
        dumpDealerDb()




