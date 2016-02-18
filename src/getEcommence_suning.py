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

def getCategoryUrl(site="",url=""):
    catDb = openTable(tableName=global_setting['catTable'])
    r = session.get(url)
    if not r.text:
        return False

    soup = BeautifulSoup(r.text)
    for level1 in soup.select('.sFloor'):
        curLevel1 = level1.select('.sName')[0].text
        curLevel1 = re.sub('\s', '', curLevel1)
        for level2 in level1.select('dl'):
            curLevel2 = level2.select('dt')[0].text
            curLevel2 = re.sub('\s', '', curLevel2)
            for level3 in level2.select('span a'):
                curLevel3 = re.sub('\s', '', level3.text)
                curlUrl = level3['href']
                retFind = re.findall(r'\/\d+-(\d+)-\d+\.html',curlUrl)
                if retFind:
                    curCatID = retFind[0]
                    if catDb.find({'catId':curCatID}).count() >0:
                        logger.debug('catetogy %s exists,skip\n'%(curCatID))
                    else:
                        catDb.insert({'catId':curCatID,'level1':curLevel1, 'level2':curLevel2, 'level3':curLevel3, 'catUrl':curlUrl, 'site':site})
    return True

def getPidList4Cat():
    level1Filter = global_setting['level1']
    level2Filter = global_setting['level2']
    level3Filter = global_setting['level3']
    catDb = openTable(tableName=global_setting['catTable'])
    dbProductList = openTable(tableName=global_setting['productTable'])
    #支持Ctrl+c 中断
    ctlC = False
    #优先扫描最久没更新的类别
    for cat in catDb.find({'site':global_setting['site']}).sort('lasttime',ASCENDING):
        if ctlC:
            break
        if (global_setting['site']==cat['site']) and cat['catUrl'] and (global_setting['all'] or (level1Filter and cat['level1'] in level1Filter) \
            or (level2Filter and cat['level2'] in level2Filter) \
            or (level3Filter and cat['level3']  in level3Filter)):
            #产品的类别属性
            CATLIST = ('catId','catUrl','site','level1','level2','level3')
            paraCat = {}
            for li in CATLIST:
                if li in cat:
                    paraCat[li] = cat[li]
            #获取产品列表
            ctlC = getProduct(dbProductList, **paraCat)
            #记录最近更新时间及品类库存量
            catCount = dbProductList.find({'$and':[{'catId':cat['catId']},{'site':cat['site']}]}).count()
            lastFreshDate = datetime.datetime.utcnow()
            catDb.update({'catId':cat['catId']},{'$set':{'lasttime':lastFreshDate}})
            catDb.update({'catId':cat['catId']},{'$push':{'timeline':{'lasttime':lastFreshDate,'count':catCount}}})

def getCatPageNum(url):
    r = session.get(url)
    soup = BeautifulSoup(r.text)
    strPages = soup.select('#pageTotal')
    if strPages:
        pages = int(strPages[0].text)
    else:
        pages = 0
    return pages


def getProduct(dbProductList,**cat):
    SUFFIX = 'http://list.suning.com/0-%s-%s.html'
    catUrl = cat['catUrl']
    totalPages = getCatPageNum(catUrl)
    logger.info("begin:%s\t->%s\t->%s,\ttotal %d page" %(cat['level1'],cat['level2'],cat['level3'], totalPages) )
    rule = re.compile(r'id=\"plist\".*?>(.*?)<div class=\"clr\"',re.S)
    Skip = False
    for page in range(totalPages):
        if Skip:
            break
        try:
            progressBar("getting pages",page,totalPages)
            urlPage = SUFFIX %(cat['catId'],page)   #苏宁从0开始编号
            sleepTime = random.random()*global_setting['sleep']+0.1
            time.sleep(sleepTime)
            r = session.get(urlPage)
            if not r:
                logger.exception('failed in getting url:%s,skip'(urlPage))
                continue
            soup = BeautifulSoup(r.text)
            skuLists=[]
            for li in soup.select('.item.fl'):
                product = {}
                product.update(cat)
                soupItem = li.select('.sellPoint')[1]
                product['url'] = soupItem['href']
                strSku = re.findall(r'\/(\d+)\.html',product['url'])
                if not strSku:
                    continue
                product['sku'] = strSku[0]

                product['name'] = re.sub('\s','',soupItem.text)
                reBrand = re.findall(r'^(.*?)[\s（]',soupItem.text)
                if reBrand:
                    product['brand'] = reBrand[0]
                searchPrice = li.select_one('.i-collect.searchBg.collectI')
                if searchPrice:
                    retMatch = re.findall(r'(\d+),.*?(\d+)',searchPrice['href'])
                    strPrice = ''
                    if retMatch:
                        (le,ri) = retMatch[0]
                        if le and ri:
                            strPrice = ri + '_' + le
                if strPrice:
                     skuLists.append(strPrice)
                try:
                    if dbProductList.find({u'sku':product['sku']}).count() >0:
                        if global_setting['delta']:
                            logger.debug('Delta:on, category %s scanning finished' %(cat['catId']) )
                            Skip = True
                            break
                        else:
                            logger.debug('%s exist,skip' %(product['sku']))
                    else:
                        dbProductList.insert(product)
                    if global_setting['price'] and len(skuLists) >= 20:
                        updatePrice(skuLists,dbProductList)
                        skuLists = []
                    if global_setting['spec']:
                        getProductDetail(product['sku'],product['url'],dbProductList)
                except Exception, e:
                    logger.exception("error in Page:%d, skuid:%s, reason:%s" %(page, product['sku'], str(e)))
                    continue

        except (KeyboardInterrupt, SystemExit), e:
            logger.critical("app is interrupted, finished pages:%d" %(page))
            Skip = True
            return Skip
        except Exception,e:
            logger.exception("error in Page:%d, reason:%s" %(page,str(e)))
    logger.debug('category %s getting finished'%(cat['level3']))


def getProductDetail(sku, url, db):
    tableRule=re.compile(r'<table.*?class="Ptable".*?<\/table>',re.S)
    if not url:
        return False
    productDetail = {}
    sleepTime = random.random()*global_setting['sleep']+0.1
    time.sleep(sleepTime)
    r = session.get(url)
    try:
        table = re.findall(tableRule,r.text)[0]
        if not table:
            return productDetail
        soup = BeautifulSoup(table)
        trs = soup('tr')
        for tr in trs:
            if len(tr('td')) == 2:
                productDetail[tr('td')[0].text.replace('.','')] = tr('td')[1].text
                #product[tr('td')[0].text.replace('.','').encode('utf-8')] = tr('td')[1].text.encode('utf-8')
        db.update({'sku':sku},{'$set':{'spec':productDetail}})
    except Exception,e:
        logger.exception("error in parsing sku:%s\t page:%s,reson:%s" %(sku, url,str(e)))
        return False
    return True

def updatePrice(skuLists,db):
    priceUrl = 'http://ds.suning.cn/ds/general/%s-9264-1--1--getDataFromDsServer2.jsonp'
    sleepTime = random.random()*global_setting['sleep']+0.1
    time.sleep(sleepTime)
    strSku = ",".join(skuLists)
    r = session.get(priceUrl %(strSku))
    if not r.text:
        return False
    reMatch = re.findall(r'(\{.*\})',r.text)
    jsonPriceLists = {}
    if reMatch:
        jsonPriceLists = json.loads(reMatch[0])
    for price in jsonPriceLists['rs']:
        if price['price'] and price['cmmdtyCode']:
            skuid = re.sub('^0+','',price['cmmdtyCode'])
            price = price['price']
            curTime = datetime.datetime.utcnow()
            db.update({'sku':skuid},{'$set':{'price':price}})
            db.update({'sku':skuid},{'$push':{'timeline':{'date':curTime,'price':price}}})
    return True

def parseCommandLine():
    para = {}
    options,args = getopt.getopt(sys.argv[1:],"h",['site=', 'level1=', 'level2=', 'level3=', 'host=', 'loglevel=','port=','sleep=', 'database=','productTable=','catTable=','pagesize=', 'hasPrice','all','batchUpdate', 'hasSpec','delta', 'help','catUpdate'])
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
        elif opt in ['--hasPrice','--hasSpec','--delta','--batchUpdate','--catUpdate','--all']:
            strKey = re.sub('-','',opt)
            para[strKey] = True
        if opt in ['-h','--help']:
            usage()
            sys.exit()
    return para

def updateBactch():
    dbProductList = openTable(tableName=global_setting['productTable'])
    updateCatids = getUpdateCat()
    for catid,catname in updateCatids:
        skuList = []
        logger.info('price updating to categary %s(%s) '%(catname,catid))
        for product in dbProductList.find({'catId':catid}):
            try:
                if global_setting['spec']:
                    getProductDetail(product['sku'],product['url'],dbProductList)
                skuList.append(product['sku'])
                if len(skuList) > global_setting['pagesize']:
                    updatePrice(skuList, dbProductList);
                    skuList = []
            except Exception, e:
                logger.exception("error in updating pricing:%s %s " %(catname, str(e)))
                continue
    return True



def getUpdateCat():
    level1Filter = global_setting['level1']
    level2Filter = global_setting['level2']
    level3Filter = global_setting['level3']
    retCat = []
    catDb = openTable(tableName=global_setting['catTable'])
    for cat in catDb.find({'site':global_setting['site']}).sort('lasttime',ASCENDING):
        if (global_setting['site']==cat['site']) and cat['catUrl'] and ((level1Filter and cat['level1'] in level1Filter) \
            or (level2Filter and cat['level2'] in level2Filter) \
            or level3Filter and cat['level3']  in level3Filter):
            retCat.append((cat['catId'],cat['level3']))
    return retCat

def usage():
    print "Usage: python getCategory.py [--help] [--site] [--hasPrice] [--hasSpec] [--loglevel] [--homeUrl]  [--host] [--sleep] [--port] [--sleep] [--database] [--productTable] [--catTable] [--level1] [--level2] [--level3] [--delta] [--batchUpdate] [--catUpdate]\n"

global_setting = {}
global session
global logger
if __name__ == '__main__':

    session = requests.Session()
    retPara = parseCommandLine()
    global_setting['loglevel'] = retPara.get('loglevel','INFO')
    logger = setLog(global_setting['loglevel'])
    logger.debug('log level, %d' %(logger.level))
    global_setting['site'] = retPara.get('site',u'京东')
    global_setting['targetUrl'] = retPara.get('homeUrl','http://www.suning.com/emall/pgv_10052_10051_1_.html')
    global_setting['level1'] = retPara.get('level1',None)
    global_setting['level2'] = retPara.get('level2',None)
    global_setting['level3'] = retPara.get('level3',None)
    global_setting['spec'] = retPara.get('hasSpec',False)
    global_setting['price'] = retPara.get('hasPrice',False)
    global_setting['delta'] = retPara.get('delta',False)
    global_setting['host'] = retPara.get('host','127.0.0.1')
    global_setting['port'] = retPara.get('port','27017')
    global_setting['database'] = retPara.get('database','productKB')
    global_setting['catTable'] = retPara.get('catTable','catdb')
    global_setting['productTable'] = retPara.get('productTable','productdb')
    global_setting['pagesize'] = retPara.get('pagesize',60)
    global_setting['batchUpdate'] = retPara.get('batchUpdate',False)
    global_setting['catUpdate'] = retPara.get('catUpdate',False)
    global_setting['sleep'] = retPara.get('sleep',0.5)
    global_setting['all'] = retPara.get('all',False)
    #import pdb;pdb.set_trace()
    if global_setting['catUpdate']:
        getCategoryUrl(site=global_setting['site'],url=global_setting['targetUrl'])
    if global_setting['batchUpdate']:
        updateBactch()
    else:
        getPidList4Cat()



