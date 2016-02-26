#!/usr/bin/env python
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
    for level1 in soup.select('.classify_books'):
        curLevel1 = level1.select('.classify_title')[0].text
        curLevel1 = re.sub('\s', '', curLevel1)
        for level2 in level1.select('.classify_kind'):
            curLevel2 = level2.select('.classify_kind_name')[0].text
            curLevel2 = re.sub('\s', '', curLevel2)
            for level3 in level2.select('ul li a'):
                #curLevel3 = re.sub('\s', '', level3.text)
                curLevel3 =  level3.text.strip()
                curlUrl = level3['href']
                retFind = re.findall(r'\/cp(.*)\.html',curlUrl)
                if retFind:
                    curCatID = retFind[0]
                    catType = 'book'
                else:
                    retFind = re.findall(r'\/cid(.*)\.html',curlUrl)
                    if retFind:
                        curCatID = retFind[0]
                        catType = 'nonbook'
                if retFind:
                    if catDb.find({'catId':curCatID}).count() >0:
                        logger.debug('catetogy %s exists,skip\n'%(curCatID))
                    else:
                        catDb.insert({'catId':curCatID,'level1':curLevel1, 'level2':curLevel2, 'level3':curLevel3, 'catUrl':curlUrl,'catType':catType, 'site':site})
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
        if (global_setting['site']==cat['site']) and cat['catUrl'] and(global_setting['all'] or ((level1Filter and cat['level1'] in level1Filter) \
            or (level2Filter and cat['level2'] in level2Filter) \
            or level3Filter and cat['level3']  in level3Filter)):
            #产品的类别属性
            CATLIST = ('catId','catUrl','site','level1','level2','level3','catType')
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
    try:
        r = session.get(url)
        soup = BeautifulSoup(r.text)
        retSoup = soup.select('.data')
        if retSoup:
            strPage = retSoup[0].select('span')[2].text
        else:
            strPage = soup.select('.page')[0].select('span')[2].text
        pages = int(strPage.split('/')[1])
    except Exception, e:
        logger.Exception('error in getting categoy page num(url:%s),skip,reason:%s' %(url, str(e)))
        pages = -1
    return pages

def parseNonLbTemplate(li):
    product = {}
    product['sku'] = li['id']
    product['url'] = "http://product.dangdang.com/%s.html" %(product['sku'])
    product['name'] = li.select('.name')[0].a.text.strip()
    reBrand = re.findall(u'^(.*?)[\s （]',product['name'])
    if reBrand:
        product['brand'] = re.sub(u'[\[【].*[\]】]','',reBrand[0])
    product['price'] = li.select('.price_n')[0].text
    product['price'] = re.sub(r'[^.\d]','',li.select('.price_n')[0].text)
    return product

def parseLbTemplate(li):
    product = {}

    soup = li.select('.name')[0]
    product['name'] = soup.a.text.strip()
    product['url'] = re.sub(r'#.*$','',soup.a['href'])
    strId = re.findall(r'\/(\d+)\.html',product['url'])
    if strId:
        product['sku'] = strId[0]
    reBrand = re.findall(u'^(.*?)[\s （]',product['name'])
    if reBrand:
        product['brand'] = re.sub(u'[\[【].*[\]】]','',reBrand[0])
    else:
        product['brand'] = ''
    product['price'] = re.sub(r'[^.\d]','',li.select('.price_n')[0].text)
    return product

def getProduct(dbProductList,**cat):
    catTypeSuffix = {'book':'http://category.dangdang.com/pg%d-cp%s.html','nonbook':'http://category.dangdang.com/pg%d-cid%s.html'}
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
            urlPage = catTypeSuffix[cat['catType']] % (page+1, cat['catId'])
            sleepTime = random.random()*global_setting['sleep']+0.1
            time.sleep(sleepTime)
            r = session.get(urlPage)
            soup = BeautifulSoup(r.text)
            listSoup = soup.select('.list_aa.listimg') or soup.select('.list_aa.bigimg')
            isLb = False
            if listSoup:
                lis = listSoup[0].select('li')
            else:
                lis = soup.select('li[name=lb]')
                if lis:
                    isLb = True
            for li in lis:
                product = {}
                product.update(cat)
                if not isLb:
                    product.update(parseNonLbTemplate(li))
                else:
                    product.update(parseLbTemplate(li))
                del product['catUrl']
                del product['catType']
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
    priceUrl = 'http://p.3.cn/prices/mgets?skuIds=J_%s&type=1'
    sleepTime = random.random()*global_setting['sleep']+0.1
    time.sleep(sleepTime)
    strSku = ",J_".join(skuLists)
    r = session.get(priceUrl %(strSku))
    if not r.text:
        return False
    jsonPriceLists = json.loads(r.text)
    for price in jsonPriceLists:
        if price['p'] and price['id']:
            skuid = price['id'].replace('J_','')
            price = price['p']
            curTime = datetime.datetime.utcnow()
            db.update({'sku':skuid},{'$set':{'price':price}})
            db.update({'sku':skuid},{'$push':{'timeline':{'date':curTime,'price':price}}})
    return True

def parseCommandLine():
    para = {}
    options,args = getopt.getopt(sys.argv[1:],"h",['site=', 'level1=', 'level2=', 'level3=', 'host=', 'port=','sleep=', 'database=','productTable=','catTable=','pagesize=', 'hasPrice','batchUpdate', 'hasSpec','delta','all', 'help','catUpdate'])
    for opt, value in options:
        if opt in ['--level1','--level2','--level3']:
            strKey = re.sub('-','',opt)
            para[strKey] = value.decode('gb2312').split(',')
        elif opt in ['--site','--database','--catTable','--productTable']:
            strKey = re.sub('-','',opt)
            para[strKey] = value.decode('gb2312')
        elif opt in ['--host','--port','--pagesize','--sleep']:
            strKey = re.sub('-','',opt)
            para[strKey] = value
        elif opt in ['--hasPrice','--hasSpec','--delta','--batchUpdate','--catUpdate', '--all']:
            strKey = re.sub('-','',opt)
            para[strKey] = True
        if opt in ['-h','--help']:
            usage()
            sys.exit()
    return para

def updateBactch():
    dbProductList = openTable(tableName=global_setting['productTable'])
    updateCatids = getUpdateCat()
    import pdb;pdb.set_trace()
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
        if (global_setting['site']==cat['site']) and cat['catUrl'] and (global_setting['all'] or (level1Filter and cat['level1'] in level1Filter) \
            or (level2Filter and cat['level2'] in level2Filter) \
            or level3Filter and cat['level3']  in level3Filter):
            retCat.append((cat['catId'],cat['level3']))
    return retCat

def usage():
    print "Usage: python getCategory.py [--help] [--site] [--all] [--hasPrice] [--hasSpec] [--homeUrl]  [--host], [--port] [--sleep] [--database] [--productTable] [--catTable] [--level1] [--level2] [--level3] [--delta] [--batchUpdate] [--catUpdate]\n"

global_setting = {}
global session
global logger
if __name__ == '__main__':
    logger = setLog('INFO')
    logger.debug('log level, %d' %(logger.level))
    session = requests.Session()
    retPara = parseCommandLine()
    global_setting['site'] = retPara.get('site',u'当当')
    global_setting['targetUrl'] = retPara.get('homeUrl','http://category.dangdang.com/?ref=www-0-C')
    global_setting['level1'] = retPara.get('level1',None)
    global_setting['level2'] = retPara.get('level2',None)
    global_setting['level3'] = retPara.get('level3',None)
    global_setting['spec'] = retPara.get('hasSpec',False)
    global_setting['price'] = retPara.get('hasPrice',False)
    global_setting['delta'] = retPara.get('delta',False)
    global_setting['host'] = retPara.get('host','127.0.0.1')
    global_setting['port'] = retPara.get('port','27017')
    global_setting['database'] = retPara.get('database','dangdang')
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



