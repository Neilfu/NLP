#coding=gb2312
import re
import requests
from bs4 import BeautifulSoup
import json
from myHelper import setLog, progressBar,openTable
import time
  
tableRule=re.compile(r'<table.*?class="Ptable".*?<\/table>',re.S)
URL='http://list.jd.com/list.html?cat=9987,653,655&page=%s&sort=sort_winsdate_desc&go=0'
priceUrl = 'http://p.3.cn/prices/mgets?skuIds=%s&type=1'
logger = setLog('INFO')
logger.debug('log level, %d' %(logger.level))

def getPageNum(no=1):
    r = session.get(URL %(no))
    soup = BeautifulSoup(r.text)
    strPages = soup.find('span',attrs={'class':'fp-text'})
    if strPages:
        pages = int(strPages.text.split('/')[1])
    else:
        pages = 0
    return pages

def updatePrice(skuLists):
    strSku = ",J_".join(skuLists)
    r = session.get(priceUrl %(strSku))
    if not r.text:
        return False
    jsonPriceLists = json.loads(r.text)
    for price in jsonPriceLists:
        if price['p'] and price['id']:
            dbProductList.update({'sku':price['id'].replace('J_','')},{'$set':{'price':price['p']}})  
    return True
    
def getProductDetail(url):
    productDetail = {}
    if not url:
        return productDetail
    time.sleep(0.02)
    r = session.get(url)
    table = re.findall(tableRule,r.text)[0]
    if not table:
        return productDetail
    soup = BeautifulSoup(table)
    trs = soup('tr')
    for tr in trs:
        if len(tr('td')) == 2:
            product[tr('td')[0].text.replace('.','')] = tr('td')[1].text
            #product[tr('td')[0].text.replace('.','').encode('utf-8')] = tr('td')[1].text.encode('utf-8')
    return productDetail
    
def getFieldNames():
    fieldNames = set()
    for row in dbProductList.find():
        fieldNames.update(row.keys())
    fieldNames.remove('')
    return fieldNames

def dumpProduct():
    fieldHeaders = list(getFieldNames())
    DUMPFILE = open('d:/product.csv','w')
    DUMPFILE.write('\t'.join(fieldHeaders).encode('utf-8')+'\n')
    for row in dbProductList.find():
        line = []
        for field in fieldHeaders:
            if field == u'_id':
                line.append(str(row.get(field,'')))
            else:
                line.append(row.get(field,''))
        DUMPFILE.write( '\t'.join(line).encode('utf-8')+'\n')
    DUMPFILE.close()


dbProductList = openTable(dbName='shouji',tableName='productList')
session = requests.Session()
totalPages=getPageNum() 

rule = re.compile(r'id=\"plist\".*?>(.*?)<div class=\"clr\"',re.S)
for page in range(totalPages):
    try:
        progressBar("getting pages",page,totalPages)
        r = session.get(URL %(page+1))
        listUls = re.findall(rule,r.text)
        product={}
        soup = BeautifulSoup(listUls[0])
        skuLists=[]
        for li in soup.select('.gl-item'):
            product={}
            product['sku'] = li.find(attrs={"data-sku":True})['data-sku']
            skuLists.append(product['sku'])
            product['url'] = li.select("div > a")[0]['href']
            try:
                if dbProductList.find({u'sku':product['sku']}).count() >0:
                    logger.debug('%s exist,skip' %(product['sku']))
                    continue
                product.update(getProductDetail(product['url']))
                dbProductList.insert(product)
            except Exception, e:
                logger.exception("error in Page:%d, skuid:%s, reason:%s" %(page, product['sku'], str(e)))
                continue            
        updatePrice(skuLists)

    except (KeyboardInterrupt, SystemExit), e:
        logger.critical("app is interrupted, finished pages:%d" %(page))
        break
    except Exception,e:
        logger.exception("error in Page:%d, reason:%s" %(page,str(e)))


        