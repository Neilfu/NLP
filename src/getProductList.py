import time
import re
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
import logging
import json

import sys     
def progressBar(num=1, total=100, bar_word=".", sep= 1 ):
    stars=('-','\\','|','/')
    rate = int(float(num) / float(total)*1000)
    if rate % sep == 0:
        sys.stdout.write(str(rate/10.0)+'%  '+stars[(rate / sep) % len(stars)]+"\r")
        sys.stdout.flush()
 
    
tableRule=re.compile(r'<table.*?class="Ptable".*?<\/table>',re.S)
URL='http://list.jd.com/9987-653-655-0-0-0-0-0-0-0-1-5-%s-1-19-1601-3633-0.html'
priceUrl = 'http://p.3.cn/prices/mgets?skuIds=%s&type=1'

def setLog(level=logging.INFO,logfile='../log.txt'):

    logging.basicConfig(
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                )
    logger = logging.getLogger()
    handler = logging.FileHandler(logfile)
    logger.addHandler(handler)
    console = logging.StreamHandler()
    #logger.addHandler(console)
    logger.setLevel(level)
    return logger

logger = setLog(level=logging.ERROR)
logger.debug('log level, %d' %(logger.level))

def getPageNum(no=1):
    r = session.get(URL %(no))
    soup = BeautifulSoup(r.text)
    strPages = soup.find('span',attrs={'class':'text'})
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
    r = session.get(url)
    logger.info('finish getting %s' %(url))
    table = re.findall(tableRule,r.text)[0]
    if not table:
        return productDetail
    soup = BeautifulSoup(table)
    trs = soup('tr')
    for tr in trs:
        if len(tr('td')) == 2:
            product[tr('td')[0].text.replace('.','')] = tr('td')[1].text
    return productDetail
    
        
    
    
 

con = MongoClient()
db = con['shouji']
dbProductList = db['productList']

startT = time.time()
session = requests.Session()
totalPages=getPageNum() 

rule = re.compile(r'id=\"plist\".*?>(.*?)<\/div>\s+<script',re.S)

for page in range(totalPages):
    try:
        progressBar(page,totalPages)
        r = session.get(URL %(page+1))
        listUls = re.findall(rule,r.text)
        product={}
        soup = BeautifulSoup(listUls[0])
        skuLists=[]
        for li in soup('li'):
            product={}
            product['sku'] = li['sku']
            skuLists.append(product['sku'])
            product['url'] = li.select('.p-name')[0].a['href']
            try:
                product.update(getProductDetail(product['url']))
                if dbProductList.find({u'sku':product['sku']}).count() >0:
                    logger.debug('%s exist,skip' %(product['sku']))
                    continue   
                dbProductList.insert()   
            except Exception, e:
                logger.exception("error in Page:%d, skuid:%s, reason:%s" %(page, product['sku'], str(e)))
                continue            
        updatePrice(skuLists)
    except:
        logger.exception("error in Page:%d, reason:%s" %(page,str(e)))

        
        