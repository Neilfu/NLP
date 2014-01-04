#coding=gb2312
import re
import requests
from bs4 import BeautifulSoup
import json
from myHelper import setLog, progressBar,openTable
  
tableRule=re.compile(r'<table.*?class="Ptable".*?<\/table>',re.S)
URL='http://list.jd.com/9987-653-655-0-0-0-0-0-0-0-1-5-%s-1-19-1601-3633-0.html'
priceUrl = 'http://p.3.cn/prices/mgets?skuIds=%s&type=1'
logger = setLog('INFO')
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
    table = re.findall(tableRule,r.text)[0]
    if not table:
        return productDetail
    soup = BeautifulSoup(table)
    trs = soup('tr')
    for tr in trs:
        if len(tr('td')) == 2:
            product[tr('td')[0].text.replace('.','')] = tr('td')[1].text
    return productDetail
    
 

 
dbProductList = openTable(dbName='shouji',tableName='productList')
session = requests.Session()
totalPages=getPageNum() 

rule = re.compile(r'id=\"plist\".*?>(.*?)<\/div>\s+<script',re.S)
for page in range(totalPages):
    try:
        progressBar("getting pages",page,totalPages)
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

        
        