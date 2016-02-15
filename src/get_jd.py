#coding=utf-8
import re
import requests
from bs4 import BeautifulSoup
import json
from myHelper import setLog, progressBar,openTable
import time


logger = setLog('INFO')
logger.debug('log level, %d' %(logger.level))


def getCategoryUrl(site="",url=""):
    catDb = openTable(tableName='catdb')
    r = session.get(url)
    if not r.text:
        return False

    soup = BeautifulSoup(r.text)
    for level1 in soup.select('.category-item'):
        curLevel1 = level1.select('.mt')[0].text
        curLevel1 = re.sub('\s', '', curLevel1)

        for level2 in level1.select('dl'):
            curLevel2 = level2.select('dt')[0].text
            curLevel2 = re.sub('\s', '', curLevel2)
            for level3 in level2.select('dd a'):
                curLevel3 = re.sub('\s', '', level3.text)
                curlUrl = level3['href']
                retFind = re.findall(r'=(.*)$',curlUrl)
                if retFind:
                    curCatID = retFind[0]
                    catDb.insert({'catId':curCatID,'level1':curLevel1, 'level2':curLevel2, 'level3':curLevel3, 'catUrl':curlUrl, 'site':site})

def getPidList4Cat(hasSpec=False,hasPrice=True,site='',level1Filter=None, level2Filter=None, level3Filter=None):
    catDb = openTable(tableName='catdb')
    for cat in catDb.find():
        if cat['catUrl'] and ((level1Filter and cat['level1'] in level1Filter) \
            or (level2Filter and cat['level2'] in level2Filter) \
            or level3Filter and cat['level3']  in level3Filter):
            #catUrl = cat['catUrl']
            #getProduct(catUrl,cat['level1'],cat['level2'],cat['level3'])
            paraCat = cat
            del paraCat['_id']
            getProduct(hasSpec, hasPrice,**paraCat)

def getCatPageNum(url):
    r = session.get(url)
    soup = BeautifulSoup(r.text)
    strPages = soup.find('span',attrs={'class':'fp-text'})
    if strPages:
        pages = int(strPages.text.split('/')[1])
    else:
        pages = 0
    return pages


def getProduct(hasSpec=True,hasPrice=True,**cat):
    SUFFIX = '&page=%s&sort=sort_winsdate_desc'
    catUrl = cat['catUrl']
    dbProductList = openTable(tableName='products')
    totalPages = getCatPageNum(catUrl)
    rule = re.compile(r'id=\"plist\".*?>(.*?)<div class=\"clr\"',re.S)
    for page in range(totalPages):
        try:
            progressBar("getting pages",page,totalPages)
            urlPage = catUrl + SUFFIX
            time.sleep(0.5)
            r = session.get(urlPage %(page+1))
            listUls = re.findall(rule,r.text)
            soup = BeautifulSoup(listUls[0])
            skuLists=[]
            for li in soup.select('.gl-item'):
                product = {}
                product.update(cat)
                product['sku'] = li.find(attrs={"data-sku":True})['data-sku']
                skuLists.append(product['sku'])
                product['url'] = li.select("div > a")[0]['href']
                product['name'] = li.select('.p-name')[0].a.em.text
                reBrand = re.findall(r'^(.*?)[\s（]',product['name'])
                if reBrand:
                    product['brand'] = reBrand[0]
                try:
                    if dbProductList.find({u'sku':product['sku']}).count() >0:
                        logger.debug('%s exist,skip' %(product['sku']))
                        continue
                    dbProductList.insert(product)
                    if hasSpec:
                        getProductDetail(product['sku'],product['url'],dbProductList)
                except Exception, e:
                    logger.exception("error in Page:%d, skuid:%s, reason:%s" %(page, product['sku'], str(e)))
                    continue

            if hasPrice:
                updatePrice(skuLists,dbProductList)
        except (KeyboardInterrupt, SystemExit), e:
            logger.critical("app is interrupted, finished pages:%d" %(page))
            break
        except Exception,e:
            logger.exception("error in Page:%d, reason:%s" %(page,str(e)))

def getProductDetail(sku, url, db):
    tableRule=re.compile(r'<table.*?class="Ptable".*?<\/table>',re.S)
    if not url:
        return False
    productDetail = {}
    time.sleep(0.5)
    r = session.get(url)
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
    return True

def updatePrice(skuLists,db):
    priceUrl = 'http://p.3.cn/prices/mgets?skuIds=J_%s&type=1'
    time.sleep(0.5)
    strSku = ",J_".join(skuLists)
    r = session.get(priceUrl %(strSku))
    if not r.text:
        return False
    jsonPriceLists = json.loads(r.text)
    for price in jsonPriceLists:
        if price['p'] and price['id']:
            db.update({'sku':price['id'].replace('J_','')},{'$set':{'price':price['p']}})
    return True

if __name__ == '__main__':

    session = requests.Session()
    logger = setLog('INFO')
    #getCategoryFile('./jd_index.html')
    getCategoryUrl(u'京东','http://www.jd.com/allSort.aspx')
    getPidList4Cat(hasSpec=False,hasPrice=True,site='京东',level1Filter=[u'数码'])