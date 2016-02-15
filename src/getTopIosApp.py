#coding=utf-8
import re
import sys, getopt
import requests
from bs4 import BeautifulSoup
import logging
from pymongo import MongoClient
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
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
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

def getCatList(topN=30):
    targetURL = 'https://itunes.apple.com/cn/genre/ios-cai-wu/id6015?mt=8'
    r = session.get(targetURL)
    if not r.text:
        return None
    soup = BeautifulSoup(r.text)
    div = soup.select('#genre-nav')
    retList = {}
    if div:
        urlList = div[0].select('li')
        for li in urlList:
            url = li.a['href']
            name = li.a.text
            retList.update({name:url})
    return retList

def getTopNApp(catname='', catUrl=''):
    r = session.get(catUrl)
    if not r.text:
        return None

    soup = BeautifulSoup(r.text)
    div = soup.select('#selectedcontent')
    retList = []
    if div:
        urlList = div[0].select('li')
        for li in urlList:
            url = li.a['href']
            reId =re.findall(r'\/(id.*)\?',url)
            if reId:
                appid = reId[0]
            name = li.a.text
            retList.append((appid,name))
    return retList

def dumpApplist(savePath='./iostopnapp.csv',topN=30,**appList):
    WFILE = open(savePath, 'w')
    WFILE.write("no\tcategogy\tappname\tappid\n")
    for cat in appList:
        cnt = 1
        for app in appList[cat]:
            line = '%d\t%s\t%s\t%s\n'%(cnt,cat,app[0],app[1])
            WFILE.write(line.encode('utf-8'))
            cnt = cnt +1
            if cnt >topN:
                break
    WFILE.close()

global session
global logger
if __name__ == '__main__':
    logger = setLog('INFO')
    session = requests.Session()
    catList = getCatList()
    appList = {}
    for cat in catList:
        logger.info('begin getting %s:\n' %(cat))
        try:
            list1Cat = getTopNApp(cat, catList[cat])
            appList[cat] = list1Cat
        except Exception,e:
            logger.exception("error in get %s, reason:%s" %(cat, str(e)))
            continue
    dumpApplist(topN=50,**appList)
