import time
import re
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
import json
from logHelp import setLog

logger = setLog('INFO')
logger.debug('log level, %d' %(logger.level))

URL='http://club.jd.com/review/907424-1-2-0.html'
session = requests.Session()

def getCommentPages(url):
    rule=re.compile(r'(<div class=\"pagin fr\">.*?<\/div>)',re.S)
    r = session.get(url)
    try:
        listUrls = re.findall(rule,r.text)[0]
        soup = BeautifulSoup(listUrls)
        strPage = soup('a')[-2].text
        pages = int(strPage.replace('.',""))
    except Exception, e:
        logger.exception(str(e))
        pages=-1
    finally:
        return pages

pages = getCommentPages(URL)