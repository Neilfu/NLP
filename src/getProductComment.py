#!/usr/bin/python  
#coding=gbk
import re
import requests
from bs4 import BeautifulSoup
from myHelper import setLog,openTable,progressBar



#解析出当前产品评论的总页数
def getCommentPages(session,url):
    rule=re.compile(r'(<div class=\"pagin fr\">.*?<\/div>)',re.S)
    r = session.get(url)
    try:
        listUrls = re.findall(rule,r.text)[0]
        soup = BeautifulSoup(listUrls)
        strPage = soup('a')[-2].text
        pages = int(strPage.replace('.',""))
    except Exception, e:
        logger.exception(str(e))
        pages=13
    finally:
        return pages

#解析单条评论信息
def getCommentRecord(div):
    commentRecord = {}
    commentRecord['commentStar'] = div.select('.o-topic')[0]('span')[0]['class'][1][2]
    #解析评价时间
    commentRecord['commentDate'] = div.select('.date-comment')[0].a.text.strip()
    #解析用户评价标签<span class="comm-tags" href="#none"><span>性价比高</span></span>
    commentRecord['commentTags'] = [tag.span.text for tag in div.select('.comm-tags')]
    #解析用户评价心得 <dl><dd>比商场便宜多了，非常喜欢！</dd></dl>
    comments = div.select('.comment-content')[0]('dl')[1].dd.text
    commentRecord['comments'] = re.sub(r'[\t\n\r #:]','',comments)
    #解析购买时间
    dlLists = div.select('.dl-extra')[0]('dl')
    commentExtra = {}
    for dl in dlLists:
        title = re.sub(unicode(r'[：　#]'),'',dl.dt.text)
        commentExtra[title] = re.sub(r'[\s\r\n#]','',dl.dd.text)
    commentRecord['commentExtra'] = commentExtra
    #解析用户ID名称
    u_name = div.select('.u-name')[0].a
    commentRecord['commentUsername'] = u_name.text.strip()
    commentRecord['commentUserid'] = re.findall(r'com/(.*).html',u_name['href'])[0]
    return commentRecord 

ruleDiv = re.compile(r'(<div id=\"comments-list\".*?)<div class=\"clearfix\"',re.S)
if __name__ == '__main__':
    logger = setLog('CRITICAL')
    logger.debug('log level, %d' %(logger.level))
    
    URL='http://club.jd.com/review/%s-0-%s-0.html'
    session = requests.Session()
    
    commentFilename ='../comments.txt'
    COMMFILE = open(commentFilename, 'w')
    tblProductList = openTable(dbName='shouji',tableName='productList')

    for product in tblProductList.find({u'操作系统':{'$regex':'Android'}}): 
        try:
            skuid= product['sku']
            COMMFILE.write("@@<<<product skuid:%s>>>\n" %(skuid))
            pages = getCommentPages(session,URL %(skuid,1))
            isFirst = True
            NewestDate = ''
            OldestDate = ''
            if pages > 0 :
                for page in range(pages):
                    progressBar(page,pages)
                    try:
                        r = session.get(URL %(skuid,page+1))
                        listDiv = re.findall(ruleDiv,r.text)[0]
                        soup = BeautifulSoup(listDiv)
                        divLists = soup.select('div[class="mc"]')
                        divCount = 0
                        for div in divLists:
                            try:
                                commentRecord = getCommentRecord(div)
                                lineFormat= u'@#评论时间:%s#星级:%s#用户名:%s#用户号:%s#标签:%s#心得:%s'
                                lineString =  lineFormat %(commentRecord['commentDate'],commentRecord['commentStar'],
                                                           commentRecord['commentUsername'],commentRecord['commentUserid'],
                                                           u','.join(commentRecord['commentTags']),commentRecord['comments'])             
                                for key in commentRecord['commentExtra']:
                                    lineString = lineString + '#%s:%s' %(key,commentRecord['commentExtra'][key])
                                COMMFILE.write(unicode(lineString.encode('gb2312','ignore')))
                                COMMFILE.write('\n')
                                #记录当前解析的位置（时间维度），避免后续重复
                                if isFirst:
                                    NewestDate = commentRecord['commentDate']
                                    isFirst = False
                                OldestDate = commentRecord['commentDate']
                                divCount = divCount + 1
                            except Exception, e:
                                logger.warning("Div sparsing error in page:%d div:%d,cause:%s" %(page,divCount,unicode(e)))
                                continue               
                    except Exception, e:
                        logger.warning("page sparsing error in page:%d cause:%s" %(page,unicode(e)))
                        continue
            #更新产品库的解析状态
            product[u'最新记录'] = NewestDate
            product[u'最早记录'] = OldestDate
            tblProductList.update({u'sku':skuid},product)   
        #except KeyboardInterrupt, e:
        #    print "Interupted!"
        finally:
            COMMFILE.close()      
            
    
            
    


    

#pages = getCommentPages(URL)