#coding=gb2312
import re
import time
import requests
from bs4 import BeautifulSoup
from myHelper import setLog,openTable,progressBar,CommentRecord



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
        pages=1
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
    logger = setLog('INFO')
    logger.debug('log level, %d' %(logger.level))
    
    URL='http://club.jd.com/review/%s-0-%s-0.html'
    session = requests.Session()
    
    commentFilename ='../comments.txt'
    myCommentRecord = CommentRecord(commentFilename)
    
    
    #COMMFILE = open(commentFilename, 'w')
    tblProductList = openTable(dbName='shouji',tableName='productList')

    for product in tblProductList.find({u'操作系统':{'$regex':'Android'}}): 
        try:
            skuid= product['sku']
            
            #firstDate：上次获取到的最新评论时间，lastDate：上次获取到的最远评论时间
            if u'最新记录' in product:
                firstDate = time.strptime(product[u'最新记录'],'%Y-%m-%d %H:%M')
            else:
                firstDate = time.localtime(time.time())
            
            if u'最早记录' in product:
                lastDate = time.strptime(product[u'最早记录'],'%Y-%m-%d %H:%M')
            else:
                lastDate = firstDate
            #NewestDate：本次解析获取的评论最新时间，OldestDate：本次获取到的最远评论时间
            NewestDate = firstDate
            OldestDate = lastDate                            
            
            myCommentRecord.writeproductHead(skuid)   
            #COMMFILE.write("@@<<<product skuid:%s>>>\n" %(skuid))
            pages = getCommentPages(session,URL %(skuid,1))
            isFirst = True

            if pages > 0 :
                for page in range(pages):
                    progressBar('getting product %s' %(skuid),page+1,pages)
                    try:
                        r = session.get(URL %(skuid,page+1))
                        listDiv = re.findall(ruleDiv,r.text)[0]
                        soup = BeautifulSoup(listDiv)
                        divLists = soup.select('div[class="mc"]')
                        divCount = 0
                        for div in divLists:
                            divCount = divCount + 1
                            try:
                                commentRecord = getCommentRecord(div)
                                #如果评论时间在【lastDate，firstDate】之间，说明已经获取过，跳过
                                currentRecordDate = time.strptime(commentRecord['commentDate'],'%Y-%m-%d %H:%M')
                                if currentRecordDate <= NewestDate and currentRecordDate >  OldestDate:
                                    logger.info('\nskip! product:%s, page:%s, currentRecordDate:%s,NewestDate:%s,OldestDate:%s' 
                                                 %(skuid, page, time.strftime('%Y-%m-%d %H:%M', currentRecordDate),
                                                   time.strftime('%Y-%m-%d %H:%M', NewestDate),
                                                   time.strftime('%Y-%m-%d %H:%M', OldestDate)
                                                   )
                                                )
                                    continue
                                elif currentRecordDate > NewestDate:
                                    NewestDate = time.strptime(commentRecord['commentDate'],'%Y-%m-%d %H:%M')
                                else:
                                    OldestDate = time.strptime(commentRecord['commentDate'],'%Y-%m-%d %H:%M')
                                myCommentRecord.writeCommentsRecord(commentRecord)
                                
                            except Exception, e:
                                logger.warning("Div sparsing error in page:%d div:%d,cause:%s" %(page,divCount,unicode(e)))
                                continue               
                    except Exception, e:
                        logger.warning("page sparsing error in page:%d cause:%s" %(page,unicode(e)))
                        continue
        finally:   
            #更新产品库的解析状态
            product[u'最新记录'] = time.strftime('%Y-%m-%d %H:%M', NewestDate)
            product[u'最早记录'] = time.strftime('%Y-%m-%d %H:%M', OldestDate)
            tblProductList.update({u'sku':skuid},product)
            myCommentRecord.flushCommentsRecord()
    
            
    


    

#pages = getCommentPages(URL)