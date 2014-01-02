#coding=gb2312
import re
import time
import requests
from bs4 import BeautifulSoup
from myHelper import setLog,openTable,progressBar,CommentRecord



#��������ǰ��Ʒ���۵���ҳ��
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

#��������������Ϣ
def getCommentRecord(div):
    commentRecord = {}
    commentRecord['commentStar'] = div.select('.o-topic')[0]('span')[0]['class'][1][2]
    #��������ʱ��
    commentRecord['commentDate'] = div.select('.date-comment')[0].a.text.strip()
    #�����û����۱�ǩ<span class="comm-tags" href="#none"><span>�Լ۱ȸ�</span></span>
    commentRecord['commentTags'] = [tag.span.text for tag in div.select('.comm-tags')]
    #�����û������ĵ� <dl><dd>���̳����˶��ˣ��ǳ�ϲ����</dd></dl>
    comments = div.select('.comment-content')[0]('dl')[1].dd.text
    commentRecord['comments'] = re.sub(r'[\t\n\r #:]','',comments)
    #��������ʱ��
    dlLists = div.select('.dl-extra')[0]('dl')
    commentExtra = {}
    for dl in dlLists:
        title = re.sub(unicode(r'[����#]'),'',dl.dt.text)
        commentExtra[title] = re.sub(r'[\s\r\n#]','',dl.dd.text)
    commentRecord['commentExtra'] = commentExtra
    #�����û�ID����
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

    for product in tblProductList.find({u'����ϵͳ':{'$regex':'Android'}}): 
        try:
            skuid= product['sku']
            
            #firstDate���ϴλ�ȡ������������ʱ�䣬lastDate���ϴλ�ȡ������Զ����ʱ��
            if u'���¼�¼' in product:
                firstDate = time.strptime(product[u'���¼�¼'],'%Y-%m-%d %H:%M')
            else:
                firstDate = time.localtime(time.time())
            
            if u'�����¼' in product:
                lastDate = time.strptime(product[u'�����¼'],'%Y-%m-%d %H:%M')
            else:
                lastDate = firstDate
            #NewestDate�����ν�����ȡ����������ʱ�䣬OldestDate�����λ�ȡ������Զ����ʱ��
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
                                #�������ʱ���ڡ�lastDate��firstDate��֮�䣬˵���Ѿ���ȡ��������
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
            #���²�Ʒ��Ľ���״̬
            product[u'���¼�¼'] = time.strftime('%Y-%m-%d %H:%M', NewestDate)
            product[u'�����¼'] = time.strftime('%Y-%m-%d %H:%M', OldestDate)
            tblProductList.update({u'sku':skuid},product)
            myCommentRecord.flushCommentsRecord()
    
            
    


    

#pages = getCommentPages(URL)