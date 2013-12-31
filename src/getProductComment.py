#!/usr/bin/python  
#coding=gbk
import re
import requests
from bs4 import BeautifulSoup
from myHelper import setLog,openTable,progressBar



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
        pages=13
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
    logger = setLog('CRITICAL')
    logger.debug('log level, %d' %(logger.level))
    
    URL='http://club.jd.com/review/%s-0-%s-0.html'
    session = requests.Session()
    
    commentFilename ='../comments.txt'
    COMMFILE = open(commentFilename, 'w')
    tblProductList = openTable(dbName='shouji',tableName='productList')

    for product in tblProductList.find({u'����ϵͳ':{'$regex':'Android'}}): 
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
                                lineFormat= u'@#����ʱ��:%s#�Ǽ�:%s#�û���:%s#�û���:%s#��ǩ:%s#�ĵ�:%s'
                                lineString =  lineFormat %(commentRecord['commentDate'],commentRecord['commentStar'],
                                                           commentRecord['commentUsername'],commentRecord['commentUserid'],
                                                           u','.join(commentRecord['commentTags']),commentRecord['comments'])             
                                for key in commentRecord['commentExtra']:
                                    lineString = lineString + '#%s:%s' %(key,commentRecord['commentExtra'][key])
                                COMMFILE.write(unicode(lineString.encode('gb2312','ignore')))
                                COMMFILE.write('\n')
                                #��¼��ǰ������λ�ã�ʱ��ά�ȣ�����������ظ�
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
            #���²�Ʒ��Ľ���״̬
            product[u'���¼�¼'] = NewestDate
            product[u'�����¼'] = OldestDate
            tblProductList.update({u'sku':skuid},product)   
        #except KeyboardInterrupt, e:
        #    print "Interupted!"
        finally:
            COMMFILE.close()      
            
    
            
    


    

#pages = getCommentPages(URL)