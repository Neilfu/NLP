#coding=gb2312
import sys     
import logging
from pymongo import MongoClient

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
        
def openTable(dbName='shouji',tableName='productList'):       
    con = MongoClient()
    db = con[dbName]
    return db[tableName]  


    

class CommentRecord():
    def __init__(self,fileName='..\comments.txt'):
        self.FILE = open(fileName,'a')
    
    def writeproductHead(self,strSkuid):
        self.FILE.write("\n@@<<<product skuid:%s>>>\n" %(strSkuid))
    
    def writeCommentsRecord(self,dictRecord):
        lineFormat= u'@#评论时间:%s#星级:%s#用户名:%s#用户号:%s#标签:%s#心得:%s'
        lineString =  lineFormat %(dictRecord['commentDate'],dictRecord['commentStar'],
                           dictRecord['commentUsername'],dictRecord['commentUserid'],
                           u','.join(dictRecord['commentTags']),dictRecord['comments'])             
        for key in dictRecord['commentExtra']:
            lineString = lineString + '#%s:%s' %(key,dictRecord['commentExtra'][key])
        self.FILE.write(unicode(lineString.encode('gb2312','ignore')))        
        self.FILE.write('\n')
    
    def flushCommentsRecord(self):
        self.FILE.flush()
    
    def __del__(self):
        self.FILE.close()
        
        
    
    