#coding=gb2312
import sys, getopt
from myHelper import openTable

def clearTimeline(strFilter='all'):
    tblProductList = openTable()
    if strFilter =='all':
        dictFilter = {'$or':[{u'最新记录':{'$exists':'true'}},{u'最早记录':{'$exists':'true'}}]}
    else:
        dictFilter = {'sku':strFilter}
    count = 0
    for product in tblProductList.find(dictFilter):
        flag = False
        if u'最新记录' in product:
            del product[u'最新记录']
            flag = True
        if u'最早记录' in product:    
            del product[u'最早记录']
            flag = True
        if flag:
            tblProductList.update({u'sku':product['sku']},product)
            count = count + 1
    return count

if __name__ == '__main__':
    
    options,args = getopt.getopt(sys.argv[1:],"hc:")
    for opt,value in options:
        if opt == '-c' and value is not None:
            count = clearTimeline(value)
            print 'clear count:%d' %(count)
        elif opt == '-h':
            print 'usage: %s -c [skuid or all]' %(sys.argv[0])
        else:
            print 'command line error'
