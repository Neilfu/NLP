#coding=gb2312
import sys, getopt
from myHelper import openTable

def clearTimeline(strFilter='all'):
    tblProductList = openTable()
    if strFilter =='all':
        dictFilter = {'$or':[{u'���¼�¼':{'$exists':'true'}},{u'�����¼':{'$exists':'true'}}]}
    else:
        dictFilter = {'sku':strFilter}
    count = 0
    for product in tblProductList.find(dictFilter):
        flag = False
        if u'���¼�¼' in product:
            del product[u'���¼�¼']
            flag = True
        if u'�����¼' in product:    
            del product[u'�����¼']
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
