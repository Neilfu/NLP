#coding=utf-8
from myHelper import setLog, openTable

def getFieldNames():
    fieldNames = set()
    for row in dbProductList.find():
        fieldNames.update(row.keys())
    fieldNames.remove('')
    return fieldNames

def dumpProduct():
    #fieldHeaders = list(getFieldNames())
    fieldHeaders = [u'品牌',u'sku',u'_id',u'型号',u'机身内存',u'运行内存',u'操作系统版本',u'CPU品牌',u'CPU频率',u'CPU型号',u'CPU核数',u'GPU',u'触摸屏',u'屏幕材质',u'屏幕尺寸',u'分辨率',u'前置摄像头',u'后置摄像头',u'自动对焦',u'闪光灯',u'price',u'上市年份',u'上市月份',u'机身材质',u'机身重量（g）',u'机身尺寸（mm）',u'电池容量（mAh）',u'理论待机时间（小时）',u'理论通话时间（小时）',u'电池类型',u'电池更换',u'双卡机类型',u'Wi-Fi',u'3G网络制式',u'4G网络制式',u'运营商标志或内容',u'url',u'NFC(近场通讯)',u'智能机',u'超大字体']
    DUMPFILE = open('d:/product_head.csv','w')
    DUMPFILE.write('\t'.join(fieldHeaders).encode('utf-8')+'\n')
    for row in dbProductList.find():
        line = []
        for field in fieldHeaders:
            if field == u'_id':
                line.append(str(row.get(field,'')))
            else:
                line.append(row.get(field,''))
        DUMPFILE.write( '\t'.join(line).encode('utf-8')+'\n')

    DUMPFILE.close()

if __name__ == '__main__':
    logger = setLog('INFO')
    logger.debug('log level, %d' %(logger.level))
    dbProductList = openTable(dbName='shouji',tableName='productList')
    dumpProduct()