#coding=gb2312
import unittest
import pickle
import src.myHelper as helper

class TestMyHelper(unittest.TestCase):
    def setUp(self):
        self.fileName = 'data/commenttest.txt'
        RFILE = open('data/commentRecord.dump','r')
        self.dictCommentRecord = pickle.load(RFILE)
        RFILE.close()
        self.strRecord=u'@#����ʱ��:2013-12-30 11:27#�Ǽ�:5#�û���:jd_79d2d0113a95b#�û���:1718466#��ǩ:�ٶȿ�,ϵͳ����,��Ư��#�ĵ�:�����ǳ��ã�spenʹ��Ҳ�ܷ��㣬�ǳ�����~~#��������:2013-10-19#��ɫ:�غ�ɫ#�汾:3Gͨ���汾16G\n'
    
    def test_CommentRecord(self):
        myCommentRecord = helper.CommentRecord(self.fileName)
        myCommentRecord.writeproductHead('860612')
        myCommentRecord.writeCommentsRecord(self.dictCommentRecord)
        
        retFile= open(self.fileName,'r')
        strLines = retFile.readlines()
        self.assertEqual(strLines[0], '@@<<<product skuid:860612>>>\n', 'head error')
        self.assertEqual(strLines[1].decode('gb2312'), self.strRecord, 'record error')

if __name__ == '__main__':
    unittest.main()

