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
        self.strRecord=u'@#评论时间:2013-12-30 11:27#星级:5#用户名:jd_79d2d0113a95b#用户号:1718466#标签:速度快,系统流畅,很漂亮#心得:东西非常好，spen使用也很方便，非常不错~~#购买日期:2013-10-19#颜色:棕黑色#版本:3G通话版本16G\n'
    
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

