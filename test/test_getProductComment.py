#coding=gb2312
import unittest
import pickle
import src.getProductComment as Comment

class TestGetProductComment(unittest.TestCase):
    def setUp(self):
        self.url = 'http://club.jd.com/review/907424-1-2-0.html'
        self.filename = 'data/860612.html'
        self.ruleDiv = Comment.ruleDiv
        self.session = Comment.requests.Session()
    
    def test_getCommentPages(self):
        pages = Comment.getCommentPages(self.session,self.url)
        self.assertEqual(pages, 377)
    
    def test_getCommentRecord(self):
        htmlFile = open(self.filename,'r')
        bufferHtml = htmlFile.read().decode('gb2312')
        listDiv = Comment.re.findall(self.ruleDiv, bufferHtml)[0]
        soup = Comment.BeautifulSoup(listDiv)
        div = soup.select('div[class="mc"]')[0]
        commentRecord = Comment.getCommentRecord(div)
        self.assertEqual(commentRecord['commentUsername'], 'jd_79d2d0113a95b',u'commentUsername error')
        self.assertEqual(commentRecord['commentUserid'], '1718466','commentUserid')
        self.assertEqual(commentRecord['commentStar'], '5','commentStar error')
        self.assertEqual(commentRecord['commentDate'], u'2013-12-30 11:27', 'commentDate error ')
        self.assertDictEqual(commentRecord['commentExtra'],{u'��������':u'2013-10-19',u'��ɫ':u'�غ�ɫ',u'�汾':u'3Gͨ���汾16G'}, 'commentExtra error')
        self.assertListEqual(commentRecord['commentTags'], [u'�ٶȿ�',u'ϵͳ����',u'��Ư��'], 'commentTags error')
        
        WFILE = open('data/commentRecord.dump','w')
        pickle.dump(commentRecord, WFILE)
        WFILE.close()
 
if __name__ == '__main__':
    unittest.main()

