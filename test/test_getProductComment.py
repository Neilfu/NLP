import unittest
import src.getProductComment as Comment

class TestGetProductComment(unittest.TestCase):
    def setUp(self):
        self.url = 'http://club.jd.com/review/907424-1-2-0.html'
    
    def test_getCommentPages(self):
        pages = Comment.getCommentPages(self.url)
        self.assertEqual(pages, 365)

if __name__ == '__main__':
    unittest.main()