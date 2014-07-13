import unittest
from api import canonicalize

class TestApiCore(unittest.TestCase):

    def test_canonicalize(self):
        self.assertEquals(canonicalize('http://www.buzzfeed.com/user-name/post-slug?utm_term=foobar&b=2&a=1#bizbaz'),
                          'http://www.buzzfeed.com/user-name/post-slug?a=1&b=2')
        self.assertEquals(canonicalize('http://www.buzzfeed.com/'),
                          'http://www.buzzfeed.com/')
        self.assertEquals(canonicalize('http://www.buzzfeed.com/user-name/post-slug'),
                          'http://www.buzzfeed.com/user-name/post-slug')
        self.assertEquals(canonicalize('http://www.buzzfeed.com/user-name/post-slug?a=1&b=2'),
                          'http://www.buzzfeed.com/user-name/post-slug?a=1&b=2')
        self.assertEquals(canonicalize('http://www.buzzfeed.com/user-name/post-slug?b=2&a=1'),
                          'http://www.buzzfeed.com/user-name/post-slug?a=1&b=2')
        self.assertEquals(canonicalize('http://www.buzzfeed.com/user-name/post-slug?b=2&a=1&utm_term=foobar'),
                          'http://www.buzzfeed.com/user-name/post-slug?a=1&b=2')
        self.assertEquals(canonicalize('http://www.buzzfeed.com/user-name/post-slug?utm_term=foobar&b=2&a=1'),
                          'http://www.buzzfeed.com/user-name/post-slug?a=1&b=2')
        self.assertEquals(canonicalize('http://www.buzzfeed.com/user-name/post-slug?utm_term=foobar&b=2&a=1#bizbaz'),
                          'http://www.buzzfeed.com/user-name/post-slug?a=1&b=2')
        with self.assertRaises(ValueError):
            canonicalize('/user-name/post-slug')
