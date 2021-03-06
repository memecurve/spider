import logging

import thrift

from api.services.db import HbaseInternals
from api.settings import LOG_LEVEL

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.propagate = True
logger.setLevel(LOG_LEVEL)


class WordCount(object):

    def __init__(self):
        self.i = HbaseInternals()

    TABLE = 'wordcount'

    def get_bin(self):
        """
        Returns the current word count bin.

        :rtype int: The bin as a unix timestamp
        """
        return int(self.i.get_timestamp()/15) * 15

    def store(self, word_counts):
        """
        Takes a dict containing {word => frequency} as input and increments all the records in the database for the
        current bin.

        :param dict( unicode, int ) word_counts: The word counts to record.
        """
        logging.debug("Called store. {0}".format(word_counts))
        for word, freq in word_counts.iteritems():
            logging.debug("Logging count, {0}, for ".format(freq))
            logging.debug(word)
            logger.debug(u"Storing {0} counts of {1}".format(freq, word))
            logger.debug("Calling inc...")
            try:
                total = self.i.inc(self.TABLE, word.encode('utf_16_be'), 'bin:{0}'.format(self.get_bin()), how_much=int(freq))
            except thrift.Thrift.TApplicationException, e:
                logger.warning("Couldn't increment for word: {0}".format(e))


