import logging

from api.services.db import HbaseInternals
from api.settings import LOG_LEVEL

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.propagate = True
logger.setLevel(LOG_LEVEL)


class WordCount(HbaseInternals):

    TABLE = 'wordcount'
    _instance = None

    def get_bin(self):
        """
        Returns the current word count bin.

        :rtype int: The bin as a unix timestamp
        """
        return int(self.get_timestamp()/15) * 15

    def store(self, word_counts):
        """
        Takes a dict containing {word => frequency} as input and increments all the records in the database for the
        current bin.

        :param dict( unicode, int ) word_counts: The word counts to record.
        """
        for word, freq in word_counts.iteritems():
            logging.debug("Logging count, {0}, for ".format(freq))
            logging.debug(word)

            try:
                total = self.inc(table=self.TABLE, row_key=word, column_family=str(self.get_bin()), how_much=freq)
                logging.debug("Up to: {0}".format(total))
            except UnicodeEncodeError, e:
                logging.warning("Caught UnicodeEncodeError: {0}".format(e))
                total = self.inc(table=self.TABLE, row_key=word.encode('utf8'), column_family=str(self.get_bin()), how_much=freq)
                logging.debug("Up to: {0}".format(total))

