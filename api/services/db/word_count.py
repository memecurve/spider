from api.services.db import HbaseInternals

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
            try:
                self.inc(table=self.TABLE, row_key=word, column_family=str(self.get_bin()), how_much=freq)
            except UnicodeEncodeError, e:
                self.inc(table=self.TABLE, row_key=word.encode('utf8'), column_family=str(self.get_bin()), how_much=freq)

