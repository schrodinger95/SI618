'''
An old version was created by Dr. Yuhang Wang. Solutions created by Josh Gardner.
'''

import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r'\b[A-Za-z]{4,15}\b')


class WordCounter(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def mapper(self, _, line):
        try:
            # # +++your code here+++
            bill_type = line.split('\t')[1]
            title = line.split('\t')[2]
            if title is not None:
                matches = WORD_RE.findall(title)
                # # +++your code here+++
                for word in matches:
                    yield (bill_type, word.lower()), 1
        except:
            pass

    def combiner(self, key, counts):
        # +++your code here+++
        yield key, sum(counts)

    def reducer(self, key, counts):
        # +++your code here+++
        yield key[0] + "\t" + key[1], str(sum(counts))
      

if __name__ == '__main__':
    WordCounter.run()
