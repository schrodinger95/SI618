import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r'\b[a-zA-Z]{4,}\b')


class MRMostUsedWords(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def mapper(self, _, line):
        try:
            year = line.split('\t')[5]
            if line is not None:
                matches = WORD_RE.findall(line)
                for word in matches:
                    yield year + "\t" + word.lower(), 1
        except:
            pass

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        yield key, str(sum(counts))


if __name__ == "__main__":
    MRMostUsedWords.run()
