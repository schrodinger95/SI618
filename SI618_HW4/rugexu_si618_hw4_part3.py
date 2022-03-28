import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'\b[a-zA-Z]{4,}\b')


class MRMostUsedWords(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def mapper_get_words(self, _, line):
        try:
            year = line.split('\t')[5]
            if line is not None:
                matches = WORD_RE.findall(line)
                for word in matches:
                    yield (year, word.lower()), 1
        except:
            pass

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        yield word[0], (word[1], sum(counts))

    def reducer_find_max_word(self, year, word_count_pairs):
        sorted_word_count_pairs = sorted(word_count_pairs, reverse=True, key=lambda x: x[1])
        for word_count_pair in sorted_word_count_pairs:
            yield str(year), word_count_pair[0] + "\t" + str(word_count_pair[1])

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]


if __name__ == "__main__":
    MRMostUsedWords.run()
