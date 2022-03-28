import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'\b[a-zA-Z]{4,}\b')


class MRMostUsedWords(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def mapper_get_words(self, _, line):
        try:
            if line is not None:
                matches = WORD_RE.findall(line)
                for word in matches:
                    yield (len(word), word.lower()), 1
        except:
            pass

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        yield word[0], (word[1], sum(counts))

    def reducer_find_max_word(self, length, word_count_pairs):
        max_word_count_pairs = max(word_count_pairs, key=lambda x: x[1])
        yield str(length), max_word_count_pairs[0] + "\t" + str(max_word_count_pairs[1])

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]


if __name__ == "__main__":
    MRMostUsedWords.run()
