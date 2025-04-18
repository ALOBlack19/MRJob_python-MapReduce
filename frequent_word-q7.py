from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\b\w+\b")
# Used again to extract each word, ignoring commas, dots, etc.

class MRMostFrequentWords(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1  # Emit each word (in lowercase)

    def reducer_init(self):
        self.max_count = 0
        self.freq_words = []

    def reducer(self, word, counts):
        total = sum(counts)
        if total > self.max_count:
            self.max_count = total
            self.freq_words = [word]
        elif total == self.max_count:
            self.freq_words.append(word)

    def reducer_final(self):
        for word in self.freq_words:
            yield word, self.max_count

if __name__ == '__main__':
    MRMostFrequentWords.run()