# -*- coding: utf-8 -*-
# This line specifies the encoding of the file.
# Allows the file to contain non-ASCII characters. Example: "#" "@" "!" etc.
from mrjob.job import MRJob

# 3. Output the longest word(s) in the input file. If there are multiple words with the same length, output them all.


with open('long_word.txt', 'w') as f:
    f.write("""
Sometimes supercalifragilisticexpialidocious is just a word people
remember.
However, pseudopseudohypoparathyroidism is another long one!
""")

class MRLongestWord(MRJob):
    def mapper(self, _, line):
        # Split the line into words
        words = line.split()
        for word in words:
            # Bring each word with its length
            yield len(word), word

    def reducer(self, length, words):
        # Find the longest word(s)
        longest_words = list(words)
        yield length, longest_words

if __name__ == '__main__':
    MRLongestWord.run()






