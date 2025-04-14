# -*- coding: utf-8 -*-
# This line specifies the encoding of the file.
# Allows the file to contain non-ASCII characters. Example: "#" "@" "!" etc.
from mrjob.job import MRJob

# 5. Calculate the average length of words in the file using MapReduce.
# (Keep track of total number of characters and total number of words)


with open('avg_length.txt', 'w') as f:
    f.write("""
Data is beautiful and powerful.
Understanding data helps build solutions.
""")

class MRavg_length(MRJob):
    def mapper(self, _, line):
        # Split the line into words and calculate the length of each word
        words = line.split() # Split line into words
        for word in words:
            word_length = len(word)
            yield "average_length", (word_length, 1) # Generate key-value pairs for each word with its length and count.

    def reducer(self, key, values):
        # Sum up the total lengths and total words
        total_length = 0
        total_words = 0
        for length, count in values:
            total_length += length
            total_words += count

        # Calculate the average length of words
        if total_words > 0:
            average_length = total_length / total_words
            yield key, average_length

if __name__ == '__main__':
    MRavg_length.run()