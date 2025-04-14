# -*- coding: utf-8 -*-
# This line specifies the encoding of the file.
# Allows the file to contain non-ASCII characters. Example: "#" "@" "!" etc.

from mrjob.job import MRJob
# 2. Find the frequency of each character in a given text file.
# Create a text file(word_freq.txt) with this content:

with open("char_freq.txt", "w") as f: # We chose to replace "word_freq.txt" with "char_freq.txt" to avoid confusion with the previous task.
    f.write("""
Hello World!
MapReduce is fun.
Python is awesome.
"""
) 

class MRCharFreq(MRJob):
    def mapper(self, _, line): 
        for char in line:
            if char.isalpha():  # Check if the character is an alphabet
                yield char.lower(), 1  # Yield the character in lowercase with a count of 1

    def reducer(self, char, counts):
        yield char, sum(counts)  # Sum the counts for each character

if __name__ == '__main__': # Main function to run the MapReduce job
    MRCharFreq.run()


