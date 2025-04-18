# -*- coding: utf-8 -*-
# This line specifies the encoding of the file.
# Allows the file to contain non-ASCII characters. Example: "#" "@" "!" etc.
from mrjob.job import MRJob

# Solve the following problems using MRJob in python:
# 1. Count the number of words in a text excluding stop words. Ignore the stop words “The”, “is”, “a”.

with open("word_count.txt", "w") as f:
    f.write("""
This is a simple text file. This file is meant to test word count.
The test includes a few common words and uncommon ones.
"""

) # Create a text file with the above content.


class MRwordcount(MRJob): # Defining a MapReduce job (class) to count words inside of the txt file.
    STOP_WORDS = {"the", "is", "a"} # Set of stop words to ignore (inside of a list)

    def mapper(self, _, line): #Mapper function to read the input file line by line
        # Mapper function to create -->key-value<-- pairs for each word in the line
        for word in line.split():# Loop to split the line into words
            word = word.lower() 
            if word not in self.STOP_WORDS: # Check if the word is not a stop word
                yield word, 1 # Yield 

    def reducer(self, word, counts): #Reducer function to sum the counts of each word.
        # "word" is the key and "counts" is a generator of values (1s) for that key.
        yield word, sum(counts)

if __name__ == '__main__': # Main function to run the MapReduce job
    MRwordcount.run()
