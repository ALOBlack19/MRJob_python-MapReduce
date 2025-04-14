# -*- coding: utf-8 -*-
# This line specifies the encoding of the file.
# Allows the file to contain non-ASCII characters. Example: "#" "@" "!" etc.
from mrjob.job import MRJob

# 4. Each line of the file contains <username> <tweet>. Use MapReduce to count how many tweets each user has made.

with open('tweets.txt', 'w') as f:
    f.write("""
alice I love data science.
bob MapReduce is great!
alice Python is fun.
carol I’m learning so much!
bob This is amazing.
""")
    
class MRTweetCount(MRJob):
    def mapper(self, _, line):
        # Split the line into username and tweet
        line = line.strip()
        username = None
        tweet = None
        # Check if the line is not empty and contains a space (indicating a username and tweet) 
        if line:  # Ensure the line isn't empty
            username, tweet = line.split(' ', 1) 
        yield username, 1  # Yield the username with a count of 1

    def reducer(self, username, counts):
        yield username, sum(counts)  # Sum the counts for each username
# 
if __name__ == '__main__':
    MRTweetCount.run()