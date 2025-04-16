from mrjob.job import MRJob
import re

HASHTAG_RE = re.compile(r"#\w+")
# This matches any word starting with '#' followed by letters/numbers

class MRHashtagCount(MRJob):

    def mapper(self, _, line):
        for hashtag in HASHTAG_RE.findall(line):
            yield hashtag.lower(), 1  # Emit each hashtag

    def reducer(self, hashtag, counts):
        yield hashtag, sum(counts)

if __name__ == '__main__':
    MRHashtagCount.run()