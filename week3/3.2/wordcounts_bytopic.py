'''The classic MapReduce job: count the frequency of words.'''
from mrjob.job import MRJob
from string import punctuation

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in line.split():
            yield (word.strip(punctuation).lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))
if __name__ == "__main__":
	MRWordFreqCount.run()