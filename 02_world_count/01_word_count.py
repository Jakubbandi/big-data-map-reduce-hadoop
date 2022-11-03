from mrjob import MRJob
from mrjob.step import MRStep

class MRWordCount(MRJob)
    def mapper(self, _, line):
        yield line,1

if __name__ == '__main__':
    MRWordCount.run()