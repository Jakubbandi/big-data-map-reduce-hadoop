from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w]+")

class MRFood(MRJob):

    def steps(self):
        return[
            MRStep(mapper=self.mapper)
        ]

    def mapper(self, _, line):
        (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator, HelpfulnessDenominator
         , Score, Time, Summary, Text) = line.split('\t')
        words = WORD_RE.findall(Text)
        words = filter(lambda word: len(word)>1, words)
        words = map(str.lower, words)
        yield None, list(words)

    def reducer(self, key, values):
        yield key, max(values)


if __name__ == '__main__':
    MRFood.run()