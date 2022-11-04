from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'[\w]+')

class MRJobFirstStep(MRJob):

    def steps(self):
        return[
            MRStep(mapper=self.mapper)
        ]
    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word, 1