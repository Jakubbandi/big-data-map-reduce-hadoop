from mrjob.job import MRJob
from mrjob.step import MRStep

class MRFlights(MRJob):

    def steps(self):
        return[
            MRStep(mapper=self.mapper)
        ]


    def mapper(self, _, line):
        year, items = line.split('\t')
        year = year[1:-1]
        items = items[1:-1]
        yield year, items


if __name__ == '__main__':
    MRFlights.run()