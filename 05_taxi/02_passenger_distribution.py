from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMap(MRJob):

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RetacodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')
        yield passenger_count, tolls_amount

    def reducer(self, key, values):
        num = 0
        for value in values:
            num += 1
        yield key, num

if __name__ == '__main__':
    MRMap.run()