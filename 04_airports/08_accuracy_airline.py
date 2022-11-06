from mrjob.job import MRJob
from mrjob.step import MRStep

class MRAccuracyAirline(MRJob):

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')

        if departure_delay == '':
            departure_delay = 0

        if arrival_delay == '':
            arrival_delay = 0

        departure_delay = abs(departure_delay)
        arrival_delay = abs(arrival_delay)

        yield airline, (departure_delay,arrival_delay)

    def reducer(self, key, values):
        total_dep = 0
        total_arr = 0
        num_rows = 0
        for value in values:
            total_dep +=value[0]
            total_arr +=value[1]
            num_rows += 1
            yield key, (total_dep / num_rows, total_arr / num_rows)

if __name__ == '__main__':
    MRAccuracyAirline.run()