from mrjob.job import MRJob
import sys
import re

class VisibilityAndStation(MRJob):

    # Mapper function
    def mapper(self, _, line):
        station_id = line[4:10]
        visibility_dist = line[78:84]
        quality_code = line[84:85]

        try:
            visibility_dist = int(visibility_dist)
            quality_code = int(quality_code)
            station_id = int(station_id)
        except ValueError:
            return

        # Check if wind direction is missing or not of good quality
        if visibility_dist == 999999 or quality_code not in [0, 1, 4, 5, 9]:
            return

        yield station_id, visibility_dist


    # Reducer function
    def reducer(self, station_id, visibility_dist):
        # Iterate through visibility distances and yield each one
        for distance in visibility_dist:
            yield station_id, distance

if __name__ == '__main__':
    VisibilityAndStation.run()


