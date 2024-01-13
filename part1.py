from mrjob.job import MRJob
import re

class Wind_Dir_Avg(MRJob):

    # Mapper function
    def mapper(self, _, line):
        year = line[15:19]
        month = line[19:21]
        wind_dir = line[60:63]
        quality_code = line[63:64]

        try:
            wind_dir = int(wind_dir)
            quality_code = int(quality_code)
        except ValueError:
            return

        # Check if wind direction is missing or not of good quality
        if wind_dir == 999 or quality_code not in [0, 1, 4, 5, 9]:
            return

        yield (year + month, (wind_dir, 1))

    # Reducer function
    def reducer(self, key, values):
        wind_dir_total = 0
        count_total = 0

        for wind_dir, n in values:
            wind_dir_total += wind_dir
            count_total += n

        # Calculate the average wind direction and yield the result
        wind_dir_avg = wind_dir_total / count_total
        yield ('Year: ', key[:4]), ('For month: ', key[4:], 'Average: ', wind_dir_avg)
if __name__ == '__main__':
    Wind_Dir_Avg.run()



