from pyspark import SparkContext
import re
import sys

def calculate_range(values):
    max_val = max(values)
    min_val = min(values)
    return max_val - min_val

def main():
    # Create the SparkContext
    sc = SparkContext(appName='CeilingHeightRange')

    # Read the input file into an RDD
    input_rdd = sc.textFile("/user/student15/input_ProjectPart2/Project_Data/")

    # Filter out the records
    filtered_rdd = input_rdd.filter(lambda line: line[70:75] != "99999" and int(line[75:76]) in [0, 1, 4, 5, 9])

    # Extract station id and ceiling height from the filtered record
    filtered_map_rdd = filtered_rdd.map(lambda line: (line[4:10], int(line[70:75])))

    # Calculate the range
    station_range_rdd = filtered_map_rdd.reduceByKey(lambda ht1, ht2: max(ht1, ht2) - min(ht1, ht2))

    # Save the output into a output file
    station_range_rdd.saveAsTextFile("/user/student15/output_ProjectPart2-1/")


    sc.stop()

if __name__ == '__main__':
   main()

