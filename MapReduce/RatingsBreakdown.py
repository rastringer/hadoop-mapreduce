# Hadoop MapReduce
# Find movie review data here:
# http://media.sundog-soft.com/hadoop/u.data

from mrjob.job import MRJob
from mrjob.job import MRStep

class RatingsBreakdown(MRJob):
    # Define single mapreduce phase
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
            reducer=self.reducer_count_ratings)
    ]

# Define mapper to break up lines of data on tab, return key value pair of rating and 1
def mapper_get_ratings(self, _, line):
  (userID, movieID, rating, timestamp) = line.split('/t')
  yield rating, 1

# Reduce key values, return rating and rating count
def reducer_count_ratings(self, key, values):
    yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()

# Using Python locally:
# python RatingsBreakdown.py
# Output:

# "5"     21203
# "1"     6111
# "2"     11370
# "3"     27145
# "4"     34174

# Using Hadoop:
# python RatingsBreakdown.py  -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data
# No configs found; falling back on auto-configuration

# Output:
# "1"     6111
# "2"     11370
# "3"     27145
# "4"     34174
# "5"     21203

# To count the no. of ratings for each movie, we can simply change the yield value in the mapper function:

# def mapper_get_ratings(self, _, line):
#   (userID, movieID, rating, timestamp) = line.split('/t')
#   yield movieID, 1
