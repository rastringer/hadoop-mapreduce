from pyspark import SparkConf, SparkContext

# Create dictionary to convert IDs to movie names

def loadMovieNames():
    movieNames = {}
    with open('ml-100l/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

# Convert lines of u.data to movieID, rating, 1.0 so we can add ratings and number of ratings for each movie

def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load ID to movie name table
    movieNames = loadMovieNames()

    # Load raw u.data file
    lines = sc.textFile('hdfs:///user/maria_dev/ml-100k/u.data')

    # Convert to movieID - rating, 1:0
    movieRatings = lines.map(parseInput)

    # Reformat to movieID, sumOfRatings, totalRatings
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[1]))

    # Map to movieID, avg rating
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # Sort by avg rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    for result in results:
        print(movieNames[result[0]], result[1])

# Results:

# ('3 Ninjas: High Noon At Mega Mountain (1998)', 1.0)
# ('Beyond Bedlam (1993)', 1.0)
# ('Power 98 (1995)', 1.0)
# ('Bloody Child, The (1996)', 1.0)
# ('Amityville: Dollhouse (1996)', 1.0)
# ('Babyfever (1994)', 1.0)
# ('Homage (1995)', 1.0)
# ('Somebody to Love (1994)', 1.0)
# ('Crude Oasis, The (1995)', 1.0)
# ('Every Other Weekend (1990)', 1.0)
