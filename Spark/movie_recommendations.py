from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Load movie ID - movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

# Convert u.data lines to userID, movieID, rating rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # Load movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get raw data
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Convert to RDD of row objects with userID, movieID, rating
    ratingsRDD = lines.map(parseInput)

    # Convert to DataFrame and cache
    ratings = spark.createDataFrame(ratingsRDD).cache()

    # Create ALS collaborative filtering model from data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # Print ratings from user 0:
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']

    print("\nTop 20 recommendations:")
    # Find movies rated > 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # Construct test dataframe for user 0 with every movie rated > 100 times
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

    # Run model on popular movies list for user ID 0
    recommendations = model.transform(popularMovies)

    # Top 20 movies with highest predicted rating for  user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])

    spark.stop()

# Top 20 recommendations:

# (u'Crimson Tide (1995)', nan)
# (u'Ghost and the Darkness, The (1996)', nan)
# (u'Courage Under Fire (1996)', nan)
# (u"It's a Wonderful Life (1946)", nan)
# (u'Jungle2Jungle (1997)', nan)
# (u'Big Night (1996)', nan)
# (u'Grease (1978)', nan)
# (u'Murder at 1600 (1997)', nan)
# (u'Con Air (1997)', nan)
# (u'Gone with the Wind (1939)', nan)
# (u'Dragonheart (1996)', nan)
# (u"What's Eating Gilbert Grape (1993)", nan)
# (u'Peacemaker, The (1997)', nan)
# (u'Natural Born Killers (1994)', nan)
# (u"My Best Friend's Wedding (1997)", nan)
# (u'Beauty and the Beast (1991)', nan)
# (u'Right Stuff, The (1983)', nan)
# (u'M*A*S*H (1970)', nan)
# (u'Mother (1996)', nan)
# (u'Eraser (1996)', nan)
