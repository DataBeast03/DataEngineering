from __future__ import print_function
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":


	# Create StreamingContext with 2 threads, and batch interval of 1 second
	sc = SparkContext("local[2]", "NetworkWordCount")
	ssc = StreamingContext(sc, 2)

	# Create DStream to listen to hostname:port
	lines = ssc.socketTextStream("localhost", 9999)

	# Split each line into words
	words = lines.flatMap(lambda line: line.split(" "))

	# Count words in each batch
	pairs = words.map(lambda word: (word, 1))
	wordCounts = pairs.reduceByKey(lambda x, y: x + y)

	# Print first 10 elements of each RDD in DStream 
	wordCounts.pprint()

	# Start computation
	ssc.start()

	# Wait streaming to terminate
	ssc.awaitTermination()