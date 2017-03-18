from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pprint import pprint


if __name__ == "__main__":

	def updateFunction(newValues, runningCount):
	    if runningCount is None:
	       runningCount = 0
	    return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count

	def check_process(line):
	    if "DEAD_PROCESS:" in line:
	        return "PROCESS ENDED"
	    if "USER_PROCESS:"  in line:
	        return "PROCESS STARTED"


	# Create StreamingContext with 2 threads, and batch interval of 1 second
	sc = SparkContext(appName="ProcessTracker")
	ssc = StreamingContext(sc, 2) # change batch duration to 10 sec for offical answer 
	ssc.checkpoint('ckpt')

	#display = lines.map(check_process) # to display output directly
	ds = ssc.socketTextStream("localhost", 9999)\
	        .map(lambda line: check_process(line))\
	        .map(lambda process: (process,1))\
	        .updateStateByKey(updateFunction)

	ds.pprint()
	# Start computation
	ssc.start()

	ssc.awaitTermination()
	#ssc.stop(stopSparkContext=True, stopGraceFully=True)