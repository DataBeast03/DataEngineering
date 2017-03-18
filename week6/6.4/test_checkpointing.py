from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from text_file_util import xrange_write
    
from pprint import pprint
    
def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)  
    
checkpointDir = 'ckpt'
    
def functionToCreateContext():
    ssc = StreamingContext(SparkContext(), batchDuration=2)
    
    # Add new values with previous running count to get new count
    ds = ssc.textFileStream('input') \
        .map(lambda x: int(x) % 10) \
        .map(lambda x: (x,1)) \
        .updateStateByKey(updateFunction)
    ds.pprint()
    ds.count().pprint()
    
    # Set up checkpoint
    ssc.checkpoint(checkpointDir)
    return ssc
    
print 'Initializing ssc'
ssc = StreamingContext.getOrCreate(
    checkpointDir, functionToCreateContext)
    
print 'Starting ssc'
ssc.start()
    
# Write data to textFileStream
xrange_write()