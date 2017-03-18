from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pprint import pprint

PROC_UNKNOWN=0
PROC_STARTED=1
PROC_STOPPED=2

def line_to_event(line):
    fields = line.split(':')
    if len(fields) > 3: 
        process_state = fields[3].strip()
        if process_state == 'USER_PROCESS': return PROC_STARTED
        if process_state == 'DEAD_PROCESS': return PROC_STOPPED
    return PROC_UNKNOWN

def event_count_to_text((event,count)):
    if event == PROC_STARTED: return "Started: " + str(count)
    if event == PROC_STOPPED: return "Stopped: " + str(count)
    return ""

BATCH_DURATION = 10
WINDOW_DURATION = 60 
SLIDE_DURATION = 20

sc = SparkContext(appName='PythonStreamingQueueStream')
ssc = StreamingContext(sc, BATCH_DURATION)

ssc.checkpoint('ckpt')

ssc.socketTextStream("localhost", 9999)\
  .map(line_to_event)\
  .filter(lambda event: event == PROC_STARTED or event == PROC_STOPPED)\
  .map(lambda event: (event,1))\
  .reduceByKey(lambda count1,count2: count1+count2)\
  .pprint()
  # .reduceByKeyAndWindow(
  #     func=lambda count1, count2: count1 + count2,
  #     invFunc=lambda count1, count2: count1 - count2, 
  #     windowDuration=WINDOW_DURATION,
  #     slideDuration=SLIDE_DURATION)\
  # .map(event_count_to_text)\
  # .transform(lambda rdd: rdd.sortBy(lambda text: text))\
  # .saveAsTextFiles('process-stats')

ssc.start()
ssc.awaitTermination()
