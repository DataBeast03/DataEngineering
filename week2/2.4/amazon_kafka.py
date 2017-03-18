from __future__ import print_function

import sys
import time
import happybase
import simplejson as json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def get_purchases(datum):
    item = datum['dataunit']['purchase_edge']['item_id']['item_number']
    user = datum['dataunit']['purchase_edge']['userID']['user_number']
    true_as_of_secs = datum['pedigree']['true_as_of_secs']
    return user, item,true_as_of_secs

def get_user_gender(gender_datum):
    
    gender  = gender_datum['dataunit']['user_property']['property']['gender']
    user_id = gender_datum['dataunit']['user_property']['userID']['user_number']
    ts      = gender_datum['pedigree']['true_as_of_secs']
    
    return user_id, gender, ts

def get_user_location(location_datum):
    
    city    = location_datum['dataunit']['user_property']['property']['location']['city']
    state   = location_datum['dataunit']['user_property']['property']['location']['state']
    country = location_datum['dataunit']['user_property']['property']['location']['country']
    user_id = location_datum['dataunit']['user_property']['userID']['user_number']
    ts      = location_datum['pedigree']['true_as_of_secs']
    
    return user_id, city,state,country, ts

def get_user_name(name_datum):
    
    name    = name_datum['dataunit']['user_property']['property']['user_name']['name']
    user_id = name_datum['dataunit']['user_property']['userID']['user_number']
    ts      = name_datum['pedigree']['true_as_of_secs']
    
    #     return user_id, name, ts
    return  user_id, name, ts

def get_itemName(item_datum):
    ID     = item_datum['dataunit']['item_property']['item_id']['item_number']
    name    = item_datum['dataunit']['item_property']['property']['item_name']
    ts      = item_datum['pedigree']['true_as_of_secs']
    return ID, name, ts

def get_review_rating(review_datum):
    user_id = review_datum['dataunit']['review_edge']['userID']['user_number']
    item_id = review_datum['dataunit']['review_edge']['item_id']['item_number']
    rating  = review_datum['dataunit']['review_edge']['rating']
    review  = review_datum['dataunit']['review_edge']['review']
    ts      = review_datum['pedigree']['true_as_of_secs']
    return user_id, item_id, rating, ts, review    






def partition_data(datum):
    datatype = datum['dataunit'].keys()[0]
    if datatype.endswith('property'):
        # returns partitioned properties
        return '/'.join((datatype, datum['dataunit'][datatype]['property'].keys()[0])), datum
    else:
        # returns edges
        return datatype, datum 
    
def prep_data_Hbase(datum):
    '''returns formated datum'''
    def function_call(label):
        if label == "user_name":
            return get_user_name
        elif label == 'gender':
            return get_user_gender
        elif label =="location": 
            return get_user_location
        elif label == "purchase_edge":
            return get_purchases
        elif label == "review_edge":
            return get_review_rating
        else:
            return get_itemName
        
    if "property" in datum[0]:
        label = datum[0].split("/")[1]
    else:
        label = datum[0]
        
    func = function_call(label)
    
    return func(datum[1]),label 
                
def insert_data_hbase(datum,label):
    '''inserts data into hbase in batchs - one insert per property/edge'''
    table = connection.table(label)
    if label == "user_name":
        table.put(datum[0], {'d:name'   : datum[1], 
                                  'd:ts' : str(datum[2])})
        return "insert"
    elif label == "gender":
        table.put(datum[0], {'d:gender' : datum[1],
                                'd:ts'   : str(datum[2])})
        return "insert"
    elif label == "location":
        table.put(datum[0], {'d:city'     : datum[1],
                                'd:state'  : datum[2],
                                'd:country': datum[3],
                                'd:ts'     : str(datum[4])})
        return "insert"
    elif label == "purchase_edge":
        table.put(datum[0], {'d:item_id': datum[1],
                                'd:ts'     : str(datum[2])})
        return "insert"
    elif label == "review_edge":
        table.put(datum[0], {'d:item_id': datum[1],
                                'd:rating' : str(datum[2]),
                                'd:ts'     : str(datum[3]),
                                'd:review' : str(datum[4])})
        return "insert"
    else:
        table.put(datum[0], {'d:name'   : datum[1], 
                              'd:ts'     : str(datum[2])})
        return "insert"
        
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)  
    
if __name__ == "__main__":
    

    
    sc = SparkContext(appName="Amazon-Data")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint('ckpt')
    connection = happybase.Connection(host = 'localhost')
    zkQuorum, topic = 'localhost:2181' , 'amazon-topic' # use localhost that topic is created on!
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    
    # keep count of fact labels 
#     kvs.window(windowDuration=4,slideDuration=2)\
#        .map(lambda tup: partition_data(json.loads(tup[1])))\
#        .map(lambda tup: (tup[0],1))\
#        .updateStateByKey(updateFunction).pprint()
    
    # keep count of fact labels 
    kvs.window(windowDuration=4,slideDuration=2)\
       .map(lambda tup: partition_data(json.loads(tup[1])))\
       .map(lambda tup: prep_data_Hbase(tup))\
       .saveAsTextFiles(prefix = "sparkdata/sparkdata")
            
    ssc.start()
    ssc.awaitTermination()
 