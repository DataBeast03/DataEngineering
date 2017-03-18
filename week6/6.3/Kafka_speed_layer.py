import io, random, threading, logging, time

from kafka.client   import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer
from faker import Faker

KAFKA_TOPIC = 'amazon-topic'

N = 5
n = 0
class Producer(threading.Thread):
	def gen_itemProperties_and_ReviewEdges_proto(self,gen_ID, name_list, review_list):
	    '''
	    For every 50 item names generated, 10 item reviews and ratings will be generated.
	    A reivew may be generated for a user multiple times.
	    An item may be tagged multiple times with different names.
	    Suggestion: sample itemID_list without replacement 
	    '''
	    from time import time
	    import numpy as np
	    import random
	    
	    # pair in tuples: (file_name,file_review)
	    name_review = [(name,review) for name, review in zip(name_list,review_list)]
	    
	    names_generated = 0 
	    # keep record of items have been assinged names
	    instaniated_items = []
	    
	    while True:
	        # get random ith_name_review index
	        item_i = np.random.choice(len(name_review))
	        # get ith_name value
	        item_name , _ = name_review[item_i]
	        
	        # get random item id
	        item_id = gen_ID.next()
	        
	        # store (item_id, ith_name_review_index)_ith
	        instaniated_items.append((item_id,item_i))
	        
	        yield {"pedigree": {"true_as_of_secs": get_time.next()},
	                   "dataunit": {"item_property": {"item_id": {"item_number": item_id},
	                                                 "property": {"item_name"  : item_name}}}}
	        names_generated += 1
	        
	        if names_generated % 10 == 0:
	            "Ratings go to items that already have a name and in 'instaniated_items'  "
	            # get random user
	            user_id = gen_ID.next()            
	            # get random j_th item
	            item_jth =  np.random.choice(len(instaniated_items))
	            # get (item_id, ith_name_review_index)
	            item_id, _ = instaniated_items[item_jth]
	            # pick ith_review - making sure that the correct item review is picked
	            #_ , review = name_review[item_ith]
	            
	            # generate purchase                                   
	            yield {"pedigree": {"true_as_of_secs": get_time.next()},
	                   "dataunit": {"purchase_edge": {"userID": {"user_number": user_id},
	                                                 "item_id": {"item_number": item_id},
	                                          "purchase_time" : get_time.next()}}}
	                                       
	            
	            random_int = random.randint(1,3)
	            if random_int < 3:
	                # rating is between 1 and 5 (inclusive)
	                rating = np.random.choice(np.arange(1,6))
	                # generate rating                       
	                yield  {"pedigree": {"true_as_of_secs": get_time.next()},
	                       "dataunit": {"review_edge":{"userID"  : {"user_number": user_id},
	                                                   "item_id" : {"item_number": item_id},
	                                                    "review" :   "null",
	                                                     "rating": rating}}}

	        continue 



	def gen_userProperties(self,gen_ID):
	    from time import time
	    from nltk.corpus import names
	    from numpy.random import choice, shuffle, random
	    
	    Names = names.words()
	    shuffle(Names)
	    
	    def get_gender():
	        if random() > 0.51:
	            return "MALE"
	        else:
	            return "FEMALE"        

	    
	    fake = Faker()
	    while True:
	        user_id = gen_ID.next()
	        random_int = choice(np.arange(1,11))
	        

	        first = choice(names.words())
	        last = choice(names.words())

	        yield {'pedigree': {'true_as_of_secs': get_time.next()},
	               'dataunit': {'user_property': {'userID': {'user_number': user_id},
	                                 'property': {'user_name': {"name": first + " " + last}}
	                                               }                                          
	                             }

	                }

	        if (random_int > 5):
	            yield {"pedigree": {'true_as_of_secs': get_time.next()},
	                   "dataunit": {"user_property": {"userID": {"user_number": user_id},
	                                                "property": {"gender": get_gender()}
	                                                 }
	                               }
	                  }
	        else:
	            # generate random city and state
	            yield {"pedigree": {'true_as_of_secs': get_time.next()},
	                   "dataunit": {"user_property": {"userID": {"user_number": user_id},
	                                                "property": {"location": {"city" :  fake.city(), 
	                                                                          "state": fake.state(),
	                                                                         "country": "USA"}}
	                                                  }
	                                }
	                   }

	import uuid
	import time
	def gen_ID(self):
	    while True:
	        yield str(u.get_time())+ strftime("_%Y_%H_%M", gmtime())

	def get_itemName_itemReview(self):
	    
	    from os import listdir
	    from os.path import isfile, join
	    import random
	    
	    path = "/Users/Alexander/Downloads/MicropinionDataset_09-17-2012/pre-processed"
	    file_list = [ f for f in listdir(path) if isfile(join(path,f)) ]

	    reviews = []
	    item_name = []
	     
	    random.shuffle(file_list)
	    N = len(file_list)
	    for n in xrange(N):
	        # used to concatnate lines in each review
	        review_temp = ""
	        # get item name
	        item_name.append(file_list[n].rstrip(".data"))
	        
	        f = open(path + "/" + file_list[n], 'r')
	        raw_review = f.readlines()
	        
	        for line in raw_review:
	            review_temp += line
	        
	        reviews.append(review_temp)
	    
	    return item_name, reviews

	def adj_time(self):
	    from time import time
	    import numpy as np
	    hour = 60 * 60
	    day = 24 * hour
	    week = 7 * day
	    while True:
	        frac = np.random.random_sample() # value between 0 and 1
	        yield int(time() + 1)


    def gen_userProperties(self,gen_ID):
	    from time import time
	    from nltk.corpus import names
	    from numpy.random import choice, shuffle, random
	    
	    Names = names.words()
	    shuffle(Names)
	    
	    def get_gender(self):
	        if random() > 0.51:
	            return "MALE"
	        else:
	            return "FEMALE"        

	    
	    fake = Faker()
	    while True:
	        user_id = gen_ID.next()
	        random_int = choice(np.arange(1,11))
	        

	        first = choice(names.words())
	        last = choice(names.words())

	        yield {'pedigree': {'true_as_of_secs': get_time.next()},
	               'dataunit': {'user_property': {'userID': {'user_number': user_id},
	                                 'property': {'user_name': {"name": first + " " + last}}
	                                               }                                          
	                             }

	                }

	        if (random_int > 5):
	            yield {"pedigree": {'true_as_of_secs': get_time.next()},
	                   "dataunit": {"user_property": {"userID": {"user_number": user_id},
	                                                "property": {"gender": get_gender()}
	                                                 }
	                               }
	                  }
	        else:
	            # generate random city and state
	            yield {"pedigree": {'true_as_of_secs': get_time.next()},
	                   "dataunit": {"user_property": {"userID": {"user_number": user_id},
	                                                "property": {"location": {"city" :  fake.city(), 
	                                                                          "state": fake.state(),
	                                                                         "country": "USA"}}
	                                                  }
	                                }
	                   }

    daemon = True
    def run(self):
    	n = 0
    	N = 5
        client = KafkaClient('localhost:9092')
        producer = SimpleProducer(client)
        	# get item names and review from data file
		item_names, item_reviews = self.get_itemName_itemReview()
		get_time = self.adj_time()
		# create generator instance
		gen_item_prop_review_edge = self.gen_itemProperties_and_ReviewEdges_proto(gen_ID,item_names,item_reviews)
		# create generator instance
		gen_user_props = self.gen_userProperties(gen_ID)

        while n < N:
            producer.send_messages(KAFKA_TOPIC, str(user_prop_gen.next()))
            producer.send_messages(KAFKA_TOPIC, str(item_prop_reivew_gen.next()))
            n += 1


class Consumer(threading.Thread):
    '''Consumes users from Kafka topic.'''
    daemon = True
    def run(self):
        client = KafkaClient('localhost:9092')
        consumer = KafkaConsumer(KAFKA_TOPIC,
                                 group_id='my_group',
                                 bootstrap_servers=['localhost:9092'])
        #writer = DataFileWriter(open(file_name, "w"), DatumWriter(), schema)
        for message in consumer:
            print message
             #writer.append(userDatum) # save to master data set
            
        #writer.close()  


if __name__ == "__main__":
	threads = [ Producer(), Consumer() ]
	for t in threads: t.run()





