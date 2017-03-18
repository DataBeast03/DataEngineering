import itertools
import time
import random
import uuid

from distutils import dir_util 

# Every batch_duration write a file with batch_size numbers, forever.
# Start at 0 and keep incrementing. (For testing.)

def xrange_write(
        batch_size = 5,
        batch_dir = 'input',
        batch_duration = 1):
    dir_util.mkpath('./input')
    
    # Repeat forever
    for i in itertools.count():
        # Generate data
        min = batch_size * i 
        max = batch_size * (i + 1)
        batch_data = xrange(min,max)
      
        # Write to the file
        unique_file_name = str(uuid.uuid4())
        file_path = batch_dir + '/' + unique_file_name
        with open(file_path,'w') as batch_file: 
            for element in batch_data:
                line = str(element) + "\n"
                batch_file.write(line)
    
        # Give streaming app time to catch up
        time.sleep(batch_duration)