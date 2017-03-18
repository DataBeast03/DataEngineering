# Spark Web UI guide

1. Click into  **`PySparkShell`**

   ![image](images/stage_1.png)
   
2. You will see no jobs since you have not run anything yet.
   
   ![image](images/stage_2.png)
   
3. Load in the airline data in the IPython notebook. Refresh the UI and still you should see nothing since 
   you have not run an action and due to lazy evaluation, nothing would run unless an action is run.
   
   ```python
   # DON'T INCLUDE THE '[' AND ']'
   link = 's3n://[YOUR_AWS_ACCESS_KEY_ID]:[YOUR_AWS_SECRET_ACCESS_KEY]@mortar-example-data/airline-data'
   airline = sc.textFile(link)
   ```
   
4. Print the first 2 line by doing a `.take(2)` on the RDD. Refresh the jobs page of the UI and you should see
   some jobs running. 
   
   ![image](images/stage_3.png)
   
5. Click into one of the jobs and you will see the following of the details of the specific job.

   ![image](images/stage_4.png)

6. Click on the job again and you will details about the workload on each of your workers, i.e. how many tasks/partitions
   under the job are assigned to each worker, how many tasks each worker has successfully completed, how long they 
   took, etc. 
   
   ![image](images/stage_5.png)






