Lab
===============================================================
Leveraging the work you did in `hdfs2`, load the page view data from `SuperWebAnalytics/master/page_view` and write batch jobs (in Spark) to do the following:  

1. URL normalization (strip '`http://`', '`#`'s, '`?`'s, etc.)
2. Deduplicate page views
3. Roll up page-views over time (by hour, day, week, and month)