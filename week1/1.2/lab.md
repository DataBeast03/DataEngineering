Data Modeling Practice
----------------------

In this exercise, you will demonstrate your understanding of normalization by designing a data model. You have two files: `https://s3-us-west-2.amazonaws.com/dsci6007/data/dbip-city-2015-07.csv` (henceforth `dbip`) and `https://s3-us-west-2.amazonaws.com/dsci6007/data/link.log`.  
`dbip` is essentially a lookup table to find the geolocation for any given IP address. However, because there are over four billion IPv4 addresses (to say nothing of IPv6), each row represents a range. For example:

    ip_start     	ip_end     	country 	stateprov   	city
    194.45.112.64   194.45.112.71   NL     Gelderland     Arnhem
    194.45.112.72   194.45.112.79   NL        Drenthe  Hoogeveen
    194.45.112.80   194.45.112.87   NL  North Holland  Amsterdam
    194.45.112.88   194.45.112.95   NL  North Holland     Diemen
    194.45.112.96   194.45.112.103  NL  South Holland  Rotterdam
    194.45.112.104  194.45.112.111  DE      Thuringia     Erfurt
    194.45.112.112  194.45.112.119  NL  South Holland  Rotterdam
    194.45.112.120  194.45.112.135  DE      Thuringia     Erfurt

`link.log` contains simulated data representing a link from one IP address to one or more other IP addresses. The data look like this:

    source         	trusted	ts              	destinations
    49.73.120.173   False   2015-07-07 13:13:29 {"40.158.102.3"}
    206.248.235.182 False   2015-07-07 13:13:32 {"33.159.193.158"}
    186.39.249.152  False   2015-07-07 13:13:33 {"206.208.143.6", "96.168.100.43"}

    
What we want is a fully normalized database supporting SQL queries that will be able to show us which cities linked to which, at what time, and so on.

1. Create a normalized database schema for these data using Gliffy.
2. Write `CREATE TABLE` statements to generate these tables.

Normalization Practice
----------------------

Starting from the solution to this previous exercise, work in pairs to load the data into your database. Then create views to answer the following questions:

1. For each country, how many trusted and how many untrusted incoming links are made?
2. For each country, how many domestic and how many international outgoing links are made?