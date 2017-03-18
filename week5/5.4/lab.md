Lab
===

Step 1: Install HBase
---------------------

Install HBase on your machine.

Bring it up in *standalone* mode.

Bring up the HBase shell.

Type `puts "hello world"` and see that it works.

Follow the steps in the lecture notes.

Step 2: Download Data
---------------------

Download stock data for these companies: AAPL, AMZN, GOOG, MSFT

Here is how to get the stock for AAPL, for example:

<http://real-chart.finance.yahoo.com/table.csv?s=AAPL&g=d&ignore=.csv>

```bash
# Download CSV files for all companies.
for SYMBOL in AAPL AMZN GOOG MSFT; do
  echo "${SYMBOL}: Writing ${SYMBOL}.csv"
  URL="http://real-chart.finance.yahoo.com/table.csv?s=${SYMBOL}&g=d&ignore=.csv"
  curl -L ${URL} > ${SYMBOL}.csv
done
```

Step 3: Design Row Key
----------------------

Design a row key schema for storing this data in HBase. 

Your goal is to calculate the minimum/maximum/average prices for each
company over the period covered in the data.

Step 4: Upload Data
-------------------

Using the lecture notes as a guide create a HBase shell script to
upload the CSV data into HBase.

Step 5: Calculate Statistics
----------------------------

Use `scan` (or the `range` function in `hbase-jruby`) to go through
the data for each company and calculate the minimum, maximum, and
average prices for each company across all time.

Step 6: Row Key Redesign 
------------------------

Suppose the hedge fund you work for, HedgeBase, now wants to create an
index fund based on the stocks of these companies. Each share of the index
fund is made up of exactly one share of the constituent companies.

They want to use HBase to calculate the minimum/maximum/average price
of this index fund.

What row key should they use? 

Design the system that would calculate the minimum, maximum, and
average price of the index fund.

- The row key in this case must be `date:stock`
