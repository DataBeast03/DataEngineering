# Introduction to MapReduce and MRJob

This morning we will be covering Map Reduce with [MRJob](https://pythonhosted.org/mrjob/) locally.

## Meet MrJob

`mrjob` is a Python package that helps you write and run Hadoop Streaming jobs. It supports Amazon's Elastic MapReduce (EMR) and it also works with your own Hadoop cluster.  It has been released as an open-source framework by Yelp and we will use it as interface for Hadoop due to its legibility and ease to use with MapReduce tasks.  Feel free to take a pause and read through some of the [mrjob docs](http://mrjob.readthedocs.org/en/latest/index.html) or this [mrjob tutorial](https://pythonhosted.org/mrjob/guides/quickstart.html) to familarize yourself with the main concepts.

mrjob fully supports Amazon's Elastic MapReduce (EMR) service, which allows you
to buy time on a Hadoop cluster on an hourly basis. It also works with your own
Hadoop cluster.

Some important features:

* Run jobs on EMR, your own Hadoop cluster, or locally (for testing).
* Write multi-step jobs (one map-reduce step feeds into the next)

## Setup

1. Install the `mrjob` python module:

    ```
    pip install mrjob
    ```

2. We're going to be using the [Reuters 20 Newsgroups dataset](http://qwone.com/~jason/20Newsgroups/) today.

    Download options:
    
    * Download it [here](http://qwone.com/~jason/20Newsgroups/20news-19997.tar.gz).
    * Another location if first is down [here](http://kdd.ics.uci.edu/databases/20newsgroups/20newsgroups.html).
    * Or find it on the ZA Time Capsule in `datasets`.

3. Create a mini version of the dataset with these commands:

    Feel free to modify this if you want to choose documents from different groups.

    ```
    mkdir mini_20_newsgroups
    mkdir mini_20_newsgroups/comp.windows.x
    mkdir mini_20_newsgroups/rec.motorcycles
    mkdir mini_20_newsgroups/sci.med
    cp 20_newsgroups/comp.windows.x/663* mini_20_newsgroups/comp.windows.x
    cp 20_newsgroups/rec.motorcycles/10311* mini_20_newsgroups/rec.motorcycles
    cp 20_newsgroups/sci.med/5889* mini_20_newsgroups/sci.med
    ```
    
## Word Count Map Reduce Job

Word Counts is the "Hello World" of Map Reduce. Take a look at this canonical example which will return the count of each word across all the documents:

```python
'''The classic MapReduce job: count the frequency of words.'''

from mrjob.job import MRJob
from string import punctuation


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in line.split():
            yield (word.strip(punctuation).lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordFreqCount.run()

```

1. Let's try running this!

    * Create a file `wordcounts.py` with the above contents.
    * Run it locally: `python wordcounts.py mini_20_newsgroups > counts`
    * Look at the results: `subl counts`

    Note how clever it is! It takes your directory name and goes through every directory within it and finds all the documents. It will pass to your script each line of the files separately. MrJob is doing a lot of work for you.

**Note:** In practice, you would run your code in the cloud like this (you won't be doing this today):

* on EMR: `python wordcounts.py words.txt -r emr > counts`
* on your Hadoop cluster: `python wordcount.py words.txt -r hadoop > counts`


## Word Counts per Topic

Instead of getting total word counts across all the documents, let's get the word count for each topic. We'd like results that look like this:

```
"comp.windows.x_about"	12
```

This means the word `about` appeared in `comp.windows.x` documents 12 times.

1. Create a new file called `wordcounts_bytopic.py`. Use the code from the previous section as a starting point.

2. Modify the key to include the topic along with the word. To get the topic, import the `os` module and use `os.environ['map_input_file']` to get the filename. From the filename you should be able to pull out the topic name.

3. Run the job with `python wordcounts_bytopic.py mini_20_newsgroups > countsbytopic` and manually inspect the results.


## Extra Credit 1: json and tokenization

Now that you have experience with setting up a very simple map reduce job, we can get our hands dirty with the NYT.

Use `articles.json` from this repo. Each line is the json of a single nyt article.

1. Repeat what you have done in the previous exercise to tokenize all of the articles. You will need to use the `json` module and the `json.loads` method to load the data.

2. Try out another method of tokenization. Above we used the string `split` method, which is the simplest possibility.

    * Try using regular expressions with the `re` module like in [mrjob's tutorial](https://pythonhosted.org/mrjob/guides/quickstart.html#writing-your-second-job).
    * Use the [nltk tokenizer](http://nltk.org/api/nltk.tokenize.html). You could even use a [stemmer](http://www.nltk.org/howto/stem.html).

3. Get a count of the number of times each word appears in each article.

4. Additionally, within the same job, get the total number of times each word appears across all the documents.


## Extra Credit 2: Counters

Since word counts are really common, MrJob has a counter built in. Here's a version of the simple word count script using the counter:

```python
'''The classic MapReduce job: count the frequency of words.'''

from mrjob.job import MRJob
from string import punctuation


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in line.split():
            self.increment_counter("word", word.strip(punctuation).lower())

if __name__ == '__main__':
    MRWordFreqCount.run()

```

1. Run this. Note that the output is now in the logs, so you need to run it like this to save the results:

    ```
    python wordcounts2.py mini_20_newsgroups/ 2> counts2
    ```

2. Can you use `increment_counter` above instead of how you implementing your word count by topic script?


## Extra extra credit: Run on Amazon EMR

1. Sign up for an Amazon EMR account.

    * Create an [Amazon Web Services account](http://aws.amazon.com/)
    * Sign up for [Elastic MapReduce](http://aws.amazon.com/elasticmapreduce/)
    * Get your access and secret keys: Click "Security Credentials" on [your account page](http://aws.amazon.com/account/)

2. Set up your MrJob config file. Check out the [documentation](https://pythonhosted.org/mrjob/guides/emr-quickstart.html).

    * Create `~/.mrjob.conf`
    * Set the environment variables `aws_access_key_id` and `aws_secret_access_key`.

3. Do the same task, but this time in the cloud! Use the `-r emr` flag to tell `mrjob` to use EMR instead of running the job locally.
