Linux Intro
===========

Objectives
----------

By the end of this morning, you will be able to:

- `ssh` into a remote Linux server

- Examine log files using `head` and `vi`

- Create a `pipe` for interprocess communication

- Find patterns using `grep`

- `cut` out selected portions of each line of a file

- `sort` lines of text files

- report or filter out repeated lines in a file using `uniq`

Connecting to a remote computer
-------------------------------

#### ~~`telnet`~~  

#### `ssh`

SSH
---

> Secure Shell, or SSH, is a cryptographic (encrypted) network
> protocol for initiating text-based shell sessions on remote machines
> in a secure way.
 
![Ssh_binary_packet](https://upload.wikimedia.org/wikipedia/commons/0/0f/Ssh_binary_packet_alt.svg)


    ssh -i ~/.ssh/galvanize-DEI.pem ec2-user@ec2-52-27-60-213.us-west-2.compute.amazonaws.com ls

Unix Shell Walkthrough
----------------------

#### Grab `shakespeare-sonnets.txt`.

    curl -LO http://dsci6007.s3.amazonaws.com/data/shakespeare-sonnets.txt

#### Let's look at the first few lines.
    
    head shakespeare-sonnets.txt 

#### Let's skip the first two lines:

    tail -n +3 shakespeare-sonnets.txt | head


#### Let's cut out the chapter : verse part:

    tail -n +3 shakespeare-sonnets.txt | cut -d' ' -f2- | head

#### Let's translate the characters to lower case:

    tail -n +3 shakespeare-sonnets.txt | cut -d' ' -f2- | tr 'A-Z' 'a-z' | head

#### Let's tokenize our words:


    tail -n +3 shakespeare-sonnets.txt | 
        cut -d' ' -f2- | 
        tr 'A-Z' 'a-z' | 
        tr -sc 'a-z' '\012' | 
        head

#### Let's sort them:


    tail -n +3 shakespeare-sonnets.txt |
        cut -d' ' -f2- |
        tr 'A-Z' 'a-z' |
        tr -sc 'a-z' '\012' |
        sort |
        head

#### What is our vocabulary?

    tail -n +3 shakespeare-sonnets.txt |
        cut -d' ' -f2- |
        tr 'A-Z' 'a-z' |
        tr -sc 'a-z' '\012' |
        sort |
        uniq |
        head

#### How big is our vocabulary?

    tail -n +3 shakespeare-sonnets.txt | 
        cut -d' ' -f2- | 
        tr 'A-Z' 'a-z' | 
        tr -sc 'a-z' '\012' | 
        sort | 
        uniq | 
        wc -w

#### How many times does each word occur?

    tail -n +3 shakespeare-sonnets.txt | cut -d' ' -f2- | 
        tr 'A-Z' 'a-z' | tr -sc 'a-z' '\012' | 
        sort | uniq -c | head

#### How might we construct a rhyming dictionary?

    tail -n +3 shakespeare-sonnets.txt | cut -d' ' -f2- | 
        tr 'A-Z' 'a-z' | tr -sc 'a-z' '\012' | 
        sort | uniq | rev | sort | rev | head

#### What was the penultimate word of each sentence?

    tail -n +3 shakespeare-sonnets.txt | cut -d' ' -f2- | awk '{print $(NF-1)}' | head

#### What was the antepenultimate word of each sentence?

    tail -n +3 shakespeare-sonnets.txt | cut -d' ' -f2- | awk '{print $(NF-2)}' | head

#### What's the word count of those words?

    tail -n +3 shakespeare-sonnets.txt |
        cut -d' ' -f2- |
        awk '{print $(NF-2)}' |
        sort |
        uniq -c |
        head

#### Let's delete punctuation:

    tail -n +3 shakespeare-sonnets.txt |
        cut -d' ' -f2- |
        awk '{print $(NF-2)}' |
        sort |
        tr -d '[:punct:]' |
        uniq -c |
        head


Parsing Web Log Files
---------------------

1. Look at the file `data/NASA_access_log_Jul95.gz` without
saving the uncompressed version using `gunzip -c`.

2. Find the total number lines in the files.

3. Using `gunzip -c` find the total number of 400 errors in the file.
This includes errors such as 401, 404, etc.

4. Find the total number of 500 errors in the file. Again include all
errors from 500-599.

5. Find the total count of all the different status codes in the
`NASA_access_log_Jul95.gz` file.

Note
----

- Using `export LC_CTYPE=C` sets the locale to the default locale
  instead of ASCII.

- Setting this enables commands like `rev` and `tr` to not produce
  `Illegal byte sequence` warnings.


