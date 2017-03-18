Miniquiz
--------

Q: Create the multiplication table of 7 multiplied by numbers from 1
to 30.

for i in {1..30}; do echo $(( $i*7)) ; done

OR 

echo $[7*{1..30}]

Q: How can I list hidden directories and view their permissions?

ls -al

Q: What will this output?

`(echo a ; echo b ; echo a ; echo b) | sort | uniq -c`

 2 a
 2 b