import boto

# Connect to S3
access_key, access_secret_key = 'YOUR ACCESS KEY', 'YOUR SECRET ACCESS KEY'
conn = boto.connect_s3(access_key, access_secret_key)

# List all the buckets
all_buckets = [b.name for b in conn.get_all_buckets()]
print all_buckets

# Check if bucket exist. If exist get bucket, else create one
bucket_name = 'galvanizebucket'

if conn.lookup(bucket_name) is None:
    b = conn.create_bucket(bucket_name, policy='public-read')
else:
    b = conn.get_bucket(bucket_name)

# Write new file
file_object = b.new_key('sample.txt')
file_object.set_contents_from_string('HAHAHAH',
                                     policy='public-read')

# Read from file
print file_object.get_contents_as_string()

# Print all the files in the bucket
filenames = [f.name for f in b.list()]
print filenames

# Delete a file
a = b.new_key('somefilename')
a.delete()

# Delete Bucket
# Must delete all files in bucket
# Before deleting bucket
conn.delete_bucket('galvanizebucket')

