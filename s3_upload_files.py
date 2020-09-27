import boto3
import glob

Bucket_name ='police-calls-records-per-year'
pattern = "/home/mariaj/records/"

# Generate the boto3 client for interacting with S3
s3 = boto3.client('s3',region_name ='us-west-2', 
                        # Set up AWS credentials that were get through IAM
                        aws_access_key_id= 'Your_key_id', 
                        aws_secret_access_key= 'Your_secret_key')


# Create a new Bucket with the name police-calls-records-per-year in 
# the region us-west-2(Oregon)
s3.create_bucket(Bucket = Bucket_name,
                CreateBucketConfiguration=
                {'LocationConstraint': 'us-west-2'})


# Print the buckets that are on s3
buckets = s3.list_buckets()
print(buckets['Buckets'])


# Upload the files
"""
  In the path /home/mariaj/records there are the following files:
  '2019_Police_Calls.cvs', '2018_Police_Calls.cvs',
  '2017_Police_Calls.cvs', '2016_Police_Calls.cvs',
  '2015_Police_Calls.cvs', ...

  glob with the method glob1 will find for us all the names files 
  that end with .csv in the given path o pattern.
"""

records_csv = glob.glob1(pattern, '*.csv')

# upload_file method params: path to file to upload, Bucket name, s3 object name or key.
for file_name in records_csv:
  s3.upload_file(pattern + file_name,
                  Bucket_name, 
                  file_name,
                  # AWS defaults to denying permission so with ACL we made the object public to read
                  # Change the ContentType if the object is an image, pdf, etc
                  ExtraArgs={'ContentType': "text/plain", 'ACL': "public-read"} 
                )

response = s3.list_objects(Bucket = Bucket_name)

for obj in response['Contents']:
  print(obj['Key'])
  # Print public object URL: bucketname.s3.amazonaws.com/key, 
  # previously above the object has been set public-read with ACL
  print("https://{}.s3.amazonaws.com/{}".format(Bucket_name, obj['Key']))
