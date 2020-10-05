import boto3
from botocore.exceptions import ClientError
import logging
import glob

bucket_name ='police-calls-records-per-year'
pattern = "/home/mariaj/records/"

# Generate the boto3 client for interacting with S3
s3 = boto3.client('s3',region_name ='us-west-2', 
                        # Set up AWS credentials that were get through IAM
                        aws_access_key_id= 'key_id', 
                        aws_secret_access_key= 'secret_key')


# Create a new Bucket with the name police-calls-records-per-year in 
# the region us-west-2(Oregon)
s3.create_bucket(Bucket = bucket_name,
                CreateBucketConfiguration=
                {'LocationConstraint': 'us-west-2'})

# Print the buckets that are on s3
buckets = s3.list_buckets()
print(buckets['Buckets'])


def pc_files_names(pattern):
    """Find all the files names on the pc

    :param pattern: The path where are all the files

    In the path or pattern /home/mariaj/records there are 
    the following files:
    '2019_Police_Calls.cvs', '2018_Police_Calls.cvs',
    '2017_Police_Calls.cvs', '2016_Police_Calls.cvs',
    '2015_Police_Calls.cvs', ...

    glob with the method glob1 will find for us all the names files 
    that end with .csv in the given path or pattern.
    """
    files_names_list = glob.glob1(pattern, '*.csv')
    return files_names_list


def upload_file(file_name_path, bucket_name, object_name):
    """Upload a file to an S3 bucket and set public access

    :param file_name_path: pattern + file_name (absolute path to the file)
    :param bucket: Bucket to upload the file
    :param object_name: object name sets how appears on the s3 bucket
    :return: True if file was uploaded, else False
    """  
    try:
        response = s3.upload_file(file_name_path, bucket_name, object_name,
                                # AWS defaults to denying permission so with ACL we made the object public to read
                                # Change the ContentType if the object is an image, pdf, etc
                                ExtraArgs={'ContentType': "text/plain", 'ACL': "public-read"} )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_files_to_s3(bucket_name, pattern, files_names_list):
    """Upload several files from the pc to S3 object

    :param bucket_name: Bucket name to upload to (string) 
    :param pattern: Path to the file on the pc
    :param pc_files_names: List with the files names on the pc
    :return: True if files were uploaded, else False
    """
    try:
      for file_name in files_names_list:
          upload_file(pattern + file_name, bucket_name, file_name)
          
    except ClientError as e:
        logging.error(e)
        return False
    return True


def public_object_url(object_name, bucket_name = bucket_name):
    """Public object URL (previously the object should have setted public-read with ACL) 
    
    Params to the s3.upload_file method
    :param bucket_name: Bucket name 
    :param object_name: Object key or the path which was saved the object in the s3 bucket.
    :return: tuple with the object name and the public url
    """
  # public object URL: bucket_name.s3.amazonaws.com/key, 
    url_object = "https://{}.s3.amazonaws.com/{}".format(bucket_name, obj['Key'])
    return (object_name, url_object)


if __name__ == '__main__':
  # Find the files names
  files_names_list = pc_files_names(pattern)

  # If upload_files_to_s3 function uploaded the files then it will return true
  if upload_files_to_s3(bucket_name, pattern, files_names_list):
    response = s3.list_objects(Bucket = bucket_name)
    for obj in response['Contents']:
      object_name = obj['Key']
      print(public_object_url(object_name, bucket_name))