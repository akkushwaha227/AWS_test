
import boto3
import sys
import os
from botocore.config import Config

config = Config(retries = dict(max_attempts=10, mode='standard'))


debug = False


file_name = "cluster_analysis.txt"
# bucket_name='bucket-for-testing-221'
# prefix = "feature_aws_health_checks/aws_server_health_check/aws_health_check_result/"

if debug:
    file_uri = r""   
else:
    file_uri = "/tmp/"


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    captureErr = "Line No. : " + str(lineno)  + " | ERROR: " + str(exc_obj)
    return captureErr


def read_s3_files(bucket_name, prefix):

    s3 = boto3.client('s3', config=config)
    count = -1
    result = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    # print(f"{result = }")
    for o in result.get('Contents'):
        data = s3.get_object(Bucket=bucket_name, Key=o.get('Key'))
        contents = data['Body'].read().decode("utf-8")
        # print(contents)
        count = count + 1
        if count == 0:
            continue
        create_report(str(count))
        create_report(contents)
        
        


def create_report(row):

    print(row)
    
    with open(file_uri+file_name, 'a') as file:
        # Write content to the file
        file.write(str(row).replace("\r","") )


'''
    This function upload the files from the list to 
    the S3 bucket and at particular key loacation provided.
'''
def upload_report(bucket_name, key):
    print("upload_report called")
    try:
        print("filename: ", file_uri+file_name)
        s3 = boto3.resource('s3', config=config)
        s3.meta.client.upload_file(file_uri+file_name, bucket_name, key)
    except:
        exception = PrintException()
        print(exception)
        print("Bucket Name: ", bucket_name)
        print("Key :", key)
    else:
        print("Successfully Uploaded")
        print(f"Saved at: {bucket_name}/{key}")




def lambda_handler(event, context):
    # TODO implement
    print("Received Event: ", event)
    
    try:
        if os.path.isfile(file_uri+file_name):
            os.remove(file_uri+file_name)

        read_s3_files(event["S3_Bucket"], event["S3_directory_name"]+"temp/")
        if not(debug):
            upload_report(event["S3_Bucket"], event["S3_directory_name"]+file_name)

    except:
        exception = PrintException()
        print("================================")
        print("Something went wrong.")
        print("Exception: ",exception)
        print("================================")

    return event

# https://apis.photos.live/photos/cmw?page_size=32&category=29.07.23%20Sabado

if __name__ == "__main__":
    event = {"S3_Bucket": "bucket-for-testing-221", "S3_directory_name": "feature_aws_health_checks/my_test/"}
    lambda_handler(event,"")


