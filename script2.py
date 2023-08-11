import boto3
import sys
import os
from collections import Counter
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr


debug = True
# file_name = "cluster_analysis.txt"
table_name = "photos-live-events"
dynamoDB_column = "cluster_id"
dynamoDB_index_name = 'pk-created_at-index'
api_cluster_id_attribute = 'id'

config = Config(retries = dict(max_attempts=10, mode='standard'))
dynamodb = boto3.resource('dynamodb', config=config)
table = dynamodb.Table(table_name)
age_ranges = [(2, 5), (5, 12), (12, 15), (15, 19), (19, 29), (29, 39), (39, 49), (49, 64), (64, 74), (74, 1000)]
age_group_counts = {f"{start}-{end}": 0 for start, end in age_ranges}
gender_list = []


if debug:  
    file_uri = r"C:\Users\amank\OneDrive\Desktop\\"   
else:
    file_uri = "/tmp/"



def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    captureErr = "Line No. : " + str(lineno)  + " | ERROR: " + str(exc_obj)
    return captureErr


def get_cluster_list(cluster_id):
    
    try:
        response = table.query(

            IndexName= dynamoDB_index_name,
            ScanIndexForward = True,
            FilterExpression=Attr(dynamoDB_column).eq(cluster_id),
            KeyConditionExpression=Key('pk').eq('externaoficial')
            )
        
        if response['Items']:
            
            for item in response['Items']:
                print(item)
                extract_required_data(item['details'])
        else:
            print(response['Items'])


        while response.get('LastEvaluatedKey'):
            response = table.query(

                IndexName= dynamoDB_index_name,
                ScanIndexForward = True,
                FilterExpression=Attr(dynamoDB_column).eq(cluster_id),
                KeyConditionExpression=Key('pk').eq('externaoficial'),
                ExclusiveStartKey= response.get('LastEvaluatedKey')
                
            )
            if response['Items']:
                # print(response['Items'])
                for item in response['Items']:
                    print(item)
                    extract_required_data(item['details'])
            else:
                print(response['Items'])


    except:
        print(PrintException())
        return False
        

    else:
        my_str = ''
        item_counts = Counter(gender_list)
        total_items = len(gender_list)
        if not(total_items == 0):
            # print("=============================================================")
            create_report("\n=============================================================\n")
            # print("Cluster ID: ", cluster_id)
            create_report(f"Cluster ID: {cluster_id}\n")
            my_str = my_str + str(cluster_id)
            # print("\nGender Ratio:")
            create_report("\nGender Ratio:\n")
            
            
            for item, count in item_counts.items():
                percentage = (count / total_items) * 100
                my_str = my_str +","+ str(item) +","+ str(count) +","+ str(f"{percentage:.2f}%")
                # print(f"{item}: {count} occurrences, {percentage:.2f}%")
                create_report(f"{item}: {count} occurrences, {percentage:.2f}%\n")

            # Print the age group counts
            # print("\nAge Group Ratio:")
            create_report("\nAge Group Ratio:\n")
            
            for age_group, count in age_group_counts.items():
                
                percentage = (count / total_items) * 100
                my_str = my_str +","+ str(age_group) +","+ str(count) +","+ str(f"{percentage:.2f}%")
                if count == 0:
                    continue
                # print(f"{age_group} age group: {count} people, {percentage:.2f}%")
                create_report(f"{age_group} age group: {count} people, {percentage:.2f}%\n")

            # print("=============================================================")
            create_report("\n=============================================================\n")


            # print(my_str)
            return True
        else:
            print("No Result Found for Cluster ID: ", cluster_id)
            return False



def extract_required_data(response):

    data = str(response)

    gender = data.split('"Gender": {"Value": ')[1].split(', "Confidence": ')[0][1:-1]
    age_range = data.split('"AgeRange": {')[1].split('},')[0].replace(" ", "").split('"Low":')[1].split(',"High":')
    low_age = int(age_range[0])
    high_age = int(age_range[1])
    average_age = int(((low_age + high_age) / 2))
    print(f'{low_age = }')
    print(f'{high_age = }')
    print(f'{average_age = }')
    print(f'{gender = }')
    calc_age_group(average_age, gender)



def calc_age_group(average_age, gender):
    
    for start, end in age_ranges:
        if start <= average_age < end:
            age_group_counts[f"{start}-{end}"] += 1

    gender_list.append(gender)




def create_report(row):

    print(row)
    with open(file_uri+file_name, 'a') as file:
        # Write content to the file
        file.write(row)


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
    global file_name
    file_name = event['cluster_id']+".txt"
    try:
        if os.path.isfile(file_uri+file_name):
            os.remove(file_uri+file_name)
        
        if  get_cluster_list(event['cluster_id']) and not(debug):
            upload_report(event["S3_Bucket"], event["S3_directory_name"]+"temp/"+file_name)

    except:
        exception = PrintException()
        print("================================")
        print("Something went wrong.")
        print("Exception: ",exception)
        print("================================")

    return event

# https://apis.photos.live/photos/cmw?page_size=32&category=29.07.23%20Sabado

if __name__ == "__main__":
    event = {"S3_Bucket": "bucket-name", "S3_directory_name": "Script_Reports/", "cluster_id":"1689033510-a04141a4-2437-4a57-93b1-97f4f6cf71ab"}
    lambda_handler(event,"")
