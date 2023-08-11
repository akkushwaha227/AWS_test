import boto3
import sys
import json
import os
import http.client
from botocore.config import Config

config = Config(retries = dict(max_attempts=10, mode='standard'))


debug = True

api_cluster_id_attribute = 'cluster_ids'
client = boto3.client('lambda', config=config)


if debug:  
    file_uri = r"C:\Users\amank\OneDrive\Desktop\\" 
    lambda_name = ""  
else:
    file_uri = "/tmp/"
    lambda_name = os.environ["lambda_name"]


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    captureErr = "Line No. : " + str(lineno)  + " | ERROR: " + str(exc_obj)
    return captureErr


def read_api(api_host, api_path, params, S3_Bucket, S3_directory_name):

    headers = {}
    lambda_input = {"S3_Bucket": S3_Bucket, "S3_directory_name": S3_directory_name}
    # Create an HTTP connection
    connection = http.client.HTTPSConnection(api_host)

    api_path = f"{api_path}?{'&'.join([f'{key}={value}' for key, value in params.items()])}"

    # print(api_path)

    connection.request("GET", api_path, headers=headers)
    response = connection.getresponse()

    if response.status == 200:
        # Read and decode the JSON response
        json_data = response.read().decode("utf-8")

        # Parse the JSON data
        parsed_json = json.loads(json_data)

        print(parsed_json['photos'])

        for item in parsed_json['photos']:
            # print(item)
            # print(f"{item['photo_id'] = }")
            count = 0
            if isinstance(item[api_cluster_id_attribute], str):
                print(f'{item[api_cluster_id_attribute] = }')
                lambda_input["cluster_id"] = item[api_cluster_id_attribute]
                if not(debug):  
                    execute_lambda_function(lambda_name, lambda_input)
            else:
                for cluster_id in item[api_cluster_id_attribute]:
                    count = count +1
                    print(f"{cluster_id = }")
                    lambda_input["cluster_id"] = cluster_id
                    if not(debug):
                        execute_lambda_function(lambda_name, lambda_input)

        while parsed_json.get('next_token') :
            api_path = f"{api_path}?{'&next_token='+str(parsed_json.get('next_token'))}"

            connection.request("GET", api_path, headers=headers)
            response = connection.getresponse()

            if response.status == 200:
                json_data = response.read().decode("utf-8")
                parsed_json = json.loads(json_data)


                # print(parsed_json)

                for item in parsed_json['photos']:
                    # print(item)
                    # print(f"{item['photo_id'] = }")
                    if isinstance(item[api_cluster_id_attribute], str):
                        print(f'{item[api_cluster_id_attribute] = }')
                        lambda_input["cluster_id"] = item[api_cluster_id_attribute]
                        if not(debug):
                            execute_lambda_function(lambda_name, lambda_input)
                    else:
                        for cluster_id in item[api_cluster_id_attribute]:
                            count = count +1
                            print(f"{cluster_id = }")
                            lambda_input["cluster_id"] = cluster_id
                            if not(debug):
                                execute_lambda_function(lambda_name, lambda_input)

    else:
        print("Request failed with status code:", response.status)


    # Close the connection
    connection.close()


def execute_lambda_function(f_name, input):

    lambda_payload = str(json.dumps(input))
    print(f"{lambda_payload = }")
    response = client.invoke(
                    FunctionName= f_name, 
                    InvocationType='Event',
                    Payload=lambda_payload
                )
    print("Lambda Invoked")
    # print(response['Payload'].read())



def lambda_handler(event, context):
    # TODO implement
    print("Received Event: ", event)

    try:
        
        read_api(event['api_host'], event['api_path'], event['params'], event['S3_Bucket'], event['S3_directory_name'])

    except:
        exception = PrintException()
        print("================================")
        print("Something went wrong.")
        print("Exception: ",exception)
        print("================================")

    return event

# https://apis.photos.live/photos/cmw?page_size=32&category=29.07.23%20Sabado
# https://apis.photos.live/photos/321evento?page_size=32&category=uai
# https://apis.photos.live/photos/321evento?page_size=32&category=Categoria%20Carai
# https://apis.photos.live/photos/321evento/photographer?photographer=Joana%20Piovani

if __name__ == "__main__":
    event = {"S3_Bucket": "bucket-name", "S3_directory_name": "Script_Reports/", "api_host": "apis.photos.live", "api_path":"/photos/321evento/photographer", "params": {"photographer": 'Joana%20Piovani'}}
    lambda_handler(event,"")
