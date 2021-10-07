import boto3
import uuid

s3 = boto3.client('s3')


def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        path = event['Records'][0]['s3']['object']['key']
        data = s3.get_object(Bucket=bucket, Key=path)['Body'].read()
        data_list = data.decode().replace('\r','').split('\n')

        fields = data_list[0].split(',')

        for line in data_list[1:]:
            temp_dict = {}
            parsed_line = line.split(',')
            for i in range(len(fields)):
                temp_dict[fields[i]] = parsed_line[i]
            temp_dict["id"] = str(uuid.uuid4())
            resp = put_csv_data(temp_dict, "case")
            print(resp) 
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e



        
def put_csv_data(data, table_name, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.eu-central-1.amazonaws.com")

    table = dynamodb.Table(table_name)
    response = table.put_item(
       Item=data
    )
    return response
