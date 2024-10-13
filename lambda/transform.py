import json
import boto3

s3 = boto3.client('s3')

def handler(event, context):
    
    #extract info
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    if key.startswith('transformed/'):
        print(f'Ignoring the file already transformed: {key}')
        return {
            'statusCode': 200,
            'body': json.dumps(f"Arquivo ignorado: {key}")
        }


    #Download the file from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')

    #Transforme text to upper case
    transformed_content = content.upper()

    #Upload to S3 back
    new_key = f"transformed/{key}"
    s3.put_object(Bucket=bucket, Key=new_key, Body=transformed_content)

    return {
        'status': 200,
        'body': json.dumps(f"File transformed and saved to {new_key} successfully!")
    }