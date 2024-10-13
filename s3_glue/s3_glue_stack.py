from aws_cdk import (
    # Duration,
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_iam as iam,
    aws_glue as glue
)
from constructs import Construct

class S3GlueStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, 
                        "ETLBucket_jefersonrocha")

        lambda_function = _lambda.Function(self, "TransformLambda",
                                        runtime=_lambda.Runtime.PYTHON_3_9,
                                        handler="transform.handler",
                                        code=_lambda.Code.from_asset("lambda"))
        
        bucket.grant_read_write(lambda_function)

        notification = s3n.LambdaDestination(lambda_function)
        bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification)

        #Glue catalog database
        database = glue.CfnDatabase(self, "ETLDatabase",
                                    catalog_id=self.account,
                                    database_input={
                                        "Name": "etl_database"
                                    })

        table = glue.CfnTable(self, "ETLTable",
                            catalog_id=self.account,
                            database_name=database.ref,
                            table_input={
                                "Name": "etl_table",
                                "StorageDescriptor": {
                                    "Columns": [
                                            {"Name": "column1", "Type": "string"},
                                            {"Name": "column2", "Type": "string"}
                                    ],
                                    "Location": f"s3//{bucket.bucket_name}/",
                                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                                    "Compressed": False,
                                    "Compressed": False,
                                        "SerdeInfo": {
                                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                                            "Parameters": {"field.delim": ","}
                                        } 
                                }

                            })