import aws_cdk as core
import aws_cdk.assertions as assertions

from s3_glue.s3_glue_stack import S3GlueStack

# example tests. To run these tests, uncomment this file along with the example
# resource in s3_glue/s3_glue_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = S3GlueStack(app, "s3-glue")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
