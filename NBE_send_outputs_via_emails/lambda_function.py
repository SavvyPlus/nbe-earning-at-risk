import boto3
from botocore.exceptions import ClientError
import time
from config import bucket_nbe


def lambda_handler(event, context):
    run_id = event['run_id']
    job_id = event['job_id']

    retry = 10
    interval = 60
    for i in range(retry):
        s3 = boto3.resource('s3')
        files_path = 'Outputs_PBI/{}/{}/'.format(run_id, job_id)  # run_id, job_id
        bucket = s3.Bucket(bucket_nbe)
        key_lst = []
        for object_summary in bucket.objects.filter(Prefix=files_path):
            key_lst.append(object_summary)
        if len(key_lst) == 3:
            print('Sending emails...')
            SENDER = "weiliang.zhou@zawee.work"
            RECIPIENT = ["weiliang.zhou@zawee.work"]

            # The subject line for the email.
            SUBJECT = "NextBusinessEnergy - Earning At Risk output files"

            # The email body for recipients with non-HTML email clients.
            BODY_TEXT = "..."

            # The HTML body of the email.
            BODY_HTML = """<html> <head></head> <body> <h1>Amazon SES Test (SDK for Python)</h1> 
            <p>This email was sent with <a href='https://aws.amazon.com/ses/'> Amazon SES 
            </a> using <a href='https://aws.amazon.com/sdk-for-python/'> AWS SDK for Python (Boto)</a>.</p> </body> </html>"""

            # The character encoding for the email.
            CHARSET = "UTF-8"

            # Create a new SES resource and specify a region.
            client = boto3.client('ses', region_name='ap-southeast-2')

            # Try to send the email.
            try:
                # Provide the contents of the email.
                response = client.send_email(
                    Destination={
                        'ToAddresses': RECIPIENT,
                    },
                    Message={
                        'Body': {
                            'Html': {
                                'Charset': CHARSET,
                                'Data': BODY_HTML,
                            },
                            'Text': {
                                'Charset': CHARSET,
                                'Data': BODY_TEXT,
                            },
                        },
                        'Subject': {
                            'Charset': CHARSET,
                            'Data': SUBJECT,
                        },
                    },
                    Source=SENDER,
                    # If you are not using a configuration set, comment or delete the
                    # following line
                    # ConfigurationSetName=CONFIGURATION_SET,
                )
            # Display an error if something goes wrong.
            except ClientError as e:
                print(e.response['Error']['Message'])
            else:
                print("Email sent! Message ID:" + response['MessageId'])
            break
        else:
            print('Outputs are incomplete yet, waiting {} seconds...'.format(interval))
            time.sleep(interval)
