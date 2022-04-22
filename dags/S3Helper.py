import boto3

AWSREGION = "ap-southeast-1"
AWSKEY = "AKIAXL3EKMZMGCYQJBOJ"
AWSSECRETKEY = "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"

############################################# s3 object ###########################################


def get_s3():
    s3 = boto3.resource(
        service_name='s3',
        region_name=AWSREGION,
        aws_access_key_id=AWSKEY,
        aws_secret_access_key=AWSSECRETKEY
    )
    return s3
