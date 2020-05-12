""" To be imported to extraction scripts to push data to S3. """

import os
import shutil
import tempfile
import boto3


def load_s3_json(dataframe, local_file_name, s3_bucket, output_filename):
    """ This saves a dataframe locally to a temporary file and then uploads it """

    temp_dir = tempfile.TemporaryDirectory()
    input_filename = os.path.join(temp_dir.name, local_file_name)
    dataframe.to_json(input_filename, orient="records")
    load_s3(s3_bucket, input_filename, output_filename)
    # remove temp directory if done
    if os._exists(temp_dir.name):
        shutil.rmtree(temp_dir.name)

def load_s3_parquet(dataframe, local_file_name, s3_bucket, output_filename):
    """ This saves a dataframe locally to a temporary file and then uploads it """

    temp_dir = tempfile.TemporaryDirectory()
    input_filename = os.path.join(temp_dir.name, local_file_name)
    dataframe.to_parquet(input_filename)
    load_s3(s3_bucket, input_filename, output_filename)
    # remove temp directory if done
    if os._exists(temp_dir.name):
        shutil.rmtree(temp_dir.name)

def load_s3(s3_bucket, input_filename, output_filename):

    """ Pushes file to S3. """

    s3_storage = boto3.resource("s3")
    s3_storage.meta.client.upload_file(input_filename, s3_bucket, output_filename)
    print(
        f"COMPLETE: {input_filename} loaded into \
    s3:// {s3_bucket} as {output_filename}"
    )

def check_s3_for_file(s3_bucket, output_filename):

    """Checks to see if a file already exists in a bucket, returns TRUE/FALSE"""
    s3_storge = boto3.resource("s3")
    bucket = s3_storge.Bucket(s3_bucket)
    objs = list(bucket.objects.filter(Prefix=output_filename))
    return len(objs) > 0 and objs[0].key == output_filename
