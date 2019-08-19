__author__ = 'teemu kanstren'

import boto3
import logging
import db_writer
import json
import sys

#https://stackoverflow.com/questions/49260369/aws-lambda-call-function-from-another-aws-lambda-using-boto3-invoke
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.invoke_async

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    #root.addHandler(handler)

def handler(event, context):
    setup_logging()
    logging.info("retrieving latest epochs")
    dbw = db_writer.DBWriter()
    dbw.init_data()
    latest_epochs = dbw.fetch_latest_epochs()
    dbw.close()
    return {"latest_epochs": latest_epochs}

