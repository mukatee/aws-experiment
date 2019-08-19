import csv
import glob
from datetime import datetime
import mysql.connector as db_conn
from mysql.connector import errorcode
from collections import defaultdict
from io import BytesIO, StringIO
import boto3
import os, sys
import logging

# https://stackoverflow.com/questions/7929364/python-best-practice-and-securest-to-connect-to-mysql-and-execute-queries
SQL_SELECT_USERS = "SELECT user_id, user_nick, discriminator FROM users"
SQL_INSERT_USER = "INSERT INTO users (user_nick, discriminator) VALUES (%s, %s)"
SQL_SELECT_SERVERS = "SELECT server_id, server_name FROM servers"
SQL_INSERT_SERVER = "INSERT INTO servers (server_id, server_name) VALUES (%s, %s)"
SQL_SELECT_CHANNELS = "SELECT server_id, channel_id, channel_name FROM channels"
SQL_INSERT_CHANNEL = "INSERT INTO channels (server_id, channel_id, channel_name) VALUES (%s, %s, %s)"
#SQL_SELECT_LATEST_MSG_ID = "SELECT msg_id, msg_timestamp, msg_epoch FROM messages  "
SQL_SELECT_LATEST_MSG_DATE = "SELECT MAX(msg_timestamp) FROM messages"
SQL_SELECT_LATEST_MSG_EPOCH = "SELECT MAX(msg_epoch) FROM messages WHERE channel_id = %s"
SQL_INSERT_MESSAGE = "INSERT INTO messages (msg_id, msg_txt, msg_timestamp, msg_epoch, user_id, server_id, channel_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PW = os.environ['DB_USER_PW']
#getenv handles missing values with default rather than exception
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)
S3_PREFIX = "logs1/"
S3_PREFIX = ""

class DBWriter():
    def __init__(self):
        self.cnx = db_conn.connect(user=DB_USER, password=DB_PW, database=DB_NAME, host=DB_HOST)
        self.cursor = self.cnx.cursor()
        self.users = {}
        self.servers = {}
        self.channels = {}

    def init_data(self):
        cursor = self.cursor
        users = self.users
        servers = self.servers
        channels = self.channels

        cursor.execute(SQL_SELECT_USERS)
        for (user_id, user_nick, discriminator) in cursor:
            #print(f"{user_id}, {user_nick} {discriminator}")
            users[f"{user_nick}#{discriminator}"] = user_id
        print(f"found {len(users)} users")

        cursor.execute(SQL_SELECT_SERVERS)
        for (server_id, server_name) in cursor:
            print(f"{server_id}, {server_name}")
            servers[server_id] = server_name

        cursor.execute(SQL_SELECT_CHANNELS)
        for (server_id, channel_id, channel_name) in cursor:
            print(f"{server_id}, {channel_id}, {channel_name}")
            channels[channel_id] = {"server_id": server_id, "channel_name": channel_name} #see above, channel id's should be globally unique

        cursor.execute(SQL_SELECT_LATEST_MSG_DATE)
        latest_date = cursor.fetchone()
        latest_date = latest_date[0]
        return latest_date

    def fetch_latest_epoch(self, channel_id):
        channel_data = (channel_id,)
        self.cursor.execute(SQL_SELECT_LATEST_MSG_EPOCH, channel_data)
        latest_epoch = self.cursor.fetchone()
        latest_epoch = latest_epoch[0]
        if latest_epoch is None:
            latest_epoch = 0
        print(f"latest epoch: {latest_epoch}")
        return latest_epoch

    def fetch_latest_epochs(self):
        latest_epochs = defaultdict(int)
        for channel_id in self.channels:
            latest_epochs[channel_id] = self.fetch_latest_epoch(channel_id)
        return latest_epochs


    def insert_msgs_db(self, msg_stream, latest_epochs):
        msg_ids = {}
        users = self.users
        servers = self.servers
        channels = self.channels
        cursor = self.cursor
        cnx = self.cnx

        skipped = 0
        skipped2 = 0
        inserted = 0
        csv_reader = csv.DictReader(msg_stream)
        logging.info("starting to loop CSV contents")
        for row in csv_reader:
            #print(row)
            server_id = row["server_id"]
            server_id = int(server_id)
            channel_id = row["channel_id"]
            channel_id = int(channel_id)
            server_name = row["server_name"]
            channel_name = row["channel_name"]
            msg_id = row["message_id"]
            msg_id = int(msg_id)
            if msg_id in msg_ids:
                skipped += 1
                continue
            msg_ids[msg_id] = msg_id
            msg_txt = row["message_txt"]
            msg_timestamp = row["timestamp"]
            msg_epoch = row["epoch"]
            msg_epoch = int(msg_epoch)
            latest_epoch = latest_epochs[channel_id]
            if msg_epoch <= latest_epoch:
                skipped2 += 1
                continue
            user_nick = row["user_nick"]
            discriminator = int(row["discriminator"])
            utc_time = datetime.fromisoformat(msg_timestamp)
            user_full_nick = f"{user_nick}#{discriminator}"
            if user_full_nick not in users:
                user_data = (user_nick, discriminator)
                cursor.execute(SQL_INSERT_USER, user_data)
                cnx.commit()
                users[user_full_nick] = cursor.lastrowid

            user_id = users[user_full_nick]

            if server_id not in servers:
                server_data = (server_id, server_name)
                cursor.execute(SQL_INSERT_SERVER, server_data)
                servers[server_id] = server_name
                cnx.commit()

            if channel_id not in channels:
                channel_data = (server_id, channel_id, channel_name)
                cursor.execute(SQL_INSERT_CHANNEL, channel_data)
                channels[channel_id] = {"server_id": server_id, "channel_name": channel_name}
                cnx.commit()

            msg_data = (msg_id, msg_txt, utc_time, msg_epoch, user_id, server_id, channel_id)
            cursor.execute(SQL_INSERT_MESSAGE, msg_data)
            cnx.commit()
            inserted += 1

        print(f"inserted {inserted} messages")
        print(f"skipped {skipped} + {skipped2} messages as duplicates")

    def insert_from_s3(self, latest_epochs):
        if S3_ENDPOINT_URL is not None:
            #for local testing using min.io
            s3 = boto3.client("s3", endpoint_url = S3_ENDPOINT_URL)
        else:
            s3 = boto3.client("s3")

        logging.info("loading object list from s3")
        #this is a partial list because it would need pagination
        #https://stackoverflow.com/questions/32635785/how-do-i-list-directory-contents-of-an-s3-bucket-using-python-and-boto3
        #it shows max 1000 items, can also handle that with file paths (prefix)
        partial_list = s3.list_objects_v2(
            Bucket = 'discord-logs',
            Prefix = S3_PREFIX)
    #    Prefix = 'discord-logs1/')
        logging.info(f"loaded object list:{partial_list}")
        obj_list = partial_list['Contents']
        obj_list = [obj["Key"] for obj in obj_list]
        obj_list.sort()
        latest_file = obj_list[-1]
        logging.info(f"Processing file:{latest_file}")
        obj = s3.get_object(Bucket = 'discord-logs', Key = latest_file)
        logging.info("got file from S3")
        file_data = obj['Body'].read().decode('utf-8')
        fileobj = StringIO(file_data)
        logging.info(f"inserting msgs, str len = {len(file_data)}")
        self.insert_msgs_db(fileobj, latest_epochs)

    def insert_from_file(self, latest_epochs):
        path = "./"
        message_files = glob.glob(f"{path}/message*.csv")
        message_files.sort()
        with open(message_files[-1]) as csv_file:
            self.insert_msgs_db(csv_file, latest_epochs)

    def close(self):
        self.cursor.close()
        self.cnx.close()

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    #root.addHandler(handler)

def insert_msgs():
    logging.info("initializing")
    dbw = DBWriter()
    logging.info("initialized connections, initializing base data")
    latest_date = dbw.init_data()
    logging.info("data initialized, reading latest epoch")
    latest_epochs = dbw.fetch_latest_epochs()
    logging.info("retrieved latest epochs, inserting from s3")
    dbw.insert_from_s3(latest_epochs)
    logging.info("inserted from s3, shutting down")
    #    insert_from_file(latest_epochs)
    dbw.close()

def handler(event, context):
    setup_logging()
    insert_msgs()

#handler(None, None)

