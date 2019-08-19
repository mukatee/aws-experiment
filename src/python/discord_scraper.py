from time import time, gmtime, strptime, mktime
from json import loads as json_to_array
from os import path, makedirs, getcwd
from sys import stderr, version_info
from unicodedata import normalize
from mimetypes import MimeTypes
from random import choice
from datetime import datetime
import csv
import io
import os, sys
import boto3
from io import BytesIO
import logging
import json

random_string = lambda length: ''.join([choice('0123456789ABCDEF') for i in range(length)])
fix_utf_error = lambda string: normalize('NFKC', string).encode('iso-8859-1', 'ignore').decode('iso-8859-1')
py3_url_split = lambda url: [url.split('/')[2], '/%s' % '/'.join(url.split('/')[3::])]
get_snowflake = lambda timems: (timems - 1420070400000) << 22
get_timestamp = lambda sflake: ((sflake >> 22) + 1420070400000) / 1000.0
get_mimetype = lambda string: MimeTypes().guess_type(string)[0] if MimeTypes().guess_type(string)[0] != None else 'application/octet-stream'
get_tstruct = lambda string: strptime(string, '%d %m %Y %H:%M:%S')

TEST_MODE = os.getenv('TEST_MODE', False)
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)
S3_PREFIX = "logs1/"
BUCKET_NAME = os.environ['BUCKET_NAME']


#https://i.imgur.com/UxWvdYDr.png
#https://www.reddit.com/r/discordapp/comments/6vm67d/are_channel_ids_universally_unique/

def get_day(day, month, year):
    min_ts = mktime(get_tstruct('%02d %02d %d 00:00:00' % (day, month, year))) * 1000
    max_ts = (mktime(get_tstruct('%02d %02d %d 00:00:00' % (day, month, year))) + 86400.0) * 1000

    return [
        get_snowflake(int(min_ts)),
        get_snowflake(int(max_ts))
    ]


def safe_name(folder):
    output = ""

    for char in folder:
        if char in '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_+=.,`~!@#$%^&':
            output = '%s%s' % (output, char)

    return output


def create_query_body(**kwargs):
    query = ""

    for key, val in kwargs.items():
        if val == True and key != 'nsfw':
            query = '%s&has=%s' % (query, key[0:-1])

        if key == 'nsfw':
            query = '%s&include_nsfw=%s' % (query, str(val).lower())

    return query

if version_info.major == 3:
    from http.client import HTTPSConnection

    class Request:
        def __init__(self, headers={}):
            self.headers = headers

        def grab_page(self, url, binary=False):
            try:
                domain, path = py3_url_split(url)
                conn = HTTPSConnection(domain, 443)
                conn.request('GET', path, headers=self.headers)

                resp = conn.getresponse()
                if str(resp.status)[0] == '2':
                    return resp.read() if binary else json_to_array(resp.read())
                else:
                    stderr.write('\nReceived HTTP %s error: %s' % (resp.status, resp.reason))

            except Exception as e:
                stderr.write('\nUnknown exception occurred when grabing page contents.')

elif version_info.major == 2 and version_info.minor >= 7:
    from urllib.request import build_opener, install_opener, urlopen, HTTPError

    class Request:
        def __init__(self, headers={}):
            self.headers = headers

        def grab_page(self, url, binary=False):
            try:
                opener_headers = []

                for key, val in self.headers.items():
                    opener_headers.append((key, val.encode('iso-8859-1')))

                opener = build_opener()
                opener.addheaders = opener_headers
                install_opener(opener)

                return urlopen(url).read() if binary else json_to_array(urlopen(url).read())

            except HTTPError as err:
                stderr.write('\nReceived HTTP %s error: %s' % (err.code, err.reason))

            except Exception as e:
                stderr.write('\nUnknown exception occurred when grabing page contents.')

else:
    stderr.write('\nPython %s.%s is not supported in this script.' % (version_info.major, version_info.minor))
    exit(1)


class DiscordScraper:
    def __init__(self, jsonfile='discord.json'):
        with open(jsonfile, 'r') as config_file:
            config = json_to_array(config_file.read())

        self.headers = {
            'user-agent': config['agent'],
            'authorization': config['token']
        }

        self.types = config['types']
        self.query = create_query_body(
            images=config['query']['images'],
            files=config['query']['files'],
            embeds=config['query']['embeds'],
            links=config['query']['links'],
            videos=config['query']['videos'],
            nsfw=config['query']['nsfw']
        )

        self.directs = config['directs']
        self.servers = config['servers']
        self.channels = []
        for server in self.servers:
            #assume channels is a list per server, collect all in one flat list
            self.channels.extend(self.servers[server])
        if S3_ENDPOINT_URL is not None:
            #for local testing using min.io
            s3 = boto3.client("s3", endpoint_url = S3_ENDPOINT_URL)
        else:
            s3 = boto3.client("s3")
        self.s3 = s3


    def get_server_name_by_id(self, server):
        try:
            request = Request(self.headers)
            server_data = request.grab_page('https://discordapp.com/api/v6/guilds/%s' % server)

            if len(server_data) > 0:
                return safe_name(server_data['name'])
            else:
                stderr.write('\nUnable to fetch server name from id, defaulting to a randomly generated name instead.')
                return random_string(12)
        except:
            stderr.write('\nUnable to fetch server name from id, defaulting to a randomly generated name instead.')
            return random_string(12)

    def get_channel_name_by_id(self, channel):
        try:
            request = Request(self.headers)
            channel_data = request.grab_page('https://discordapp.com/api/v6/channels/%s' % channel)

            if len(channel_data) > 0:
                return safe_name(channel_data['name'])
            else:
                stderr.write('\nUnable to fetch channel name from id, defaulting to a randomly generated name instead.')
                return random_string(12)
        except:
            stderr.write('\nUnable to fetch channel name from id, defaulting to a randomly generated name instead.')
            return random_string(12)

    def create_folders(self, server, channel):
        folder = path.join(getcwd(), 'Discord Scrapes', server, channel)

        if not path.exists(folder):
            makedirs(folder)

        return folder

    def download(self, url, folder):
        try:
            request = Request(
                {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                 'cookie': '__cfduid=d13e75ae0431ec770f2a0e1ca6e73e8d71560897192'})
            filename = safe_name('%s_%s' % (url.split('/')[-2], url.split('/')[-1]))

            binary_data = request.grab_page(url, True)
            if len(binary_data) > 0:
                with open(path.join(folder, filename), 'wb') as bin:
                    bin.write(binary_data)
            else:
                stderr.write('\nFailed to grab contents of %s' % url)
        except:
            stderr.write('\nFailed to grab contents of %s' % url)

    def grab_data(self, latest_epochs, write_file=False):
        msgs = []
        header = ["server_id", "server_name", "channel_id", "channel_name", "message_id", "message_txt", "timestamp", "epoch", "user_nick", "discriminator"]
        msgs.append(header)
        tzdata = gmtime(time()-86400) #-86400 is for seconds in 24 hours. so this always scrapes until end of previous day

        year_now = tzdata.tm_year
        month_now = tzdata.tm_mon
        day_now = tzdata.tm_mday

        stop_scrape = False
        processed = 0
        logging.info(f"scraping servers: {self.servers.keys()}")
        for server_id in self.servers.keys() :
            if stop_scrape: break
            server_name = self.get_server_name_by_id(server_id)
            logging.info(f"scraping server {server_id}:{server_name}")
            for channels in self.servers.values():
                if stop_scrape: break
                channel_done = False
                for channel_id in channels:
                    channel_id = int(channel_id)
                    if stop_scrape or channel_done: break
                    channel_name = self.get_channel_name_by_id(channel_id)
                    channel_epoch = latest_epochs[channel_id]
                    logging.info(f"scraping channel {channel_id}:{channel_name}")

                    for year in range(tzdata.tm_year, 2015, -1):
                        if stop_scrape or channel_done: break

                        for month in range(12, 0, -1):
                            if stop_scrape or channel_done: break

                            for day in range(31, 0, -1):
                                logging.info(f"year: {year} month: {month} day: {day}")
                                if month > tzdata.tm_mon and year == tzdata.tm_year: continue
                                if month == tzdata.tm_mon and day > tzdata.tm_mday and year == tzdata.tm_year: continue
                                if processed < 0: # > 1
                                    #this is just to make it possible to stop after first scrape for testing
                                    stop_scrape = True
                                    break

                                t = (year, month, day, 0, 0, 0, 0, 0, 0)
                                this_epoch = mktime(t)

                                if this_epoch < channel_epoch:
                                    channel_done = True
                                    break
                                processed += 1

                                try:
                                    min_id, max_id = get_day(day, month, year)
                                    request = Request(self.headers)
                                    query = self.query
                                    contents = request.grab_page(
                                        'https://discordapp.com/api/v6/guilds/%s/messages/search?channel_id=%s&min_id=%s&max_id=%s&%s' % (
                                        server_id, channel_id, min_id, max_id, query))
                                    logging.info("grabbed messages:" + str(len(contents["messages"])))
                                    for messages in contents['messages']:
                                        for message in messages:
                                            # print("msg: {message}")
                                            message_id = message['id']
                                            message_txt = message['content']  # .replace(',', ';').replace('\n', ' ')
                                            author_uname = message['author']['username']  # .replace(',', ';')
                                            author_nick = message['author']['discriminator']  # number after username
                                            timestamp = message['timestamp']
                                            utc_time = datetime.fromisoformat(timestamp)
                                            epoch = (utc_time - datetime(1970, 1, 1, tzinfo=utc_time.tzinfo)).total_seconds()
                                            epoch = int(epoch)
                                            msg_data = [server_id, server_name, channel_id, channel_name, message_id, message_txt, timestamp, epoch, author_uname,
                                                        author_nick]
                                            msgs.append(msg_data)

                                except ValueError as ex:
                                    #ValueError comes when the day is out of range for month, so this skips invalid dates
                                    print(ex)
                                    pass

                                except Exception as ex:
                                    print(ex)
                                    pass
        if len(msgs) == 1:
            logging.info(f"skipping msgs upload/save - seems no new messages. count={len(msgs)}")
            return
        output = io.StringIO()
        csv_w = csv.writer(output)
        csv_w.writerows(msgs)
        csv_txt = output.getvalue()
        log_filename = f'{S3_PREFIX}messages-{year_now}-{month_now:02d}-{day_now:02d}.csv'
        if write_file:
            with open(log_filename, "w") as log_file:
                log_file.write(csv_txt)

        self.upload_to_s3(csv_txt, log_filename)

    def upload_to_s3(self, file, path, count=0):
        partial_list = self.s3.list_objects_v2(
            Bucket = BUCKET_NAME,
            Prefix = S3_PREFIX)
        logging.info(f"loaded object list:{partial_list}")
        obj_list = partial_list['Contents']
        obj_list = [obj["Key"] for obj in obj_list]
        for obj in obj_list:
            if path in obj:
                if count > 2:
                    logging.info("too many tries with _ appending, skipping this upload.")
                    return
                new_path = path.split(".csv")
                new_path = new_path[0]+"_.csv"
                logging.info(f"object with name {path} already exists. trying to append _ and redo with {new_path}.")
                return self.upload_to_s3(file, new_path, count+1)

        print(f"uploading {len(file)} bytes to {path}")
        fileobj = BytesIO(file.encode("utf-8"))
        self.s3.upload_fileobj(fileobj, BUCKET_NAME, path)
        print("upload done")


    def create_discord_logs_bucket_if_not_exists(self):
        logging.info("listing s3 buckets")
        #bucket creation should just return if it exists (link says so)
        #https://stackoverflow.com/questions/26871884/how-can-i-easily-determine-if-a-boto-3-s3-bucket-resource-exists/47565719#47565719
        from botocore.exceptions import ClientError
        location = os.environ['BUCKET_LOCATION']

        try:
            #https://stackoverflow.com/questions/33068055/boto3-python-and-how-to-handle-errors
            self.s3.create_bucket(Bucket=BUCKET_NAME,
                             ACL="private",
                             CreateBucketConfiguration = {
                                 'LocationConstraint': location
                             }
                             )
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print("Bucket already exists")
            else:
                print(f"Unexpected error: {e}")

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # root.addHandler(handler)

def is_aws():
    return os.environ.get("AWS_EXECUTION_ENV") is not None

if not is_aws():
    S3_PREFIX = ""

def get_latest_epochs():
    if is_aws():
        arn = os.environ['ARN_LAMBDA_TIMESTAMPER']
        lb = boto3.client("lambda")
        response = lb.invoke(FunctionName = arn, InvocationType = 'RequestResponse')
        response_data = response["Payload"].read().decode("utf-8")
        latest_epochs = json.loads(response_data)["latest_epochs"]
        latest_epochs = {int(k): int(v) for k, v in latest_epochs.items()}
    else:
        import db_writer
        dbw = db_writer.DBWriter()
        dbw.init_data()
        latest_epochs = dbw.fetch_latest_epochs()
        dbw.close()
    return latest_epochs

def start_db_writer():
    if is_aws():
        lb = boto3.client("lambda")
        arn = os.environ['ARN_LAMBDA_DB_WRITER']
        lb.invoke(FunctionName = arn, InvocationType = 'Event')
    else:
        import db_writer
        db_writer.insert_msgs()

def handler(event, context):
    setup_logging()
    logging.info("logging set up, creating scraper")
    ds = DiscordScraper()
    #ds.upload_to_s3("lkjsdkfl", "hop/bob.txt")
    logging.info("scraper created, creating bucket if needed")
    ds.create_discord_logs_bucket_if_not_exists()
    logging.info("bucket done, looking for data")

    latest_epochs = get_latest_epochs()

    ds.grab_data(latest_epochs, write_file = False)

    start_db_writer()


#if __name__ == '__main__':
#    handler(None, None)
