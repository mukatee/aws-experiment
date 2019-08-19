import mysql.connector as db_conn
from mysql.connector import errorcode
import os

DB_NAME = os.environ['DB_NAME']
user_name = os.environ['DB_USER']
password = os.environ['DB_USER_PW']
root_user = os.environ['DB_ROOT']
root_pw = os.environ['DB_ROOT_PW']
#server_id,server_name,channel_id,channel_name,message_id,message_txt,timestamp,epoch,userid,nickname
TABLES = {}

TABLES['servers'] = """
    CREATE TABLE servers (
      server_id BIGINT UNSIGNED NOT NULL,
      server_name varchar(32)NOT NULL,
      PRIMARY KEY (server_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
"""

TABLES['channels'] = """
    CREATE TABLE channels (
      channel_id BIGINT UNSIGNED NOT NULL,
      server_id BIGINT UNSIGNED NOT NULL,
      channel_name varchar(32) NOT NULL,
      PRIMARY KEY (channel_id), KEY server_channel (server_id, channel_id),
      CONSTRAINT channels_server_fk FOREIGN KEY (server_id)
         REFERENCES servers (server_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """

TABLES['users'] = """
    CREATE TABLE users (
      user_id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
      user_nick VARCHAR(32)NOT NULL,
      discriminator INT(4) UNSIGNED NOT NULL,
      PRIMARY KEY (user_id), UNIQUE KEY user_nick_id (user_nick, discriminator)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """

TABLES['messages'] = """
    CREATE TABLE messages (
      msg_id BIGINT UNSIGNED NOT NULL,
      msg_txt VARCHAR(2000) NOT NULL,
      msg_timestamp DATETIME NOT NULL,
      msg_epoch INT(11) UNSIGNED NOT NULL,
      user_id int(11) UNSIGNED NOT NULL,
      server_id BIGINT UNSIGNED NOT NULL,
      channel_id BIGINT UNSIGNED NOT NULL,
      PRIMARY KEY (msg_id),
      CONSTRAINT msg_user_fk FOREIGN KEY (user_id)
         REFERENCES users (user_id) ON DELETE CASCADE,
      CONSTRAINT msg_server_fk FOREIGN KEY (server_id)
         REFERENCES servers (server_id) ON DELETE CASCADE,
      CONSTRAINT msg_channel_fk FOREIGN KEY (channel_id)
         REFERENCES channels (channel_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """

def create_database(cursor):
    try:
        cursor.execute(f"CREATE USER IF NOT EXISTS '{user_name}'@'%' IDENTIFIED BY '{password}';")
        cursor.execute(f"CREATE DATABASE {DB_NAME} DEFAULT CHARACTER SET 'utf8'")
        cursor.execute(f"GRANT ALL PRIVILEGES ON {DB_NAME}.* TO '{user_name}'@'%' IDENTIFIED BY '{password}';")
    except db_conn.Error as err:
        print("Failed creating database: {}".format(err))
        #exit(1)

def handler(event, context):
    cnx = db_conn.connect(user=root_user, password=root_pw)
    cursor = cnx.cursor()

    try:
        cursor.execute("USE {}".format(DB_NAME))
    except db_conn.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            #exit(1)

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except db_conn.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cursor.close()
    cnx.close()

