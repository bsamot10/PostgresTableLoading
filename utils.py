from argparse import ArgumentParser
import psycopg2
import json
import warnings
warnings.filterwarnings("ignore")


def get_connection():
    config_file = open("config.json")
    config_conn_data = json.load(config_file)["connection"]
    config_file.close()
    try:
        connection = psycopg2.connect(database=config_conn_data["database"],
                                      user=config_conn_data["user"],
                                      password=config_conn_data["password"],
                                      host=config_conn_data["host"],
                                      port=config_conn_data["port"])
        return connection

    except Exception as e:
        print(e)
        exit()


class ConfigParser(ArgumentParser):

    def __init__(self):
        super().__init__(prog='postgreSQL', description='Import table to a postgreSQL database')

    def set_arguments(self):
        self.add_argument('-pf', '--path_file', type=str, nargs='?')
        self.add_argument('-sn', '--sheet_name', type=str, default='')
        self.add_argument('-tn', '--table_name', type=str, nargs='?')
        self.add_argument('-vo', '--varchar_only', action='store_true')
        args = self.parse_args()

        return args
