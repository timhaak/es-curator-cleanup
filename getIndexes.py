from datetime import date, datetime
from elasticsearch import Elasticsearch
import re
import yaml
from dotenv import load_dotenv, find_dotenv
import os
import sys

CONFIG_FILE_NAME = 'curator_config.yml'
CURATOR_ACTION = 'curator_action.yml'

load_dotenv(find_dotenv())

MAX_DAYS = int(os.getenv("MAX_DAYS", 3))

# Set less than 0 to do all
MAX_INDEXES = int(os.getenv("MAX_INDEXES", -1))

# Set less than 0 to do all
MAX_SUB_INDEXES = int(os.getenv("MAX_SUB_INDEXES", -1))

# Set less than 0 to do all
ES_SERVER = os.getenv("ES_SERVER", '')
ES_SERVER_PORT = os.getenv("ES_SERVER_PORT", '')

# Set less than 0 to do all
ES_SERVER_USERNAME = os.getenv("ES_SERVER_USERNAME", "")
ES_SERVER_PASSWORD = os.getenv("ES_SERVER_PASSWORD", "")

if ES_SERVER == '' or ES_SERVER_PORT == '':
    print("You need to set the env variables for ES_SERVER and ES_SERVER_PORT")
    sys.exit()

ES_SERVER_URL = "http://" + ES_SERVER + ":" + ES_SERVER_PORT + "/"

print("Connecting to " + ES_SERVER_URL)
print("Looking for indexes older than " + str(MAX_DAYS) + ' days')

if MAX_INDEXES > 0:
    print("Doing all indexes")
else:
    print("Doing " + str(MAX_INDEXES) + " indexes")

if MAX_INDEXES > 0:
    print("Doing all sub indexes")
else:
    print("Doing " + str(MAX_SUB_INDEXES) + " sub indexes per index")

if ES_SERVER_USERNAME == "":
    es = Elasticsearch(
        ES_SERVER_URL
    )
else:
    es = Elasticsearch(
        ES_SERVER_URL,
        http_auth=(ES_SERVER_USERNAME, ES_SERVER_PASSWORD)
    )

indexes = es.indices.get_alias().keys()
sorted_indexes = sorted(indexes)
now = datetime.now().date()
index_list = {}

for index in sorted_indexes:
    regexp = re.compile(r'(.*)-(\d+)\.(\d+)\.(\d+)')
    match = regexp.search(index)
    if match:
        index_date = date(
            int(match.group(2)),
            int(match.group(3)),
            int(match.group(4))
        )
        delta = now - index_date
        if delta.days > MAX_DAYS:
            # print(index)
            month_index = match.group(1) + '-' + match.group(2) + '.' + \
                match.group(3)
            # print(month_index)
            if month_index in index_list:
                if MAX_SUB_INDEXES <= 0 or \
                    len(index_list[month_index]) < \
                        MAX_SUB_INDEXES:
                    index_list[month_index].append(index)
            else:
                index_list[month_index] = [index]
            if 0 < MAX_INDEXES <= len(index_list):
                break

data = {'actions': {}}
index_count = 1
for month_index in index_list:
    data['actions'][index_count] = {
        'description': 'Create target index ' + month_index,
        'action': 'create_index',
        'options': {
            'disable_action': False,
            'name': month_index,
            'continue_if_exception': True,
        }
    }

    index_count += 1

    data['actions'][index_count] = {
        'description': '\nReindex \n\t' + str(index_list[month_index]) +
        '\n to \n\t' + month_index,
        'action': 'reindex',
        'options': {
            'disable_action': False,
            'continue_if_exception': False,
            'ignore_empty_list': True,
            'timeout': 90,
            'wait_interval': 9,
            'max_wait': -1,
            'requests_per_second': -1,
            'slices': 3,
            'request_body': {
                'source': {
                    'index': index_list[month_index]
                },
                'dest': {
                    'index': month_index
                }
            }
        },
        'filters': [
            {'filtertype': 'none'}
        ]
    }

    index_count += 1

    for day_index in index_list[month_index]:
        data['actions'][index_count] = {
            'description': 'Delete index ' + day_index +
            ' moved to ' + month_index,
            'action': 'delete_indices',
            'options': {
                'disable_action': False,
                'continue_if_exception': False,
                'ignore_empty_list': True,
                'timeout_override': None,
            },
            'filters': [
                {
                    'filtertype': 'pattern',
                    'kind': 'regex',
                    'value': '^' + re.escape(day_index) + '$',
                }
            ]
        }
        index_count += 1
        data['actions'][index_count] = {
            'description': 'Delete index ' + day_index +
            ' moved to ' + month_index,
            'action': 'delete_indices',
            'options': {
                'disable_action': False,
                'continue_if_exception': True,
                'ignore_empty_list': True,
                'timeout_override': None,
            },
            'filters': [
                {
                    'filtertype': 'pattern',
                    'kind': 'regex',
                    'value': '^' + re.escape(day_index) + '$',
                }
            ]
        }
        index_count += 1

with open(CURATOR_ACTION, 'w') as outfile:
    yaml.dump(data, outfile, default_flow_style=False)

if ES_SERVER_USERNAME != '':
    HTTP_AUTH_CREDENTIALS = ES_SERVER_USERNAME + ":" + ES_SERVER_PASSWORD
else:
    HTTP_AUTH_CREDENTIALS = ''

curator_config = {
    'client': {
        'hosts': [ES_SERVER],
        'port': ES_SERVER_PORT,
        'url_prefix': None,
        'use_ssl': False,
        'certificate': None,
        'client_cert': None,
        'client_key': None,
        'ssl_no_validate': False,
        'http_auth': HTTP_AUTH_CREDENTIALS,
        'timeout': 30,
        'master_only': False,
    },
    'logging': {
        'loglevel': 'INFO',
        'logfile': None,
        'logformat': 'default',
        'blacklist': ['elasticsearch', 'urllib3'],
    }
}

with open(CONFIG_FILE_NAME, 'w') as outfile:
    yaml.dump(curator_config, outfile, default_flow_style=False)
