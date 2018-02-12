import html
import os
import re
import sys
import time
from datetime import date, datetime

import yaml
from colorama import Fore, Style
from dotenv import load_dotenv, find_dotenv
from elasticsearch import Elasticsearch

load_dotenv(find_dotenv())

CONFIG_FILE_NAME = 'curator-job-'

FILTER_PREFIX = os.getenv("FILTER_PREFIX", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

MAX_DAYS = int(os.getenv("MAX_DAYS", 3))

# Set less than 0 to do all
MAX_INDEXES = int(os.getenv("MAX_INDEXES", 1))

# Set less than 0 to do all
MAX_SUB_INDEXES = int(os.getenv("MAX_SUB_INDEXES", 1))

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

print("Connecting to " + Fore.GREEN + ES_SERVER_URL + Style.RESET_ALL)
print("Looking for indexes older than " +
      Fore.RED + str(MAX_DAYS) + Style.RESET_ALL + ' days')

if FILTER_PREFIX != '':
    print("Only looking for indexes sthat start with " +
          Fore.MAGENTA + FILTER_PREFIX + Style.RESET_ALL)

if MAX_INDEXES > 0:
    print("Doing all indexes")
else:
    print("Doing " + str(MAX_INDEXES) + " indexes")

while True:
    time.sleep(5)
    print(datetime.now())

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
today = datetime.now()
index_list = {}

for index in sorted_indexes:
    matchRegularExpresion = r"(" + re.escape(FILTER_PREFIX) + \
                            ".*)-(\d+)\.(\d+)\.(\d+)"
    regexp = re.compile(matchRegularExpresion)
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
            month_index = match.group(1)
            if month_index not in index_list:
                index_list[month_index] = 1
            else:
                index_list[month_index] += 1

for month_index in index_list:
    print('Creating job to consolidate ' + Fore.BLUE + month_index + Style.RESET_ALL + ' with ' +
          Fore.BLUE + str(index_list[month_index]) + Style.RESET_ALL + ' indexes to consolidate')

    safeDNSName = html.escape(month_index.replace('_', '-'))
    nameWithDate = safeDNSName + today.strftime('-%Y%m%d%H%M%S')
    createJobYml = {
        'apiVersion': 'batch/v1',
        'kind': 'Job',
        'metadata': {
            'name': 'consolidate-' + nameWithDate,
            'namespace': 'ibs-monitoring',
            'labels': {
                'jobgroup': 'consolidate-indexes',
            }
        },
        'spec': {
            'template': {
                'spec': {
                    'containers': [
                        {
                            'name': 'consolidate-' + nameWithDate,
                            'image': 'artifactory.mmih.biz:5000/es-curator-cleanup',
                            'env': [
                                {
                                    'name': 'MAX_DAYS',
                                    'value': str(MAX_DAYS),
                                },
                                {
                                    'name': 'MAX_INDEXES',
                                    'value': str(1),
                                },
                                {
                                    'name': 'MAX_SUB_INDEXES',
                                    'value': str(-1),
                                },
                                {
                                    'name': 'ES_SERVER',
                                    'value': str(ES_SERVER),
                                },
                                {
                                    'name': 'ES_SERVER_PORT',
                                    'value': str(ES_SERVER_PORT),
                                },
                                {
                                    'name': 'ES_SERVER_USERNAME',
                                    'value': str(ES_SERVER_USERNAME),
                                },
                                {
                                    'name': 'ES_SERVER_PASSWORD',
                                    'value': str(ES_SERVER_PASSWORD),
                                },
                                {
                                    'name': 'FILTER_PREFIX',
                                    'value': str(month_index),
                                }
                            ]
                        }
                    ],
                    'restartPolicy': 'Never',
                }
            },
            'parallelism': 1,
            'backoffLimit': 1
        }
    }
    with open(CONFIG_FILE_NAME + safeDNSName + '.yml', 'w') as outfile:
        yaml.dump(createJobYml, outfile, default_flow_style=False)
