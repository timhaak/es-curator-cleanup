import os
import re
import subprocess
import sys
import time

from datetime import date, datetime

import yaml
from celery import Celery
from colorama import Fore, Style
from dotenv import load_dotenv, find_dotenv
from elasticsearch import Elasticsearch

load_dotenv(find_dotenv())

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_DB = os.getenv("REDIS_DB", "0")

WORKER_TIMEOUT = int(os.getenv("WORKER_TIMEOUT", "3600"))
WORKER_QUEUE_TIMEOUT = int(os.getenv("WORKER_QUEUE_TIMEOUT", "86400"))
WORKER_RESULT_TIMEOUT = int(os.getenv("WORKER_RESULT_TIMEOUT", "86400"))
WORKER_LOGGING_LEVEL = os.getenv("WORKER_LOGGING_LEVEL", "INFO")

JOB_QUEUE_NAME = os.getenv("JOB_QUEUE_NAME", "curator")

redis_url = "redis://" + REDIS_HOST + ":" + REDIS_PORT + "/" + REDIS_DB
redis_url_backend = "redis://" + REDIS_HOST + ":" + REDIS_PORT + "/1"

app = Celery(JOB_QUEUE_NAME, backend=redis_url, broker=redis_url)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],  # Ignore other content
    result_serializer='json',
    timezone='Africa/Johannesburg',
    enable_utc=True,
    result_backend=redis_url_backend,
    result_expires=WORKER_RESULT_TIMEOUT,
    worker_prefetch_multiplier=5,
    event_queue_expires=5,
    # event_queue_ttl=30,
)

app.conf.broker_transport_options = {
    'visibility_timeout': WORKER_QUEUE_TIMEOUT
}


@app.task
def consolidate_index(
    es_server_host='localhost',
    es_server_port='9200',
    es_server_username='',
    es_server_password='',
    max_days=3,
    max_indexes=1,
    max_sub_index=-1,
    index_prefix='',
    index_shards=4,
    index_replicas=1,
    reindex_slices=4,
    reindex_batch_size=4000,
    log_level='INFO'
):
    curator_config_file = 'curator_config.yml'
    curator_action_file = 'curator_action.yml'

    es_server_port = str(es_server_port)

    if es_server_host == '' or es_server_port == '':
        print("You need to set the env variables for EsServer and EsServerPort")
        sys.exit()

    es_server_url = "http://" + es_server_host + ":" + es_server_port + "/"

    print("Connecting to " + Fore.GREEN + es_server_url + Style.RESET_ALL)
    print("Looking for indexes older than " +
          Fore.RED + str(max_days) + Style.RESET_ALL + ' days')

    if index_prefix != '':
        print("Only looking for indexes sthat start with " +
              Fore.MAGENTA + index_prefix + Style.RESET_ALL)

    if max_indexes > 0:
        print("Doing all indexes")
    else:
        print("Doing " + str(max_indexes) + " indexes")

    if max_indexes > 0:
        print("Doing all sub indexes")
    else:
        print("Doing " + str(max_sub_index) + " sub indexes per index")

    if es_server_username == "":
        es = Elasticsearch(
            es_server_url
        )
    else:
        es = Elasticsearch(
            es_server_url,
            http_auth=(es_server_username, es_server_password)
        )

    indexes = es.indices.get_alias(
        index=index_prefix + '*',
        expand_wildcards='open'
    ).keys()
    sorted_indexes = sorted(indexes)
    now = datetime.now().date()
    index_list = {}

    for index in sorted_indexes:
        match_regular_expression = r"(.*)-(\d+)\.(\d+)\.(\d+)"
        regexp = re.compile(match_regular_expression)
        match = regexp.search(index)
        if match:
            index_date = date(
                int(match.group(2)),
                int(match.group(3)),
                int(match.group(4))
            )
            delta = now - index_date
            if delta.days > max_days:
                # print(index)
                month_index = match.group(1) + '-' + match.group(2) + '.' + \
                              match.group(3)
                # print(month_index)
                if month_index in index_list:
                    if max_sub_index <= 0 or len(index_list[month_index]) < max_sub_index:
                        index_list[month_index].append(index)
                else:
                    index_list[month_index] = [index]
                if 0 < max_indexes <= len(index_list):
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
                'extra_settings': {
                    'settings': {
                        'number_of_shards': index_shards,
                        'number_of_replicas': index_replicas,
                    }
                },
            }
        }

        index_count += 1

        # data['actions'][index_count] = {
        #     'description': 'Make sure target index ' + month_index + ' is open just in case',
        #     'action': 'open',
        #     'options': {
        #         'disable_action': False,
        #         'continue_if_exception': True,
        #         'ignore_empty_list': True,
        #     },
        #     'filters': [
        #         {
        #             'filtertype': 'pattern',
        #             'kind': 'regex',
        #             'value': '^' + re.escape(month_index) + '$',
        #         }
        #     ]
        # }
        #
        # index_count += 1

        data['actions'][index_count] = {
            'description': '\nReindex \n\t' + str(index_list[month_index]) +
                           '\n to \n\t' + month_index,
            'action': 'reindex',
            'options': {
                'disable_action': False,
                'continue_if_exception': False,
                'ignore_empty_list': True,
                'timeout': 300,
                'wait_interval': 9,
                'max_wait': -1,
                'requests_per_second': -1,
                'slices': reindex_slices,
                'wait_for_completion': True,
                'request_body': {
                    'source': {
                        'index': index_list[month_index],
                        'size': reindex_batch_size
                    },
                    'dest': {
                        'index': month_index
                    }
                }
            },
            'filters': [
                {
                    'filtertype': 'closed',
                    'exclude': True
                },
            ]
        }

        index_count += 1

        for day_index in index_list[month_index]:
            data['actions'][index_count] = {
                'description': 'Close index ' + day_index +
                               ' moved to ' + month_index,
                'action': 'close',
                'options': {
                    'disable_action': False,
                    'continue_if_exception': True,
                    'delete_aliases': True,
                    'ignore_empty_list': True,
                    'timeout_override': 300,
                },
                'filters': [
                    {
                        'filtertype': 'pattern',
                        'kind': 'regex',
                        'value': '^' + re.escape(day_index) + '$',
                    },
                ]
            }
            index_count += 1

    with open(curator_action_file, 'w') as outfile:
        yaml.dump(data, outfile, default_flow_style=False)

    if es_server_username != '':
        http_auth_credentials = es_server_username + ":" + es_server_password
    else:
        http_auth_credentials = ''

    curator_config = {
        'client': {
            'hosts': [es_server_host],
            'port': es_server_port,
            'url_prefix': None,
            'use_ssl': False,
            'certificate': None,
            'client_cert': None,
            'client_key': None,
            'ssl_no_validate': False,
            'http_auth': http_auth_credentials,
            'timeout': 300,
            'master_only': False,
        },
        'logging': {
            'loglevel': log_level,
            'logfile': None,
            'logformat': 'default',
            'blacklist': ['elasticsearch', 'urllib3'],
        }
    }

    with open(curator_config_file, 'w') as outfile:
        yaml.dump(curator_config, outfile, default_flow_style=False)

    process = subprocess.Popen(
        '/usr/local/bin/curator --config /es-curator-cleanup/curator_config.yml /es-curator-cleanup/curator_action.yml',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    while True:
        # output = process.stdout.readline()
        # err = process.stderr.readline()
        output, err = process.communicate()
        if process.poll() is not None:
            break
        if output:
            print(output.strip())
        if err:
            print(output.strip())
        time.sleep(5)
        print("Program still running: Pid = " + process.pid)

    print("\n\n\n\nProgram ended\n\n\n\n")
    process.wait()
    rc = process.poll()
    print(rc)
    return rc
