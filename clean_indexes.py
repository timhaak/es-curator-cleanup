from datetime import date, datetime
from elasticsearch import Elasticsearch
import re
from dotenv import load_dotenv, find_dotenv
import os
import sys
from colorama import Fore, Style
import redis
from rq import Queue
from locallib import ConsolidateIndex

load_dotenv(find_dotenv())

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

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_DB = os.getenv("REDIS_DB", "0")

WORKER_TIMEOUT = int(os.getenv("WORKER_TIMEOUT", "3600"))
WORKER_QUEUE_TIMEOUT = int(os.getenv("WORKER_QUEUE_TIMEOUT", "86400"))
WORKER_RESULT_TIMEOUT = int(os.getenv("WORKER_RESULT_TIMEOUT", "86400"))
WORKER_LOGGING_LEVEL = os.getenv("WORKER_LOGGING_LEVEL", "INFO")


def createJob(
    es_server_host='localhost',
    es_server_port='9200',
    es_server_username='',
    es_server_password='',
    max_days=3,
    max_indexes=1,
    max_sub_index=1,
    index_prefix='',
    redis_host='redis',
    redis_port='6379',
    redis_db='0',
):
    es_server_port = str(es_server_port);

    if es_server_host == '' or es_server_port == '':
        print("You need to set the env variables for EsServer and EsServerPort")
        sys.exit()

    es_server_url = "http://" + es_server_host + ":" + es_server_port + "/"

    print("Connecting to " + Fore.GREEN + es_server_url + Style.RESET_ALL)
    print("Looking for indexes older than " +
          Fore.RED + str(max_days) + Style.RESET_ALL + ' days')

    if index_prefix != '':
        print("Only looking for indexes that start with " +
              Fore.MAGENTA + index_prefix + Style.RESET_ALL)

    if max_indexes < 0:
        print("Doing all indexes")
    else:
        print("Doing " + str(max_indexes) + " indexes")

    if max_sub_index < 0:
        print("Doing all sub indexes")
    else:
        print("Doing " + str(max_sub_index) + " sub indexes per index. Done on worker")

    if es_server_username == "":
        es = Elasticsearch(
            es_server_url
        )
    else:
        es = Elasticsearch(
            es_server_url,
            http_auth=(es_server_username, es_server_password)
        )

    indexes = es.indices.get_alias(index=index_prefix + '*').keys()
    sorted_indexes = sorted(indexes)
    now = datetime.now().date()
    index_list = {}

    for index in sorted_indexes:
        match_regular_expression = r"(" + re.escape(index_prefix) + \
                                   ".*)-(\d+)\.(\d+)\.(\d+)"
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
                month_index = match.group(1)
                if month_index not in index_list:
                    index_list[month_index] = 1
                else:
                    index_list[month_index] += 1
            if -1 < max_indexes < len(index_list):
                break

    print(
        'Connecting to  ' + Fore.CYAN + redis_host + Style.RESET_ALL + ' redis on port ' +
        Fore.GREEN + redis_port + Style.RESET_ALL
    )

    redis_conn = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    queue = Queue(
        default_timeout=WORKER_TIMEOUT,
        connection=redis_conn
    )

    for month_index in index_list:
        print('Creating job to consolidate ' + Fore.BLUE + month_index + Style.RESET_ALL + ' with ' +
              Fore.BLUE + str(index_list[month_index]) + Style.RESET_ALL + ' indexes to consolidate')

        # print([
        #     ConsolidateIndex,
        #     es_server_host,
        #     es_server_port,
        #     es_server_username,
        #     es_server_password,
        #     max_days,
        #     max_indexes,
        #     max_sub_index,
        #     month_index
        # ])

        job = queue.enqueue_call(
            func=ConsolidateIndex.consolidate_index,
            args=(
                es_server_host,
                es_server_port,
                es_server_username,
                es_server_password,
                max_days,
                max_indexes,
                max_sub_index,
                month_index,
                LOG_LEVEL
            ),
            timeout=WORKER_TIMEOUT,
            result_ttl=WORKER_RESULT_TIMEOUT,
            ttl=WORKER_QUEUE_TIMEOUT
        )

        print(job.result)


createJob(
    ES_SERVER,
    ES_SERVER_PORT,
    ES_SERVER_USERNAME,
    ES_SERVER_PASSWORD,
    MAX_DAYS,
    MAX_INDEXES,
    MAX_SUB_INDEXES,
    FILTER_PREFIX,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB
)
