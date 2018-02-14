from datetime import date, datetime
from elasticsearch import Elasticsearch
import re
import yaml
import sys
from colorama import Fore, Style
import subprocess


def consolidate_index(
    es_server_host='localhost',
    es_server_port='9200',
    es_server_username='',
    es_server_password='',
    max_days=3,
    max_indexes=1,
    max_sub_index=1,
    index_prefix=''
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

    indexes = es.indices.get_alias().keys()
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
                'timeout': 300,
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
                    'timeout_override': 300,
                },
                'filters': [
                    {
                        'filtertype': 'pattern',
                        'kind': 'regex',
                        'value': '^' + re.escape(day_index) + '$',
                    },
                    {
                        'filtertype': 'pattern',
                        'kind': 'prefix',
                        'value': day_index,
                    }
                ]
            }
            index_count += 1

    with open(CURATOR_ACTION, 'w') as outfile:
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
            'loglevel': LOG_LEVEL,
            'logfile': None,
            'logformat': 'default',
            'blacklist': ['elasticsearch', 'urllib3'],
        }
    }

    with open(CONFIG_FILE_NAME, 'w') as outfile:
        yaml.dump(curator_config, outfile, default_flow_style=False)

    result = subprocess.Popen(
        '/usr/local/bin/curator --config /es-curator-cleanup/curator_config.yml /es-curator-cleanup/curator_action.yml',
        shell=True,
        stdout=subprocess.PIPE
    )
    while True:
        out = result.stderr.read(1)
        if out == '' and result.poll() is None:
            break
        if out != '':
            sys.stdout.write(out)
            sys.stdout.flush()