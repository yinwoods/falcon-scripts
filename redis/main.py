import re
import json
import time
import socket
import requests
import subprocess
from configparser import ConfigParser


def parse_to_dict(string):
    string = string.decode()
    regex = re.compile(r'(\w+):([0-9]+\.?[0-9]*)\r')
    return dict(regex.findall(string))


def main():

    config = ConfigParser()
    config.read('./config.ini')

    redis_config = config['redis']
    agent_config = config['agent']

    localhost = socket.gethostname()
    timestamp = int(time.time())

    step = 60
    insts_list = [redis_config, ]
    p = []

    monit_keys = [
        ('connected_clients', 'GAUGE'),
        ('blocked_clients', 'GAUGE'),
        ('used_memory', 'GAUGE'),
        ('used_memory_rss', 'GAUGE'),
        ('mem_fragmentation_ratio', 'GAUGE'),
        ('total_commands_processed', 'COUNTER'),
        ('rejected_connections', 'COUNTER'),
        ('expired_keys', 'COUNTER'),
        ('evicted_keys', 'COUNTER'),
        ('keyspace_hits', 'COUNTER'),
        ('keyspace_misses', 'COUNTER'),
        ('keyspace_hit_ratio', 'GAUGE'),
    ]

    for config in insts_list:
        cli, host, port = config['cli'], config['host'], config['port']
        metric = "redis"
        endpoint = localhost
        tags = 'port=%s' % port
        command = f'{cli} -h {host} -p {port} info'.split()

        try:
            output = subprocess.Popen(command, stdout=subprocess.PIPE)
            output = parse_to_dict(output.communicate()[0])
        except Exception as e:
            print(e)
            continue

        for key, vtype in monit_keys:
            # 一些老版本的redis中info输出的信息很少，如果缺少一些我们需要采集的key就跳过
            if key not in output.keys():
                continue
            # 计算命中率
            if key == 'keyspace_hit_ratio':
                try:
                    space_hits = float(output['keyspace_hits'])
                    space_misses = output['keyspace_misses']
                    total_hits = int(space_hits) + int(space_misses)
                    value = space_hits / total_hits
                except ZeroDivisionError:
                    value = 0
            # 碎片率是浮点数
            elif key == 'mem_fragmentation_ratio':
                value = float(output[key])
            else:
                # 其他的都采集成counter，int
                try:
                    value = int(output[key])
                except Exception as e:
                    continue

            i = {
                'metric': '%s.%s' % (metric, key),
                'endpoint': endpoint,
                'timestamp': timestamp,
                'step': step,
                'value': value,
                'counterType': vtype,
                'tags': tags
            }
            p.append(i)

    url = agent_config['url']
    print(json.dumps(p, indent=4))
    headers = {"Content-Type": "application/json"}
    response = requests.post(url=url, headers=headers, data=json.dumps(p))
    print(response.text)


if __name__ == '__main__':
    main()
