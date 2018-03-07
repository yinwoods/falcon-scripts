import json
import arrow
import elasticsearch
from configparser import ConfigParser
from elasticsearch import Elasticsearch


base_config = ConfigParser()
base_config.read('/cluster/software/open-falcon/falcon-scripts/nginx_logs/config.ini')

timezone = 'Asia/Shanghai'

# elasticsearch配置
elastic_config = base_config['elastic']
es = Elasticsearch(json.loads(elastic_config['host']), timeout=30)


def query(customer, api):

    start_time = arrow.now().replace(minutes=-10, tzinfo=timezone).format('DD/MMM/YYYY:HH:mm:ssZZ')
    end_time = arrow.now().replace(tzinfo=timezone).format('DD/MMM/YYYY:HH:mm:ssZZ')

    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [{
                    "query_string": {
                        "default_field": "request_referer",
                        "query": f'{api}'
                    }
                }, {
                    "range": {
                        "timelocal": {
                            "gte": f"{start_time}",
                            "lt": f"{end_time}"
                        }
                    }
                }]
            }
        }
    }
    try:
        result = es.search(index=f'{customer}_nginx', body=json.dumps(body))
    except elasticsearch.exceptions.NotFoundError as e:
        return 1
    return result['hits']['total']


if __name__ == '__main__':

    customers = ['kaiyan', 'yijiupi', 'ttgwm', 'zhulong', 'lespark', 'hongdou',
                 'tangdou', 'uplive', 'xinshang', 'dangdang', 'puyun', 'changba']
    apis = [r"*api\/recom*", r"*api\/log*"]

    for customer in customers:
        for api in apis:
            print(customer, api, query(customer, api))
