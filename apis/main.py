import json
import requests
from configparser import ConfigParser


def changba_feed_recommend():

    config = ConfigParser()
    config.read('./config.ini')
    changba_config = config['changba_feed_recommend']

    url = changba_config['url']
    data = changba_config['data']
    headers = json.loads(changba_config['headers'])

    response = requests.post(url, data=data, headers=headers)
    print(response.json())
    print(response.elapsed.microseconds)
    print(response.status_code)


if __name__ == '__main__':
    changba_feed_recommend()
