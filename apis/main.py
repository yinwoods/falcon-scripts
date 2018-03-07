import json
import time
import requests
import subprocess
from configparser import ConfigParser, BasicInterpolation


def main():

    config = ConfigParser(interpolation=BasicInterpolation())
    config.read('./config.ini')

    agent_conf = config["agent"]
    changbafeed_conf = config['changbafeed']

    command = changbafeed_conf["command"]
    result = subprocess.check_output(command, shell=True).decode('utf8')
    count = int(result.split()[0])

    result = [{
        "endpoint": agent_conf["endpoint"],
        "metric": "request_count",
        "timestamp": int(time.time()),
        "step": 60,
        "value": count,
        "counterType": "GAUGE",
        "tags": f"api={changbafeed_conf['api']}"
    }]
    result = requests.post(agent_conf["address"], data=json.dumps(result))
    print(result.text)


if __name__ == '__main__':
    main()
