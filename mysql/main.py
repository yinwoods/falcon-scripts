import requests
from sqlalchemy import text

from common import engine


def innodb_status():
    session = engine.connect()
    innodb_status = session.execute(text(
        "SHOW /*!50000 ENGINE */ INNODB STATUS")).fetchone()
    for row in innodb_status:
        print(row)


def slave():
    pass


def send(data):
    url = 'http://localhost:1993/v1/push'
    headers = {"Content-Type: application/json"}
    response = requests.post(url=url, headers=headers)
    return response.json()


if __name__ == '__main__':
    innodb_status()
