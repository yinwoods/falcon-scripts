from configparser import ConfigParser
from sqlalchemy import create_engine


config = ConfigParser()
config.read('./config.ini')

mysql_config = config['mysql']
engine = create_engine(mysql_config['sql_alchemy_conn'])
