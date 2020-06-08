# pip install mysql-connector
import mysql.connector as mysql

from szdata.utils import xylogger
from szdata.utils import xydatabase

data_analysis_config = {
    'host': 'xxx', 
    'port': 3306, 
    'user': 'xxx', 
    'password': 'xxx', 
    'database': 'little_file_merge',
    'charset': 'utf8',
    'buffered': True
}

db_hive_config = {
    "drivername": "mysql+pymysql",
    "host": "xxx",
    "port": 3306,
    "database": "hive",
    "username": "xxx",
    "password": "xxx",
    "query": {"charset": "utf8"}
}

db_hive_conn = xydatabase.DatabaseConnect(db_hive_config)
db_data_conn = mysql.connect(**data_analysis_config)
logger = xylogger.xy_logger("log/little_file_merge.log", stdout=True)

from sqlalchemy import create_engine
engine = create_engine("mysql://xxx:xxx@xxxx/little_file_merge", encoding='utf8', echo=True)