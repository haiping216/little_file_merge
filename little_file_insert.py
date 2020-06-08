# -*- coding:utf-8 -*-

import os
import sys

import pandas as pd
from datetime import datetime
from hdfs.client import Client
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

from little_file_config import *
from little_file_scan import get_table_partitions, get_table_format, full_table_partitions

def insert_files(dbName, tableName):
    cnn = mysql.connect(**data_analysis_config)
    cursor = cnn.cursor()

    try:
        # ## cleanup the history data;
        # logger.info("cleanup db table")
        # sql_cleanup = "truncate table merge_db_table"
        # cursor.execute(sql_cleanup)

        # get and insert table info
        table_format = get_table_format(dbName, tableName)
        partitions = full_table_partitions(dbName, tableName)
        sql = """
            INSERT INTO merge_db_table (dbName, tableName, format, partition1, partition2, partition3)
            values ('{}', '{}', '{}', '{}', '{}', '{}')
        """.format(dbName, tableName, table_format, partitions[0], partitions[1], partitions[2])
        logger.info(sql)
        cursor.execute(sql)

        ## cleanup the history data;
        logger.info("cleanup histroy data")
        sql_cleanup = "truncate table merge_history"
        cursor.execute(sql_cleanup)

        ## back progress data to history data
        sql_backup_progress_history = """
            INSERT INTO merge_history 
            SELECT 
                dbName, 
                tableName, 
                partition1, 
                partition2, 
                partition3,
                totalFileSize,
                totalFileNum,
                newFileNum,
                status,
                CURRENT_TIMESTAMP as createTime 
            FROM merge_progress
        """
        logger.info(sql_backup_progress_history)
        cursor.execute(sql_backup_progress_history)
        cnn.commit()

        ## clean up progress data
        sql_cleanup = "truncate table merge_progress"
        cursor.execute(sql_cleanup)
        cnn.commit()

        ## clean up dir static
        sql_cleanup = "truncate table merge_dir_static"
        cursor.execute(sql_cleanup)
        cnn.commit()

        # insert merge_dir_static
        df = pd.read_csv(os.getcwd() + "/files/{}.{}.summary.csv".format(dbName, tableName)).fillna('')
        df.to_sql(name="merge_dir_static", con=engine, if_exists='append', index=False, chunksize=10000)

        # insert merge_progress
        sql_merge_progress = """
            INSERT INTO merge_progress
            SELECT
                dbName,
                tableName,
                partition1,
                partition2,
                partition3,
                totalFileSize,
                totalFileNum,
                newFileNum,
                0 as status,
                CURRENT_TIMESTAMP as createTime 
            FROM merge_dir_static
            WHERE newFileNum < totalFileNum
        """
        logger.info(sql_merge_progress)
        cursor.execute(sql_merge_progress)
        cnn.commit()
    finally:
        cursor.close()
        cnn.close()


if __name__ == "__main__":
    lines = open("./need_merge.txt", 'r').readlines()
    for line in lines:
        dbName = line.split('.')[0].strip()
        tableName = line.split('.')[1].strip()
        insert_files(dbName, tableName)
    # insert_files('xy_snap', 'p2p_db_t_gold_product')
