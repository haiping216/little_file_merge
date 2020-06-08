# -*- coding:utf-8 -*-

import os
import sys

from datetime import datetime
from hdfs.client import Client
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext

from little_file_config import *
from little_file_insert import insert_files
from little_file_scan import scan_files, scan_summary, get_table_partitions, get_table_format

#################### settings ####################
## pass in command line
#################### config ####################
conf = SparkConf()
conf = conf.setAppName("little_file_merge")
sc = SparkSession.builder.config(conf=conf).getOrCreate().sparkContext
sqlContext = SQLContext(sc)

def src_dst_path(dbName, tableName):
    partitions = get_table_partitions(dbName, tableName)
    partitions_len = len(partitions)
    src_path = "/user/hive/warehouse/{}.db/{}/".format(dbName, tableName)
    dst_path = "/tmp/hive/warehouse/{}.db/{}/".format(dbName, tableName)
    if partitions_len == 0:
        pass
    elif partitions_len == 1:
        src_path += "%s/"
        dst_path += "%s/"
    elif partitions_len == 2:
        src_path += "%s/%s/"
        dst_path += "%s/%s/"
    elif partitions_len == 3:
        src_path += "%s/%s/%s/"
        dst_path += "%s/%s/%s/"
    else:
        print("error partitions length:{}".format(partitions_len))

    logger.info(src_path)
    logger.info(dst_path)
    return src_path, dst_path

def merge_operate(src_path, dst_path, fileNum, fileFormat):
    logger.info(src_path)
    logger.info(dst_path)
    logger.info(fileNum)

    df = sqlContext.read.load(path=src_path, format=fileFormat)
    logger.info(df.rdd.getNumPartitions())
    df.repartition(fileNum).write.save(path=dst_path, format=fileFormat, mode='overwrite')

    cmd  = "hdfs dfs -rm -r -f {}; ".format(src_path)   # remove src_path
    cmd += "hdfs dfs -mkdir -p {}; ".format(src_path)   # mkdir src_path
    cmd += "hdfs dfs -cp {}/*.orc {}".format(dst_path, src_path)    # copy dst_path to src_path
    logger.info(cmd)
    os.system(cmd)

def merge_files(dbName, tableName):
    #### initialize the mysql connection
    cnn = mysql.connect(**data_analysis_config)
    cursor = cnn.cursor()

    ## get pending merge progress
    sql = """
        SELECT 
            dbName, 
            tableName, 
            partition1, 
            partition2, 
            partition3, 
            newFileNum
        FROM  merge_progress 
        where status=0
    """
    logger.info(sql)
    cursor.execute(sql)
    cnn.commit()

    pending_list = []
    for (dbName, tableName, partition1, partition2, partition3, newFileNum) in cursor:
        pending_list.append([dbName, tableName, partition1, partition2, partition3, newFileNum])


    fileFormat = get_table_format(dbName, tableName)
    src_path, dst_path = src_dst_path(dbName, tableName)
    partitions_len = len(get_table_partitions(dbName, tableName))
    for i in range(len(pending_list)):
        if partitions_len == 0:
            pass
        elif partitions_len == 1:
            new_src_path = src_path % (partition1)
            new_dst_path = dst_path % (partition1)
        elif partitions_len == 2:
            new_src_path = src_path % (partition1, partition2)
            new_dst_path = dst_path % (partition1, partition2)
        elif partitions_len == 3:
            new_src_path = src_path % (partition1, partition2, partition3)
            new_dst_path = dst_path % (partition1, partition2, partition3)

        dbName = pending_list[i][0]
        tableName = pending_list[i][1]
        partition1 = pending_list[i][2]
        partition2 = pending_list[i][3]
        partition3 = pending_list[i][4]
        newFileNum = pending_list[i][5]


        logger.info('--start:' + dbName + '--' + tableName + '--' + partition1 
            + '--' + partition2 + '--' + partition3 + '--' + str(newFileNum))
        # continue

        # update merge status
        update_sql = """
            UPDATE merge_progress set status=1 
            where dbName='{}' and tableName='{}' and partition1='{}' and partition2='{}' and partition3='{}'
        """.format(dbName, tableName, partition1, partition2, partition3)

        logger.info("new_src_path:" + new_src_path)
        logger.info("new_dst_path:" + new_dst_path)
        logger.info("update_sql:" + update_sql)
        cursor.execute(update_sql)
        cnn.commit()

        # merge little files
        merge_operate(new_src_path, new_dst_path, newFileNum, fileFormat)

        # update merge status
        sql_update_progress = """
            UPDATE merge_progress set status=2
            where dbName='{}' and tableName='{}' and partition1='{}' and partition2='{}' and partition3='{}'
        """.format(dbName, tableName, partition1, partition2, partition3)
        logger.info(sql_update_progress)
        cursor.execute(sql_update_progress)
        cnn.commit()

        logger.info('--end:' + dbName + '--' + tableName + '--' + partition1 
            + '--' + partition2 + '--' + partition3 + '--' + str(newFileNum))


def execute():
    logger.info('##### S T A R T I N G : ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' #####')

    
    lines = open("./need_merge.txt", 'r').readlines()
    for line in lines:
        dbName = line.split('.')[0].strip()
        tableName = line.split('.')[1].strip()
        logger.info("dbName={}, tableName={}".format(dbName, tableName))

        # scan files which need to merge
        scan_summary(dbName, tableName)

        # insert to db
        insert_files(dbName, tableName)

        # merge files, update status
        merge_files(dbName, tableName)
    
    logger.info('##### ENDING : ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' #####')

    # stop sparkContext
    sc.stop()

if __name__ == "__main__":
    execute()
    
