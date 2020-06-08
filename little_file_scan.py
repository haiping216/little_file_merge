import os
import sys
import math
import pandas as pd

from little_file_config import *

def get_table_format(dbName, tableName):
    sql = """
        SELECT
            case 
                when input_format = 'org.apache.hadoop.mapred.TextInputFormat' then 'text'
                when input_format = 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' then 'orc'
                when input_format = 'org.apache.carbondata.hive.MapredCarbonInputFormat' then 'carbon'
                when input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' then 'parquet'
            else 'unknown'
            end as format
        FROM hive.TBLS as tbl
            LEFT JOIN hive.DBS as dbs on dbs.DB_ID = tbl.DB_ID 
            LEFT JOIN SDS as sds on sds.SD_ID = tbl.SD_ID
        WHERE dbs.NAME = '{}'
            AND tbl.TBL_NAME='{}'
    """.format(dbName, tableName)
    logger.info(sql)
    df = pd.read_sql(sql, db_hive_conn.engine())
    if df.empty:
        return 'unknown'
    else:
        return df['format'][0]

def get_table_partitions(dbName, tableName):
    sql = """
        SELECT
            part.PKEY_NAME as part
        FROM hive.TBLS as tbl
            LEFT JOIN hive.DBS as dbs on dbs.DB_ID = tbl.DB_ID 
            LEFT JOIN PARTITION_KEYS as part on part.TBL_ID = tbl.TBL_ID
        WHERE dbs.NAME = '{}'
            AND tbl.TBL_NAME='{}'
    """.format(dbName, tableName)
    logger.info(sql)
    df = pd.read_sql(sql, db_hive_conn.engine())
    if df.empty or df.dropna(axis=0, how='any').empty:
        return []
    else:
        return df["part"].tolist()

def full_table_partitions(dbName, tableName):
    partition_list = get_table_partitions(dbName, tableName)
    partition_list += ['' for i in range(3 - len(partition_list))]
    return partition_list

def add_table_information(dbName, tableName):
    partition_list = get_table_partitions(dbName, tableName)
    partition_list += ['' for i in range(3 - len(partition_list))]
    print(partition_list)

    table_format = get_table_format(dbName, tableName)
    del_sql = """
        DELETE FROM data.little_file_table WHERE dbName='{}' and table='{}'
    """.format(dbName, tableName)

    ins_sql = """
        INSERT INTO data.little_file_table (dbName, tableName, format, partition1, partition2, partition3) 
        values  ('{}', '{}', '{}', '{}', '{}', '{}')
    """.format(dbName, tableName, table_format, partition_list[0], partition_list[1], partition_list[2])
    cursor = db_data_conn.cursor()
    cursor.execute(del_sql)

    cursor.execute(ins_sql)
    db_data_conn.commit()

def scan_files(dbName, tableName):
    partition_list = get_table_partitions(dbName, tableName)
    cmd  = """hdfs dfs -ls /user/hive/warehouse/{}.db/{}/""".format(dbName, tableName)
    logger.info(cmd)
    # print(len(partition_list))
    for i in range(len(partition_list)):
        cmd += """*/"""

    file = os.getcwd() + "/files/{}.{}.csv".format(dbName, tableName)
    cmd += """| grep -v "+tmp" | grep -v "Found.*items" > {}""".format(file)
    logger.info(cmd)
    os.system(cmd)

    cmd = "sed -i 's/  */,/g' {}".format(file)
    logger.info(cmd)
    os.system(cmd)

def  scan_summary(dbName,  tableName):

    scan_files(dbName, tableName)
    
    def get_db(file):
        return file.split('/')[4].split('.')[0]

    def get_table(file):
        return file.split('/')[5]

    def get_partition1(file):
        lt = file.split('/')
        return '' if len(lt) < 8 else lt[6]

    def get_partition2(file):
        lt = file.split('/')
        return '' if len(lt) < 9 else lt[7]

    def get_partition3(file):
        lt = file.split('/')
        return '' if len(lt) < 10 else lt[8]


    def cacul_file_num(row):
        # if not row:   # 使用有误，row是Series，不是dict
        if row.empty:
            return 0

        return math.ceil(row['totalFileSize'] / (128 * 1024 * 1024.0))

    df = pd.read_csv(os.getcwd() + "/files/{}.{}.csv".format(dbName, tableName), 
        names=['chmod', 'num', 'ownner', 'group', 'size', 'date', 'time', 'file'])
    df['dbName'] = df['file'].apply(get_db)
    df['tableName'] = df['file'].apply(get_table)
    df['partition1'] = df['file'].apply(get_partition1)
    df['partition2'] = df['file'].apply(get_partition2)
    df['partition3'] = df['file'].apply(get_partition3)

    columns = ['size', 'file', 'dbName', 'tableName', 'partition1', 'partition2', 'partition3']
    df = df[['size', 'file', 'dbName', 'tableName', 'partition1', 'partition2', 'partition3']]
    df = df.groupby(['dbName', 'tableName', 'partition1', 'partition2', 'partition3'], 
        as_index=False).agg({'size' : ['sum'], 'file' : ['count']})
    df.columns = df.columns.droplevel(1)
    df = df.rename(columns={'size':'totalFileSize', 'file':'totalFileNum'})
    df['newFileNum'] = df.apply(cacul_file_num, axis=1)
    df.to_csv(os.getcwd() + '/files/{}.{}.summary.csv'.format(dbName, tableName), header=True, index=False)

if __name__ == "__main__":
    lines = open("./need_merge.txt", 'r').readlines()
    for line in lines:
        dbName = line.split('.')[0].strip()
        tableName = line.split('.')[1].strip()
        scan_summary(dbName, tableName)
    # print(scan_files("xy_snap", "p2p_db_t_gold_product"))
    # print(scan_summary("xy_snap", "p2p_db_t_gold_product"))
    