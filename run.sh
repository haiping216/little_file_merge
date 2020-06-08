dir=$(cd "$(dirname "$0")";pwd)

# trigger the merge job
spark-submit \
--master yarn \
--queue root.xy_etl \
--name merge_files \
--deploy-mode client \
--executor-memory 2g \
--executor-cores 1 \
--driver-memory 5g \
--driver-java-options="-Droot.logger=ERROR,console" \
--conf "spark.pyspark.driver.python=/home/hdfs/miniconda3/bin/python" \
--conf "spark.pyspark.python=/home/hdfs/miniconda3/bin/python" \
$dir/little_file_merge.py
