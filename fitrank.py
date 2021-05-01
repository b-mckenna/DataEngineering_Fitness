import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)

  """CSV file format!"""
  # member_id, first_name, last_name, calories_burned


  #TODO: connect to HDFS for data
  
  # read in csv file and split each line into words

  tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(",")) #flatmap is necessary because we're returning more than 1 element. 

  # k = member_id, v = calories_burned
  # k = tokenized[0], v = tokenized[3]
  user_cals_entry = tokenized.map(lambda member: member[0],member[3]).reduceByKey(lambda v1,v2:v1 +v2).map(lambda c: (c,c1))

  swap = user_cals_entry.map(lambda swapped: (data[1], data[0])
  # filter out words with fewer than threshold occurrences
  ranked_users = swap.sortByKeys()

  # count characters
  list = ranked_users.collect()
  print repr(list)[1:-1]


  """TO RUN THE APPLICATION on YARN"""
  """--deploy-mode client --executor-memory 1g \
--name fitrank --conf "spark.app.id=fitrank" fitrank.py hdfs://namenode_host:8020/path/to/inputfile.txt"""