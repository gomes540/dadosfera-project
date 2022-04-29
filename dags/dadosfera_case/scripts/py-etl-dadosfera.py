# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf

# main spark program
if __name__ == '__main__':
    
    # init spark session
    spark = SparkSession \
        .builder \
        .appName("etl-dadosfera-py") \
        .getOrCreate()
        
    # show configured parameters
    print(SparkConf().getAll())
    
    # set log level
    spark.sparkContext.setLogLevel("INFO")
    
    # stop spark session
    spark.stop()