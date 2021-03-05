from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yfinance as yf
from datetime import datetime
spark = SparkSession.builder.getOrCreate()
import pandas as pd
import time

sqlContext = SQLContext(spark.sparkContext)
import configparser

#import config

config = configparser.RawConfigParser()
config.read('stock_config.properties')
conf = dict(config.items(section='quotes'))
ql = conf['ql']
period = conf['period']
stock_daily = conf['stock_daily']
quotes = ql.split(',')

data = yf.download(tickers = quotes,period=period ,group_by = 'ticker',threads = True)

def get_info(t):
	tick = yf.Ticker(t)
	sector = tick.info
	return sector['sector']



frames = []
for q in quotes:
	temp = data.loc[:,q]
	temp.loc[:,'Date'] = temp.index
	temp.loc[:,'Quote'] = q
	temp.loc[:,'Sector'] = get_info(q)
	frames.append(temp)
df = pd.concat(frames)

		
mySchema = temp_schema = StructType([
    StructField('Open', DoubleType(), False),
    StructField('High', DoubleType(), False),
    StructField('Low', DoubleType(), False),
    StructField('Close', DoubleType(), False),
    StructField('Adj Close', DoubleType(), True),
    StructField('Volume', LongType(), True),
    StructField('Date', TimestampType(), True),
	StructField('Quote', StringType(), True),
	StructField('Sector', StringType(), True),
    ])
	
#df = pd.DataFrame(final_data)	
	
stock_df = spark.createDataFrame(df,schema= mySchema)
stock_df.printSchema()
stock_df.show()


stock_df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/stock',
      driver='com.mysql.jdbc.Driver',
      dbtable='stock_hist',
      user='root',
      password='angu123').mode('append').save()

seconds=time.time()
filename = stock_daily+ str(int(seconds))
stock_df.coalesce(1).write.format("com.databricks.spark.csv").save(filename)





