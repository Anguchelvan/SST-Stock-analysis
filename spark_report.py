from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import yfinance as yf
from datetime import datetime
spark = SparkSession.builder.getOrCreate()

sqlContext = SQLContext(spark.sparkContext)
import configparser

#import config

config = configparser.RawConfigParser()
config.read('stock_config.properties')
conf = dict(config.items(section='report'))
start_date = conf['start_date']
end_date = conf['end_date']

query = "(SELECT * FROM stock.stock_hist where Date >= '"+start_date+"' and Date <= '"+end_date+"') as stock_hist"

stock_df = sqlContext.read.format('jdbc').options(url='jdbc:mysql://localhost/stock', dbtable=query,driver='com.mysql.jdbc.Driver',user='root',password='angu123').load()

#stock_df = temp_df.filter(col('Date') >=start_date).filter(col('Date') <=end_date)
print('time filtered stck')
stock_df.show()

def drill_down_sector(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill_res=drill.groupBy(col('Quote')).agg(avg('Stock Splits').alias('Avg Stock Splits'),max('Volume').alias('Max Volume'),max('Dividends').alias('Max Dividends'),max('Stock Splits').alias('Max Stock Split'),min('Volume').alias('Min Volume'),min('Dividends').alias('Min Dividends'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_drill',user='root',password='angu123').mode('overwrite').save()
	
def drill_down_sector_avg_split(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(avg('Stock Splits').alias('Avg Stock Splits'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_avg_split_drill',user='root',password='angu123').mode('overwrite').save()
	
def drill_down_sector_max_volume(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill_res=drill.groupBy(col('Quote')).agg(max('Volume').alias('Max Volume'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_max_volume_drill',user='root',password='angu123').mode('overwrite').save()
	
def drill_down_sector_max_dividends(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill_res=drill.groupBy(col('Quote')).agg(max('Dividends').alias('Max Dividends'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_max_dividends_drill',user='root',password='angu123').mode('overwrite').save()

def drill_down_sector_max_splits(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill_res=drill.groupBy(col('Quote')).agg(max('Stock Splits').alias('Max Stock Split'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_max_splits_drill',user='root',password='angu123').mode('overwrite').save()
	
def drill_down_sector_min_volume(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill_res=drill.groupBy(col('Quote')).agg(min('Volume').alias('Min Volume'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_min_volume_drill',user='root',password='angu123').mode('overwrite').save()
	
def drill_down_sector_min_dividends(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill_res=drill.groupBy(col('Quote')).agg(min('Dividends').alias('Min Dividends'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_min_dividends_drill',user='root',password='angu123').mode('overwrite').save()


def drill_down_sector_quote(stock_df,sec,quote):
	drill = stock_df.filter(col('Sector')==sec).filter(col('Quote') ==quote)
	drill_res=drill.groupBy(col('Quote')).agg(avg('Stock Splits').alias('Avg Stock Splits'),max('Volume').alias('Max Volume'),max('Dividends').alias('Max Dividends'),max('Stock Splits').alias('Max Stock Split'),min('Volume').alias('Min Volume'),min('Dividends').alias('Min Dividends'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_quote_summary',user='root',password='angu123').mode('overwrite').save()

def drill_down_sector_quote_det(stock_df,sec,quote):
	drill = stock_df.filter(col('Sector')==sec).filter(col('Quote') ==quote)
	drill_res = stock_df.select(col('Stock Splits'),col('Volume'),col('Dividends'))
	drill_res.write.format('jdbc').options(url='jdbc:mysql://localhost/stock',driver='com.mysql.jdbc.Driver',dbtable='sector_quote_det',user='root',password='angu123').mode('overwrite').save()

# Summary DF	
stockfinal = stock_df.groupBy(col('Sector')).agg(avg('Stock Splits').alias('Avg Stock Splits'),max('Volume').alias('Max Volume'),max('Dividends').alias('Max Dividends'),max('Stock Splits').alias('Max Stock Split'),min('Volume').alias('Min Volume'),min('Dividends').alias('Min Dividends')).show()

#Drill Down DF sector
drill_down_sector(stock_df,'Technology')
drill_down_sector(stock_df,'Healthcare')

#drill down DF column level
drill_down_sector_avg_split(stock_df,'Technology')
drill_down_sector_avg_split(stock_df,'Healthcare')

drill_down_sector_max_volume(stock_df,'Technology')
drill_down_sector_max_volume(stock_df,'Healthcare')

drill_down_sector_max_dividends(stock_df,'Technology')
drill_down_sector_max_dividends(stock_df,'Healthcare')

drill_down_sector_max_splits(stock_df,'Technology')
drill_down_sector_max_splits(stock_df,'Healthcare')

drill_down_sector_min_volume(stock_df,'Technology')
drill_down_sector_min_volume(stock_df,'Healthcare')

drill_down_sector_min_dividends(stock_df,'Technology')
drill_down_sector_min_dividends(stock_df,'Healthcare')

#drill down ORCL & det
drill_down_sector_quote(stock_df,'Technology','AAPL')
drill_down_sector_quote_det(stock_df,'Technology','AAPL')