from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import yfinance as yf
from datetime import datetime
spark = SparkSession.builder.getOrCreate()


#quote = ['AAPL', 'MSFT', 'SAP', 'SAPGF', 'AVGO', 'FB', 'ORCL','GOOGL']
# AAPL, MSFT, SAP, SAPGF, AVGO, FB, ORCL, GOOGL
def df_recv(i):
	tick = yf.Ticker(i)
	df  = tick.history(period="max")
	df['Date'] = df.index
	df['Sector'] = 'Technology'
	df['Quote'] = i
	return df

	
def df_recv_health(j):
	tick_health = yf.Ticker(j)
	df  = tick_health.history(period="max")
	df['Date'] = df.index
	df['Sector'] = 'Healthcare'
	df['Quote'] = j
	return df
	
jnj =df_recv_health('JNJ')
dfjnj = jnj.to_dict("records")
#jnjn = spark.createDataFrame(dfjnj)
aapl = spark.createDataFrame(df_recv('AAPL'))
msft = spark.createDataFrame(df_recv('MSFT'))
sap = spark.createDataFrame(df_recv('SAP'))
sapgf = spark.createDataFrame(df_recv('SAPGF'))
avgo = spark.createDataFrame(df_recv('AVGO'))
fb = spark.createDataFrame(df_recv('FB'))
orcl = spark.createDataFrame(df_recv('ORCL'))
googl = spark.createDataFrame(df_recv('GOOGL'))
pfe = spark.createDataFrame(df_recv_health('PFE'))
#jnj = spark.createDataFrame(df_recv_health('JNJ'))
temp_df = aapl.union(msft).union(sap).union(sapgf).union(avgo).union(fb).union(orcl).union(googl).union(pfe)
start_date = '2020-01-01'
end_date = '2020-02-01'

stock_df = temp_df.filter(col('Date') >=start_date).filter(col('Date') <=end_date)
print('time filtered stck')
stock_df.show()

def drill_down_sector(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(avg('Stock Splits').alias('Avg Stock Splits'),max('Volume').alias('Max Volume'),max('Dividends').alias('Max Dividends'),max('Stock Splits').alias('Max Stock Split'),min('Volume').alias('Min Volume'),min('Dividends').alias('Min Dividends')).show()
	
def drill_down_sector_avg_split(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(avg('Stock Splits').alias('Avg Stock Splits')).show()
	
def drill_down_sector_max_volume(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(max('Volume').alias('Max Volume')).show()
	
def drill_down_sector_max_dividends(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(max('Dividends').alias('Max Dividends')).show()
	

def drill_down_sector_max_splits(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(max('Stock Splits').alias('Max Stock Split')).show()
	
def drill_down_sector_min_volume(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(min('Volume').alias('Min Volume')).show()
	
def drill_down_sector_min_dividends(stock_df,q):
	drill = stock_df.filter(col('Sector')==q)
	drill.groupBy(col('Quote')).agg(min('Dividends').alias('Min Dividends')).show()

# Summary DF	
stockfinal = stock_df.groupBy(col('Sector')).agg(avg('Stock Splits').alias('Avg Stock Splits'),max('Volume').alias('Max Volume'),max('Dividends').alias('Max Dividends'),max('Stock Splits').alias('Max Stock Split'),min('Volume').alias('Min Volume'),min('Dividends').alias('Min Dividends')).show()

#Drill Down DF
drill_down_sector(stock_df,'Technology')
drill_down_sector(stock_df,'Healthcare')
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



