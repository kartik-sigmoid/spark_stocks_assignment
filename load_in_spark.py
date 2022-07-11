from pyspark.sql import SparkSession


def load_in_spark():
    spark = SparkSession.builder.appName('Stock Project').getOrCreate()
    spark_df = spark.read.csv('stock_data/*.csv', sep=',', header=True)
    spark_df = spark_df.withColumn('Date', spark_df['Date'].cast('date'))
    spark_df = spark_df.drop_duplicates()
    spark_df.show()
    print(spark_df.count())

    spark_df.createTempView("stocks")

    query1 = """Select data.date, data.company, data.high_percent, data.low_percent from (Select date, company, 
    ((ABS(Open-High)/Open)*100) as high_percent , ((ABS(Open - Low)/Open)*100) as low_percent, dense_rank() over (
    partition by date order by high_percent desc, low_percent desc) as dense_rank from stocks )data where 
    data.dense_rank = 1 """

    query2 = """Select stock_table.company, stock_table.date, stock_table.volume from (Select date, company, volume, 
    dense_rank() over (partition by date order by volume desc) as dense_rank from stocks) as stock_table where 
    stock_table.dense_rank=1 """

    query4 = """with df1 as (select company, open from (select company, open, dense_rank() over (partition by company order by date) as d_rank1 from stocks)stock_table where stock_table.d_rank1=1) " \
          ", df2 as (select company, close from (select company, close, dense_rank() over (partition by company order by date desc) as d_rank2 from stocks)stock_table2 where stock_table2.d_rank2 = 1) " \
          "select df1.company, df1.open, df2.close, df1.open-df2.close as max_diff from df1 inner join df2 where df1.company = df2.company " \
          "order by max_diff DESC limit 1"""

    query5 = """Select company, stddev_samp(Volume) as std_deviation from stocks group by company"""

    query6 = """select avg(open) as Average_open, , company from stocks group by company"""

    query7 = """select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume 
    desc """

    query8 = """select company, AVG(volume) as Average_Volume from stocks 
    group by company order by Average_Volume desc limit 1"""

    query9 = """select company, Max(High) as Highest_Value, Min(Low) as Lowest_Value from stocks
    group by company"""

    test_query = "Select * from stocks"




    spark.sql(test_query).show()


load_in_spark()
