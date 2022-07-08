from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Stock Project').getOrCreate()
spark_df = spark.read.csv('stock_data/*.csv', sep=',', header=True)
spark_df = spark_df.withColumn('Date', spark_df['Date'].cast('date'))
spark_df.createTempView('stocks')


def max_difference_stock_daily():
    try:
        query1 = """Select stock_table.company, stock_table.date, stock_table.max_diff_stock_percent from 
                 (Select date,company,((high-open)/open)*100 as max_diff_stock_percent, dense_rank()
                 OVER ( partition by date order by ( high-open)/open desc ) as dense_rank FROM stocks)stock_table 
                 where stock_table.dense_rank=1"""
        data = spark.sql(query1).collect()
        results = {}
        for row in data:
            results[row['date'].strftime('%Y-%m-%d')] = {'company': row['company'],
                                                         'max_diff_stock_percent': row['max_diff_stock_percent']}
        return results
    except Exception as e:
        print(e)


def most_traded_stock_daily():
    try:
        query = """Select stock_table.company, stock_table.date, stock_table.volume from (Select date, company, volume,
                  dense_rank() over (partition by date order by int(volume) desc) as dense_rank from stocks)stock_table
                  where stock_table.dense_rank=1
                """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results[row['date'].strftime("%Y-%m-%d")] = {'company': row['company'], 'date': row['date'],
                                                         'volume': row['volume']}
        return results
    except Exception as e:
        print(e)


def maximum_up_and_down_movement():
    try:
        query = """
                Select stocks_table.company,abs(stocks_table.previous_close-stocks_table.open) as max_gap from 
                (Select company, open, date, close, lag(close,1,35.724998) over(partition by company order by date) as
                previous_close from stocks asc)stocks_table order by max_gap desc limit 1
            """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results['company'] = row['company']
            results['max_gap'] = row['max_gap']
        return results
    except Exception as e:
        print(e)


def maximum_movement_stock():
    try:
        query = """Select stocks_table.company, stocks_table.open, stocks_table.high, (stocks_table.high - stocks_table.open) 
              as max_diff from (Select company, (Select open from stocks limit 1) as open, max(high) as high 
              from stocks group by company)stocks_table order by max_diff desc limit 1
        """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results['company'] = row['company']
            results['open'] = row['open']
            results['high'] = row['high']
            results['max_diff'] = row['max_diff']
        return results
    except Exception as e:
        print(e)


def standard_deviation_for_stocks():
    try:
        query = """
            select Company, stddev_samp(Volume) as Standard_Deviation from stocks group by Company
        """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Standard_Deviation': val})
        return results
    except Exception as e:
        print(e)


def mean_and_median_prices_for_stocks():
    try:
        query = """
                Select company, avg(Close) as mean, percentile_approx(Close,0.5) as median from stocks group by company
            """
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append({'company': row['company'], 'mean': row['mean'], 'median': row['median']})
        return results
    except Exception as e:
        print(e)


def average_volume_for_stocks():
    try:
        query = """
            select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume desc
        """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Average Volume': val})
        return results
    except Exception as e:
        print(e)


def highest_stock_average_volume():
    try:
        query = """select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume 
        desc limit 1 """
        spark.sql(query).show()
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Average Volume': val})
        return results
    except Exception as e:
        print(e)


def highest_and_lowest_stock_prices():
    try:
        query = """
            select Company, MAX(high) as Highest_Price, MIN(low) as Lowest_Price from stocks group by Company
        """
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append(
                {'company': row['Company'], 'highest_price': row['Highest_Price'], 'lowest_price': row['Lowest_Price']})
        return results
    except Exception as e:
        print(e)
