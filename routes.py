from flask import Flask, jsonify
from pyspark_query import *

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


# Home route
@app.route("/")
def twitter_producer():
    return "This is Spark Assignment"


# Query - 1
@app.route("/query1", methods=["GET"])
def max_difference_stock():
    result = max_difference_stock_daily()
    dict_to_return = {"Max diff stock daily basis": result}
    return jsonify(dict_to_return)
    pass


# Query - 2
@app.route("/query2", methods=["GET"])
def most_traded_stock_daily_api():
    result = most_traded_stock_daily()
    dict_to_return = {"Most traded stock on each day": result}
    return jsonify(dict_to_return)


# Query - 3
@app.route("/query3", methods=["GET"])
def maximum_up_and_down_movement_stock():
    result = maximum_up_and_down_movement()
    dict_to_return = {"Max gap up and gap down": result}
    return jsonify(dict_to_return)
    pass


# Query - 4
@app.route("/query4", methods=["GET"])
def maximum_movement_stock():
    result = maximum_movement_stock()
    dict_to_return = {"Maximum Moved stock": result}
    return jsonify(dict_to_return)


# Query - 5
@app.route("/query5", methods=["GET"])
def standard_deviation():
    result = standard_deviation_for_stocks()
    dict_to_return = {"Standard Deviation": result}
    return jsonify(dict_to_return)


# Query - 6
@app.route("/query6", methods=["GET"])
def mean_and_median_stocks():
    result = mean_and_median_prices_for_stocks()
    dict_to_return = {"Mean and Median prices for stocks": result}
    return jsonify(dict_to_return)
    pass


# Query - 7
@app.route("/query7", methods=["GET"])
def average_volume_stocks():
    result = average_volume_for_stocks()
    dict_to_return = {"Average Volume for stocks": result}
    return jsonify(dict_to_return)


# Query - 8
@app.route("/query8", methods=["GET"])
def highest_stock_average_volume_api():
    result = highest_stock_average_volume()
    dict_to_return = {"Max Avg Volume stock": result}
    return jsonify(dict_to_return)


# Query - 9
@app.route("/query9", methods=["GET"])
def highest_and_lowest_stock_prices_api():
    result = highest_and_lowest_stock_prices()
    dict_to_return = {"Highest And lowest Value of stocks": result}
    return jsonify(dict_to_return)


if __name__ == '__main__':
    app.run(debug=True)
