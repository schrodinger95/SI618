# Calculate the total stars for each business category
# Original by Dr. Yuhang Wang and Josh Gardner
# Updated by Danaja Maldeniya
'''
To run on Cavium cluster:
spark-submit --master yarn --queue umsi618f21 --num-executors 16 --executor-memory 1g --executor-cores 2 spark_avg_stars_per_category.py

To get results:
hadoop fs -getmerge total_reviews_per_category_output total_reviews_per_category_output.txt
'''

import json
from pyspark import SparkContext
sc = SparkContext(appName="PySparksi618f19_total_reviews_per_category")

input_file = sc.textFile("/var/umsi618f21/hw5/yelp_academic_dataset_business.json")

def cat_reviews(data):
    cat_review_list = []
    reviews = data.get('review_count', None)
    stars = data.get('stars', None)
    attributes = data.get('attributes', None)
    categories_raw = data.get('categories', None)
    city = data.get('city', None)

    wheelchair = 0
    parking = 0
    if attributes:
        WheelchairAccessible = attributes.get('WheelchairAccessible', None)
        if WheelchairAccessible == "True":
            wheelchair = 1

        BusinessParking = attributes.get('BusinessParking', None)
        if BusinessParking:
            BusinessParking = eval(BusinessParking)
            if BusinessParking:
                garage = BusinessParking.get('garage', None)
                street = BusinessParking.get('street', None)
                lot = BusinessParking.get('lot', None)
                if garage or street or lot:
                    parking = 1
        
    if categories_raw:
        categories = categories_raw.split(', ')
        for c in categories:
            if reviews != None:
                cat_review_list.append(((city, c), (1, stars, wheelchair, parking)))
    elif reviews != None:
        cat_review_list.append(((city, 'Unknown'), (1, stars, wheelchair, parking)))

    return cat_review_list


cat_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_reviews) \
                      .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3])) \
                      .map(lambda x: (x[0], (x[1][0], x[1][1]/x[1][0], x[1][2], x[1][3]))) \
                      .sortBy(lambda x: x[0][1], ascending = True) \
                      .sortBy(lambda x: x[1][0], ascending = False) \
                      .sortBy(lambda x: x[0][0], ascending = True) \
                      .map(lambda x: x[0][0] + '\t' + x[0][1] + '\t' + str(x[1][0]) + '\t' + str(x[1][1]) + '\t' + str(x[1][2]) + '\t' + str(x[1][3]))

cat_stars.collect()
cat_stars.saveAsTextFile("total_reviews_per_category_output")
