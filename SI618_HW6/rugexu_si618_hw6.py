import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *

business = sqlContext.read.json("hdfs:///var/umsi618f21/hw6/yelp_academic_dataset_business.json")
review = sqlContext.read.json("hdfs:///var/umsi618f21/hw6/yelp_academic_dataset_review.json")

business.registerTempTable("business")
review.registerTempTable("review")

user_stats = sqlContext.sql("SELECT user_id, AVG(stars) as mean_star, STDDEV(stars) as stdev_star from review GROUP BY user_id")
user_stats.registerTempTable("user_stats")
review_user_stats = sqlContext.sql("SELECT review.*, user_stats.mean_star, user_stats.stdev_star from review INNER JOIN user_stats ON review.user_id=user_stats.user_id")
review_user_stats.registerTempTable("review_user_stats")
normalized_review_user_stats = sqlContext.sql("SELECT *, (CASE stdev_star WHEN 0.0 THEN 0 WHEN 'NaN' THEN 0.0 ELSE (stars-mean_star)/stdev_star END) as normalized_star from review_user_stats")
normalized_review_user_stats.registerTempTable("normalized_review_user_stats")
avg_normalized_business_stats = sqlContext.sql("SELECT business_id, AVG(normalized_star) as avg_normalized_star from normalized_review_user_stats GROUP BY business_id ORDER BY avg_normalized_star DESC")
avg_normalized_business_stats.registerTempTable("avg_normalized_business_stats")

q1 = sqlContext.sql("SELECT * from avg_normalized_business_stats LIMIT 100")
q1.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('rugexu_si618_hw6_output_1')

business_avg_normalized_star = sqlContext.sql("SELECT business.*, avg_normalized_business_stats.avg_normalized_star from business INNER JOIN avg_normalized_business_stats ON business.business_id=avg_normalized_business_stats.business_id")
business_avg_normalized_star.registerTempTable("business_avg_normalized_star")

q2 = sqlContext.sql("SELECT city, AVG(avg_normalized_star) as avg_rating from business_avg_normalized_star GROUP BY city ORDER BY avg_rating DESC")
q2.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('rugexu_si618_hw6_output_2')

useful_avg_normalized_business_stats = sqlContext.sql("SELECT business_id, AVG(normalized_star) as avg_normalized_star from normalized_review_user_stats WHERE useful > 0 GROUP BY business_id ORDER BY avg_normalized_star DESC")
useful_avg_normalized_business_stats.registerTempTable("useful_avg_normalized_business_stats")
business_useful_avg_normalized_star = sqlContext.sql("SELECT business.*, useful_avg_normalized_business_stats.avg_normalized_star from business INNER JOIN useful_avg_normalized_business_stats ON business.business_id=useful_avg_normalized_business_stats.business_id")
business_useful_avg_normalized_star.registerTempTable("business_useful_avg_normalized_star")

q3 = sqlContext.sql("SELECT city, AVG(avg_normalized_star) as avg_rating from business_useful_avg_normalized_star GROUP BY city ORDER BY avg_rating DESC")
q3.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('rugexu_si618_hw6_output_3')
