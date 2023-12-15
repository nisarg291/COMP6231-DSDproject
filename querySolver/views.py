import pyspark
from django.shortcuts import render
from pyspark.sql import SparkSession
from django.http import HttpResponse
from pyspark.sql.functions import sum as _sum



def get_top_10_restaurants(request):
    spark = SparkSession.builder.appName('comp6231DSD').getOrCreate()

    yelp_reviews = spark.read.json("data/yelp_academic_dataset_review.json")
    yelp_business = spark.read.json("data/yelp_academic_dataset_business.json")

    yelp_reviews = yelp_reviews.filter((yelp_reviews.stars.isNotNull()) & (yelp_reviews.business_id.isNotNull()))
    yelp_business = yelp_business.filter((yelp_business.name.isNotNull()) & (yelp_business.business_id.isNotNull()))

    # Question 1 - Which are the top 10 open restaurants that have been rated 5 stars, considering only those reviews which have a useful rating?
    reviews_5stars = yelp_reviews.filter((yelp_reviews.stars == 5) & (yelp_reviews.useful >= 1))
    open_business = yelp_business.filter((yelp_business.is_open == 1))
    reviews_5star_count = reviews_5stars.groupBy(reviews_5stars.business_id).count()
    reviews_ordered = reviews_5star_count.orderBy("count", ascending=False).limit(10)
    reviews_with_name = open_business.join(reviews_ordered, reviews_ordered.business_id == open_business.business_id,
                                           'inner')
    reviews_final = reviews_with_name.select('name', 'count').orderBy("count", ascending=False)

    # Convert DataFrame to Pandas DataFrame for simplicity
    pandas_df = reviews_final.toPandas()

    # Convert Pandas DataFrame to HTML table
    table_html = pandas_df.to_html(index=False)

    return HttpResponse(f"<h1>Top 10 Restaurants</h1>{table_html}")

def get_top_5_users(request):
    spark = SparkSession.builder.appName('comp6231DSD').getOrCreate()

    yelp_user = spark.read.json("data/yelp_academic_dataset_user.json")
    yelp_user = yelp_user.filter((yelp_user.name.isNotNull()) & (yelp_user.user_id.isNotNull()))

    sum_compliments_df = yelp_user.withColumn('total_compliments',
                                              yelp_user.compliment_hot + yelp_user.compliment_more + yelp_user.compliment_cool + yelp_user.compliment_cute + yelp_user.compliment_list + yelp_user.compliment_funny + yelp_user.compliment_note + yelp_user.compliment_photos + yelp_user.compliment_plain + yelp_user.compliment_profile + yelp_user.compliment_writer)

    compliments_ordered = sum_compliments_df.orderBy('total_compliments', ascending=False)
    top_users = compliments_ordered.limit(5)
    top_user_names = top_users.select(top_users.user_id, top_users.name, top_users.total_compliments)

    pandas_df = top_user_names.toPandas()
    table_html = pandas_df.to_html(index=False)

    return HttpResponse(f"<h1>Top 5 Users</h1>{table_html}")


def get_top_10_useful(request):
    spark = SparkSession.builder.appName('comp6231DSD').getOrCreate()

    yelp_user = spark.read.json("data/yelp_academic_dataset_user.json")
    yelp_reviews = spark.read.json("data/yelp_academic_dataset_review.json")

    yelp_reviews = yelp_reviews.filter(
        (yelp_reviews.user_id.isNotNull()) & (~pyspark.sql.functions.isnan(yelp_reviews.useful)))

    yelp_user = yelp_user.filter((yelp_user.user_id.isNotNull()))

    user_usefulness_sum = yelp_reviews.groupBy('user_id').agg(_sum('useful').alias("total_usefulness"))
    useful_user_names = user_usefulness_sum.join(yelp_user, user_usefulness_sum.user_id == yelp_user.user_id, 'inner')
    most_useful_users = useful_user_names.orderBy('total_usefulness', ascending=False)
    top_10_users = most_useful_users.limit(10).select('name', 'total_usefulness')
    pandas_df = top_10_users.toPandas()
    table_html = pandas_df.to_html(index=False)
    return HttpResponse(f"<h1>Top 5 Users</h1>{table_html}")

