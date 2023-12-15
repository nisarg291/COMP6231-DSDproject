from django.shortcuts import render
from pyspark.sql import SparkSession
from django.http import HttpResponse


def top_10_restaurants(request):
    # Initialize SparkSession
    spark = SparkSession.builder.appName('comp6231DSD').getOrCreate()

    # Data Loading
    yelp_reviews = spark.read.json("data/yelp_academic_dataset_review.json")
    yelp_business = spark.read.json("data/yelp_academic_dataset_business.json")

    # Data Cleaning
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
