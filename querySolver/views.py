import pyspark
from django.shortcuts import render
from pyspark.sql import SparkSession
from django.http import HttpResponse
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import count as _count

from google.cloud import bigquery
from django.http import JsonResponse

client = bigquery.Client()

def get_top_10_restaurants_with_query(request):
    # Create a BigQuery client
    

    # Specify your BigQuery dataset and table

    # Build the SQL query
    sql_query = """
    WITH Reviews_5Stars AS (
      SELECT
        business_id,
        COUNT(*) AS count
      FROM
        `dsd-project-practice.demobigquery.review`
      WHERE
        stars = 5
        AND useful >= 1
      GROUP BY
        business_id
    ),

    Top10Restaurants AS (
      SELECT
        b.name,
        r.count
      FROM
        Reviews_5Stars r
      JOIN
        `dsd-project-practice.demobigquery.business` b
      ON
        r.business_id = b.business_id
      WHERE
        b.is_open = 1
      ORDER BY
        r.count DESC
      LIMIT
        10
    )

    SELECT
      *
    FROM
      Top10Restaurants
    """

    # Run the query
    query_job = client.query(sql_query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a list of dictionaries

    rows = [dict(row) for row in results]
    
    str1=""" <table border='1'>
            <thead>
            <tr>
                <th>name</th>
                <th>count</th>
              
            </tr>
        </thead><tbody>"""
    for row in rows:
        str1+='<tr><td>'+row['name']+'</td><td>'+str(row['count'])+'</td></tr>'

    str1+="</tbody></table>"
    # Return the results as JSON
   
    return HttpResponse(f"<h1>Top 10 Restaurants</h1>{str1}")


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

def get_top_5_users_with_query(request):

    sql_query = """ WITH yelp_user AS (
        SELECT
            user_id,
            name,
            IFNULL(compliment_hot, 0) +
            IFNULL(compliment_more, 0) +
            IFNULL(compliment_cool, 0) +
            IFNULL(compliment_cute, 0) +
            IFNULL(compliment_list, 0) +
            IFNULL(compliment_funny, 0) +
            IFNULL(compliment_note, 0) +
            IFNULL(compliment_photos, 0) +
            IFNULL(compliment_plain, 0) +
            IFNULL(compliment_profile, 0) +
            IFNULL(compliment_writer, 0) AS total_compliments
        FROM
            dsd-project-practice.demobigquery.yelp_user
        WHERE
            name IS NOT NULL
            AND user_id IS NOT NULL
        )

        SELECT
        user_id,
        name,
        total_compliments
        FROM
        yelp_user
        ORDER BY
        total_compliments DESC
        LIMIT  5;
    """

    query_job = client.query(sql_query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a list of dictionaries

    rows = [dict(row) for row in results]

    str1=""" <table border='1'>
            <thead>
            <tr>
             <th>user_id</th>
                <th>name</th>
                <th>total_compliments</th>
              
            </tr>
        </thead><tbody>"""
    for row in rows:
        str1+='<tr><td>'+row['user_id']+'</td><td>'+str(row['name'])+'</td><td>'+str(row['total_compliments'])+'</td></tr>'

    str1+="</tbody></table>"
    # Return the results as JSON
   
    return HttpResponse(f"<h1>Top 10 Restaurants</h1>{str1}")



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

def get_top_10_useful_with_query(request):
    
    sql_query = """ WITH UserUsefulness AS (
        SELECT
            r.user_id,
            SUM(r.useful) AS total_usefulness
        FROM
            dsd-project-practice.demobigquery.review r
        WHERE
            r.user_id IS NOT NULL
            AND r.useful IS NOT NULL
        GROUP BY
            r.user_id
        ),

        Top10UsefulUsers AS (
        SELECT
            u.name,
            uu.total_usefulness
        FROM
            UserUsefulness uu
        JOIN
            dsd-project-practice.demobigquery.yelp_user u
        ON
            uu.user_id = u.user_id
        ORDER BY
            uu.total_usefulness DESC
        LIMIT
            10
        )

        SELECT
        *
        FROM
        Top10UsefulUsers;"""
    
    query_job = client.query(sql_query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a list of dictionaries

    rows = [dict(row) for row in results]

    str1=""" <table border='1'>
            <thead>
            <tr>
                <th>name</th>
                <th>total_usefulness</th>
              
            </tr>
        </thead><tbody>"""
    for row in rows:
        str1+='<tr><td>'+str(row['name'])+'</td><td>'+str(row['total_usefulness'])+'</td></tr>'

    str1+="</tbody></table>"
    # Return the results as JSON
   
    return HttpResponse(f"<h1>Top 10 Restaurants</h1>{str1}")

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
    return HttpResponse(f"<h1>Top 10 users with useful reviews</h1>{table_html}")



def get_3_star_reviews_with_query(request):

    sql_query = """WITH Star4Reviews AS (
        SELECT
            r.business_id
        FROM
            dsd-project-practice.demobigquery.review r
        WHERE
            r.stars > 3
            AND EXTRACT(YEAR FROM DATE(r.date)) = 2018
        ),

        Business4Stars AS (
        SELECT
            business_id,
            COUNT(*) AS count
        FROM
            Star4Reviews
        GROUP BY
            business_id
        ),

        Business50 AS (
        SELECT
            b.business_id, b.count
        FROM
            Business4Stars b
        WHERE
            b.count >= 50
        ),

        BusinessPA AS (
        SELECT
            business_id
        FROM
            dsd-project-practice.demobigquery.business
        WHERE
            state = 'PA'
        ),

        JoinedDF AS (
        SELECT
            b50.business_id,
            b50.count
        FROM
            Business50 b50
        JOIN
            BusinessPA bpa
        ON
            b50.business_id = bpa.business_id
        )

        SELECT COUNT(*) FROM JoinedDF as total_count
    """


    query_job = client.query(sql_query)

    # Fetch the results
    results = query_job.result()
    print(type(results))

    # Convert results to a list of dictionaries

    rows = [dict(row) for row in results]

    str1=""" <table border='1'>
            <thead>
            <tr>
                <th>total count</th>
            </tr>
        </thead><tbody>"""
    for row in rows:
      str1+='<tr><td>'+str(row['f0_'])+'</td></tr>'

    str1+="</tbody></table>"
   
   
    return HttpResponse(f"<h1>Top 10 Restaurants</h1>{str1}")

def get_3_star_reviews(request):
    spark = SparkSession.builder.appName('comp6231DSD').getOrCreate()

    yelp_business = spark.read.json("data/yelp_academic_dataset_business.json")
    yelp_reviews = spark.read.json("data/yelp_academic_dataset_review.json")

    star4_reviews = yelp_reviews.filter((yelp_reviews.stars > 3))
    star4_2018_reviews = star4_reviews.filter(star4_reviews.date.startswith('2018'))
    business_4stars = star4_2018_reviews.groupBy('business_id').agg(_count('stars').alias("count"))
    business_50 = business_4stars.filter((business_4stars['count'] >= 50))
    business_PA = yelp_business.filter((yelp_business.state == 'PA'))
    joined_df = business_50.join(business_PA, business_PA.business_id == business_50.business_id, 'inner')

    count = joined_df.count()
    pandas_df = joined_df.toPandas()
    table_html = pandas_df.to_html(index=False)
    return HttpResponse(f"<h1>Top businesses that were given 3 starts in 2018</h1>{table_html}"
                        f"<h2>Total number</h2>{count}")
