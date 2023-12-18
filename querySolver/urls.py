from django.urls import path
from .views import *

urlpatterns = [
    path('q1-bq/',get_top_10_restaurants_with_query, name="top-10"),
    path('q2-bq/',get_top_5_users_with_query, name="top-5"),
    path('q3-bq/', get_top_10_useful_with_query, name='top-10-useful'),
    path('q4-bq/', get_3_star_reviews_with_query, name='top-3-star-review'),
    path('q1-spark/', get_top_10_restaurants, name='get-top-10'),
    path('q2-spark/', get_top_5_users, name='get-top-5'),
    path('q3-spark/', get_top_10_useful, name='get-10-useful'),
    path('q4-spark/',get_3_star_reviews,name="get_3_star_reviews")

]
