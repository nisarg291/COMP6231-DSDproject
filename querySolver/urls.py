from django.urls import path
from .views import *

urlpatterns = [
    path('top-10-restaurents/',get_top_10_restaurants_with_query, name="top-10"),
    path('top-5-users/',get_top_5_users_with_query, name="top-5"),
    path('top-10-useful/', get_top_10_useful_with_query, name='top-10-useful'),
    path('top-3-star-reviews/', get_3_star_reviews_with_query, name='top-3-star-review'),
    path('get-top-10/', get_top_10_restaurants, name='get-top-10'),
    path('get-top-5/', get_top_5_users, name='get-top-5'),
    path('get-10-useful/', get_top_10_useful, name='get-10-useful'),
    path('get_3_star_reviews/',get_3_star_reviews,name="get_3_star_reviews")

]
