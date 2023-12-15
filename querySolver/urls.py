from django.urls import path
from .views import get_top_10_restaurants, get_top_5_users,get_top_10_useful

urlpatterns = [
    path('get-top-10/', get_top_10_restaurants, name='get-top-10'),
    path('get-top-5/', get_top_5_users, name='get-top-5'),
    path('get-10-useful/', get_top_10_useful, name='get-10-useful'),

]
