from django.urls import path
from .views import top_10_restaurants

urlpatterns = [
    path('get-top-10/', top_10_restaurants, name='get-top-10'),
    # Add other URLs as needed
]
