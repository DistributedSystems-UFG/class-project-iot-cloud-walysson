from django.urls import path
from . import views
from django.views.decorators.csrf import csrf_exempt

urlpatterns = [
    path('', views.login_view, name='login'),
    path('home', views.home, name='home'),
    path('temperature', csrf_exempt(views.temperature), name='temperature'),
]
