from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.display_table, name='display_table'),
    url(r'^sparql_table/$', views.sparql_table, name='sparql_table'),
    ]
