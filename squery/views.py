from django.shortcuts import render
from django.http import HttpResponse
from .models import Microcount
from .scripts import organisms


def display_table(request):
    mc = Microcount.objects.all()
    context = {'mc': mc}
    return render(request, 'squery/tables.html', context)


def sparql_table(request):
    smc = organisms.run()
    context = {'smc': smc}
    return render(request, 'squery/sparql_tables.html', context)