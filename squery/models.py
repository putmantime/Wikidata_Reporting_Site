from __future__ import unicode_literals

from django.db import models


class Microcount(models.Model):
    index = models.BigIntegerField(blank=True, null=True)
    wd_species_x = models.TextField(blank=True, null=True)
    label_x = models.TextField(blank=True, null=True)
    taxid_x = models.TextField(blank=True, primary_key=True)
    gene_count = models.TextField(blank=True, null=True)
    protein_count = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'microcounts'