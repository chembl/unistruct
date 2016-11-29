__author__ = 'mnowotka'

from django.db import models
from chembl_core_model.models import *
from chembl_core_db.db.customFields import BlobField, ChemblCharField
from chembl_core_db.db.customManagers import CompoundMolsManager
from chembl_core_db.db.models.abstractModel import ChemblAbstractModel
from chembl_core_db.db.models.abstractModel import ChemblModelMetaClass
from django.utils import six

# ----------------------------------------------------------------------------------------------------------------------


class UcCtab(six.with_metaclass(ChemblModelMetaClass, ChemblAbstractModel)):

    uci = ChemblPositiveIntegerField(primary_key=True, length=20, help_text=u'UniChem Identifier')
    molfile = ChemblTextField(blank=True, null=True, help_text=u'MDL Connection table representation of compound')

    class Meta:
        app_label = 'unistruct'
        managed = True

# ----------------------------------------------------------------------------------------------------------------------


class UcMols(six.with_metaclass(ChemblModelMetaClass, ChemblAbstractModel)):

    objects = CompoundMolsManager()

    uci = models.OneToOneField(UcCtab, primary_key=True, db_column='molregno')
    ctab = BlobField(blank=True, null=True)

    class Meta:
        app_label = 'unistruct'
        managed = True

# ----------------------------------------------------------------------------------------------------------------------
