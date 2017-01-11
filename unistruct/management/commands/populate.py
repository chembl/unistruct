from __future__ import print_function

__author__ = 'mnowotka'

import os
import sys
from django.core.management.base import BaseCommand, OutputWrapper
from optparse import make_option
from blessings import Terminal
from progressbar import ProgressBar, RotatingMarker, Bar, Percentage, ETA, Counter
from django.conf import settings


from rdkit import Chem
rdkit_inchi = None

if hasattr(Chem, 'InchiToMol'):
    rdkit_inchi = Chem.InchiToMol

elif hasattr(Chem, 'MolFromInchi'):
    rdkit_inchi = Chem.MolFromInchi

import indigo
import indigo_inchi

M = 10**6
k = 10**3
DEFAULT_LIMIT_SIZE = 20*M
DEFAULT_CHUNK_SIZE = 10*k

ESSENTIAL_LAYERS = 3
INCHI_LAYER_SEPARATOR = '/'

# ----------------------------------------------------------------------------------------------------------------------


def inchi2mol_rdkit(inchi, **_):
    mol = rdkit_inchi(str(inchi))
    if not mol:
        return
    return Chem.MolToMolBlock(mol)

# ----------------------------------------------------------------------------------------------------------------------


def inchi2mol_indigo(inchi, **kwargs):
    mol = kwargs['inchiObj'].loadMolecule(str(inchi))
    if not mol:
        return
    return mol.molfile()

# ----------------------------------------------------------------------------------------------------------------------

inchi_converters = {
    'rdkit': inchi2mol_rdkit,
    'indigo': inchi2mol_indigo,
}

# ----------------------------------------------------------------------------------------------------------------------


class Writer(object):
    """Create an object with a write method that writes to a
    specific place on the screen, defined at instantiation.
    This is the glue between blessings and progressbar.
    """
    def __init__(self, term, location):
        """
        Input: location - tuple of ints (x, y), the position
        of the bar in the terminal
        """
        self.term = term
        self.location = location

    def write(self, string):
        print(self.term.move(*self.location) + string)

# ----------------------------------------------------------------------------------------------------------------------


class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('--chunkSize', default=DEFAULT_CHUNK_SIZE, dest='chunkSize',
                    help='How many rows are migrated in single transaction.'),
        make_option('--targetDatabase', dest='targetDatabase',
                    default=None, help='Target database'),
        make_option('--sourceDatabase', dest='sourceDatabase',
                    default='UNICHEM_READER', help='Source database'),
        make_option('--sourceModelName', dest='sourceModelName',
                    default='UcStructure', help='name of the model to read data from'),
        make_option('--targetModelName', dest='targetModelName',
                    default='UcCtab', help='name of the model to load data to'),
        make_option('--offset', dest='offset',
                    default=0, help='index of the first element to populate'),
        make_option('--limit', dest='limit',
                    default=DEFAULT_LIMIT_SIZE, help='number of elements to populate'),
        make_option('--inchiConversion', dest='inchiConversion',
                    default='rdkit', help='name of the method used for inchi conversion'),
        make_option('--displayIndex', dest='displayIndex',
                    default=0, help='position where to display progress bar'),
        make_option('--displayOffset', dest='displayOffset',
                    default=0, help='position from top where the first progress bar will be displayed'),
        make_option('--ignoreFile', dest='ignoreFile',
                    default='ignore.txt', help='File with IDs of the objects to ignore during migation'),
        )

# ----------------------------------------------------------------------------------------------------------------------

    def __init__(self):
        super(Command, self).__init__()
        self.total_success = 0
        self.total_failure = 0
        self.total_empty = 0
        self.total_ignored = 0
        self.app_name = 'unistruct'
        self.verbosity = 0
        self.records_to_be_populated = 0
        self.display_index = 0
        self.display_offset = 0
        self.term = None
        self.ignores = None

# ----------------------------------------------------------------------------------------------------------------------

    def handle(self, *args, **options):
        from django.db.models import get_model
        from django.db import reset_queries
        from django.db import connections

        reset_queries()

        self.term = Terminal(stream=self.stdout)
        self.verbosity = int(options.get('verbosity'))
        self.display_index = int(options.get('displayIndex'))
        self.display_offset = int(options.get('displayOffset'))

        if settings.DEBUG:
            self.stderr.write("Django is in debug mode, which causes memory leak. "
                              "Set settings.DEBUG to False and run again.")
            return

        ignore_file = options.get('ignoreFile')
        ignore_file = os.path.abspath(os.path.expanduser(ignore_file))
        if self.verbosity >= 2:
            self.stdout.write("The ignore file is {0}".format(str(ignore_file)))

        if not (os.path.isfile(ignore_file) and os.access(ignore_file, os.R_OK)):
            if self.verbosity >= 1:
                self.stderr.write("File {0} is missing or not readable".format(ignore_file))
            return

        with open(ignore_file) as f:
            try:
                self.ignores = set([int(x.strip('\n')) for x in f.readlines()])
            except Exception as e:
                if self.verbosity >= 1:
                    self.stderr.write("There was a problem with parsing file {0}, error message: {1}"
                                      .format(ignore_file, e.message))
                return
            if self.verbosity >= 2:
                self.stdout.write("Following object IDs will be ignored: {0}".format(str(self.ignores)))


        target_database = options.get('targetDatabase')
        if not target_database:
            if self.verbosity >= 1:
                self.stderr.write("No target database given")
            return

        source_database = options.get('sourceDatabase')
        if not source_database:
            if self.verbosity >= 1:
                self.stderr.write("No source database given")
            return

        source_model_name = options.get('sourceModelName')
        if not source_model_name:
            if self.verbosity >= 1:
                self.stderr.write("No source model name given")
            return

        source_model = get_model(self.app_name, source_model_name)
        if not source_model:
            if self.verbosity >= 1:
                self.stderr.write("No model named {0} defined for {1} application".format(source_model_name,
                                                                                          self.app_name))
            return

        target_model_name = options.get('targetModelName')
        if not target_model_name:
            if self.verbosity >= 1:
                self.stderr.write("No target model name given")
            return

        target_model = get_model(self.app_name, target_model_name)
        if not target_model:
            if self.verbosity >= 1:
                self.stderr.write("No model named {0} defined for {1} application".
                                  format(target_model_name, self.app_name))
            return

        if self.verbosity >= 2:
            self.stdout.write("Populating model {0} from database {1} with data taken from model {2} from database {3}"
                              .format(target_model_name, target_database, source_model_name, source_database))

        size = int(options.get('chunkSize'))
        limit = int(options.get('limit'))
        offset = int(options.get('offset'))

        inchi_conversion = options.get('inchiConversion')
        if not inchi_conversion:
            if self.verbosity >= 1:
                self.stderr.write("No inchi conversion method given")
            return

        inchi_converter = inchi_converters.get(inchi_conversion)
        if not inchi_converter:
            if self.verbosity >= 1:
                self.stderr.write("No conversion method named {0} found.".format(inchi_conversion))
            return

        if inchi_converter == 'rdkit' and hasattr(Chem, 'INCHI_AVAILABLE'):
            if not Chem.INCHI_AVAILABLE:
                if self.verbosity >= 1:
                    self.stderr.write("RDkit was compiled without inchi support and can't be used as inchi to "
                                      "ctab converter.")
                return
            if not rdkit_inchi:
                if self.verbosity >= 1:
                    self.stderr.write("No RDkit inchi to ctab conversion method found.")
                return

        self.dispatch(target_model_name, target_database, source_model_name, source_database,
                      size, limit, offset, inchi_conversion)

        if self.verbosity >= 2:
            self.stdout.write("Finished populating model {0}".format(target_model_name))
            self.stdout.write("{0} records out of {1} has been successfully populated"
                              .format(self.total_success, self.records_to_be_populated))
            self.stdout.write("{0} records out of {1} has failed to populate"
                              .format(self.total_failure, self.records_to_be_populated))
            self.stdout.write("{0} records out of {1} were empty (no inchi key)"
                              .format(self.total_empty, self.records_to_be_populated))
            self.stdout.write("{0} records out of {1} were ignored (blacklisted)"
                              .format(self.total_ignored, self.records_to_be_populated))

        return self.total_success, self.total_failure, self.total_empty, self.total_ignored, \
               self.records_to_be_populated

# ----------------------------------------------------------------------------------------------------------------------

    def dispatch(self, target_model_name, target_database, source_model_name, source_database, size, limit, offset,
                 inchi_conversion):
        from django.db.models import get_model

        source_model = get_model(self.app_name, source_model_name)
        target_model = get_model(self.app_name, target_model_name)

        if self.verbosity >= 2:
            self.stdout.write("Trying to populate {0} records, starting from record {1}".format(limit, offset))

        source_count = source_model.objects.using(source_database).count()
        target_count = target_model.objects.using(target_database).count()

        if offset > source_count:
            if self.verbosity >= 1:
                self.stderr.write("Offset {0} is bigger than the total number of records in source model's table "
                                  "({1}). There is nothing to populate".format(offset, source_count))
            return

        if source_count == 0:
            if self.verbosity >= 1:
                self.stderr.write("Source model's table is empty. There is nothing to populate")
            return

        if source_count - offset < limit and self.verbosity >= 1:
            self.stderr.write("The actual number of records in the source model's table ({0}) is lower that the given "
                              "limit ({1}). Only {0} records will be populated".format(source_count, limit))

        self.records_to_be_populated = min(source_count - offset, limit)
        self.total_success = 0
        self.total_failure = 0
        self.total_empty = 0
        self.total_ignored = 0

        if self.verbosity >= 2:
            self.stdout.write("source count is {0}, target count is {1} limit is {2}, offset is {3}, total number of "
                              "records to populate is {4}".format(source_count, target_count, limit, offset,
                                                                  self.records_to_be_populated))

        return self.populate_model(target_model_name, target_database, source_model_name, source_database, size,
                                   self.records_to_be_populated, offset, inchi_conversion, self.display_index,
                                   self.display_offset)

# ----------------------------------------------------------------------------------------------------------------------

    def populate_model(self, target_model_name, target_database, source_model_name, source_database,
                       size, limit, offset, inchi_conversion, idx, display_offset):
        from django.db import connections
        from django.db.utils import DatabaseError

        source_connection = connections[source_database]
        if not source_connection:
            if self.verbosity >= 1:
                self.stderr.write("Can't find connection named {0} in settings".format(source_database))
            return

        target_connection = connections[target_database]
        if not target_connection:
            if self.verbosity >= 1:
                self.stderr.write("Can't find connection named {0} in settings".format(target_database))
            return

        populated = False

        while not populated:
            if size < 1:
                if self.verbosity >= 1:
                    self.stderr.write("Couldn't populate {0} using all sensible values of chunk size. Leaving model "
                                      "{0} not migrated".format(target_model_name))
                break
            try:
                self.populate(target_model_name, target_database, source_model_name, source_database,
                              size, limit, offset, inchi_conversion, idx, display_offset)
                populated = True
            except DatabaseError as e:
                if target_connection.vendor == 'mysql' and 'MySQL server has gone away' in str(e):
                    size /= 2
                    if self.verbosity >= 1:
                        self.stderr.write("Populating model {0} failed. Retrying with reduced chunk size = {1}"
                                          .format(target_model_name, size))
                else:
                    if self.verbosity >= 1:
                        self.stderr.write("Populating model {0} failed due to database error: {1}"
                                          .format(target_model_name, str(e)))
                    break
            except Exception as e:
                self.stdout.write("Populating model {0} failed with unexpected error {1}, {2}"
                                  .format(target_model_name, str(e), e.message))
                break


# ----------------------------------------------------------------------------------------------------------------------

    def convert_inchi(self, inchi_converter, pk, inchi, inchi_kwargs):
        if not inchi:
            return
        mol = None
        while True:
            try:
                mol = inchi_converter(inchi, **inchi_kwargs)
            except Exception as e:
                if self.verbosity >= 1:
                    self.stderr.write("ERROR: error ({0}) while calculating molfile for record ({1}, {2})"
                                      .format(e.message or str(e), pk, inchi))
            if mol:
                return mol
            layers = inchi.split(INCHI_LAYER_SEPARATOR)
            if len(layers) <= ESSENTIAL_LAYERS:
                return
            inchi = INCHI_LAYER_SEPARATOR.join(layers[:-1])
            if self.verbosity >= 3:
                self.stderr.write('Retrying compound {0} with inchi {1}'.format(pk, inchi))

# ----------------------------------------------------------------------------------------------------------------------

    def populate(self, target_model_name, target_database, source_model_name, source_database, size, limit, offset,
                 inchi_conversion, idx, display_offset):

        from django.db import connections
        from django.db import transaction
        from django.db.models import get_model
        from django.db.utils import DatabaseError, IntegrityError

        source_model = get_model(self.app_name, source_model_name)
        target_model = get_model(self.app_name, target_model_name)

        target_conn = connections[target_database]

        source_pk = source_model._meta.pk.name

        writer = Writer(self.term, (idx, 0))
        pbar = ProgressBar(widgets=['{0} ({1}) [{2}-{3}]: '.format(target_model_name, idx - display_offset + 1,
                                                                  offset, offset + limit),
                                    Percentage(), ' (', Counter(), ') ',
                                    Bar(marker=RotatingMarker()), ' ', ETA()],
                           fd=writer, maxval=limit).start()

        inchi_kwargs = {}

        if inchi_conversion == 'indigo':
            indigo_obj = indigo.Indigo()
            indigo_inchi_obj = indigo_inchi.IndigoInchi(indigo_obj)
            inchi_kwargs = {"inchiObj": indigo_inchi_obj}

        elif inchi_conversion == 'rdkit' and self.verbosity < 1:
            from rdkit import rdBase
            rdBase.DisableLog('rdApp.error')
            from rdkit import RDLogger
            lg = RDLogger.logger()
            lg.setLevel(RDLogger.CRITICAL)

        inchi_converter = inchi_converters.get(inchi_conversion)
        last_pk = None

        for i in range(offset, offset + limit, size):
            success = 0
            failure = 0
            empty = 0
            ignored = 0
            transaction.commit_unless_managed(using=target_database)
            transaction.enter_transaction_management(using=target_database)
            transaction.managed(True, using=target_database)
            with target_conn.constraint_checks_disabled():

                try:

                    chunk_size = min(size, limit + offset - i)
                    original_data = None

                    if not last_pk:
                        if i:
                            last_pk = source_model.objects.using(
                                source_database).order_by(source_pk).only(source_pk).values_list(source_pk)[i][0]
                        else:
                            original_data = source_model.objects.using(source_database).order_by(
                                source_pk).values_list('pk', 'standardinchi')[:chunk_size]

                    if not original_data:
                        original_data = source_model.objects.using(source_database).order_by(
                            source_pk).values_list('pk', 'standardinchi').filter(pk__gt=last_pk)[:chunk_size]

                    last_pk = original_data[chunk_size-1][0]

                    target_data = []
                    for pk, inchi in original_data:
                        if not inchi:
                            empty += 1
                            continue

                        if int(pk) in self.ignores:
                            ignored += 1
                            continue

                        ctab = self.convert_inchi(inchi_converter, pk, inchi, inchi_kwargs)
                        if not ctab:
                            failure += 1
                            continue
                        target_data.append(target_model(pk=int(pk), molfile=ctab))
                        success += 1

                    target_model.objects.using(target_database).bulk_create(target_data)

                except IntegrityError as e:
                    if self.verbosity >= 1:
                        self.stderr.write("ERROR: integrity error ({0}) occurred when processing chunk {1}-{2}"
                                          .format(e.message, i, i + size))
                    transaction.rollback(using=target_database)
                    transaction.leave_transaction_management(using=target_database)
                    continue

                except DatabaseError as e:
                    if self.verbosity >= 1:
                        self.stderr.write("ERROR: database error ({0}) occurred when processing chunk {1}-{2}"
                                          .format(e.message, i, i + size))
                    transaction.rollback(using=target_database)
                    transaction.leave_transaction_management(using=target_database)
                    raise e

            pbar.update(i - offset + 1)
            transaction.commit(using=target_database)
            transaction.leave_transaction_management(using=target_database)

            self.total_success += success
            self.total_failure += failure
            self.total_empty += empty
            self.total_ignored += ignored

        pbar.update(limit)
        pbar.finish()

# ----------------------------------------------------------------------------------------------------------------------

    def execute(self, *args, **options):

        saved_lang = None
        self.stdout = OutputWrapper(options.get('stdout', sys.stdout))
        self.stderr = OutputWrapper(options.get('stderr', sys.stderr), self.style.ERROR)

        if self.can_import_settings:
            from django.utils import translation
            saved_lang = translation.get_language()
            translation.activate('en-us')

        try:
            if self.requires_model_validation and not options.get('skip_validation'):
                self.validate()
            output = self.handle(*args, **options)
        finally:
            if saved_lang is not None:
                translation.activate(saved_lang)

        return output

# ----------------------------------------------------------------------------------------------------------------------

