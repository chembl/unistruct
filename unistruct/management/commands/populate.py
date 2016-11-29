from __future__ import print_function

__author__ = 'mnowotka'

from django.core.management.base import BaseCommand
from optparse import make_option
from django.db.utils import DatabaseError, IntegrityError
from django.core.management.color import no_style
from django.core.management.sql import custom_sql_for_model
from blessings import Terminal
from progressbar import ProgressBar, RotatingMarker, Bar, Percentage, ETA
from django.conf import settings
import django.db.transaction
import django.db.utils
import traceback

import gevent
# import gevent.monkey
# gevent.monkey.patch_all()

import multiprocessing as mp

from rdkit import Chem
rdkit_inchi = None

if hasattr(Chem, 'InchiToMol'):
    rdkit_inchi = Chem.InchiToMol

elif hasattr(Chem, 'MolFromInchi'):
    rdkit_inchi = Chem.MolFromInchi

import indigo
import indigo_inchi

indigoObj = None
indigo_inchiObj = None

term = Terminal()

M = 10**6
k = 10**3
DEFAULT_LIMIT_SIZE = 20*M
DEFAULT_CHUNK_SIZE = 10*k

# ----------------------------------------------------------------------------------------------------------------------


def inchi2mol_rdkit(inchi):
    mol = rdkit_inchi(str(inchi))
    if not mol:
        return None
    return Chem.MolToMolBlock(mol)

# ----------------------------------------------------------------------------------------------------------------------


def inchi2mol_indigo(inchi):
    print(str(inchi))
    mol = indigo_inchiObj.loadMolecule(str(inchi))
    print('done')
    if not mol:
        return None
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
    def __init__(self, location):
        """
        Input: location - tuple of ints (x, y), the position
        of the bar in the terminal
        """
        self.location = location

    def write(self, string):
        with term.location(*self.location):
            print(string)

# ----------------------------------------------------------------------------------------------------------------------


class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('--chunkSize', default=DEFAULT_CHUNK_SIZE, dest='chunk_size',
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
        make_option('--concurrencyModel', dest='concurrencyModel',
                    default=None, help='what kind of concurrency model should be used '
                                       '(can be none, process or greenlet)'),
        make_option('--numberOfJobs', dest='numberOfJobs',
                    default=0, help='when concurrency model is not null you can specify the number of concurrent '
                                    'jobs you would like to spawn. Otherwise a sensible default will be used'),
        )

# ----------------------------------------------------------------------------------------------------------------------

    def __init__(self):
        super(Command, self).__init__()
        self.total_success = 0
        self.total_failure = 0
        self.app_name = 'unistruct'
        self.verbosity = 0
        self.concurrency_model = None
        self.n_jobs = 0
        self.records_to_be_populated = 0

# ----------------------------------------------------------------------------------------------------------------------

    def handle(self, *args, **options):
        import django.db.models
        import django.db
        django.db.reset_queries()
        print(term.clear())

        if settings.DEBUG:
            self.stdout.write("Django is in debug mode, which causes memory leak. "
                              "Set settings.DEBUG to False and run again.")
            return

        target_database = options.get('targetDatabase')
        if not target_database:
            self.stdout.write("No target database given")
            return

        source_database = options.get('sourceDatabase')
        if not source_database:
            self.stdout.write("No source database given")
            return

        self.verbosity = int(options.get('verbosity'))
        self.concurrency_model = options.get('concurrencyModel')
        self.n_jobs = int(options.get('numberOfJobs', 0))

        source_model_name = options.get('sourceModelName')
        if not source_model_name:
            self.stdout.write("No source model name given")
            return

        source_model = django.db.models.get_model(self.app_name, source_model_name)
        if not source_model:
            self.stdout.write("No model named {0} defined for {1} application".format(source_model_name, self.app_name))
            return

        target_model_name = options.get('targetModelName')
        if not target_model_name:
            self.stdout.write("No target model name given")
            return

        target_model = django.db.models.get_model(self.app_name, target_model_name)
        if not target_model:
            self.stdout.write("No model named {0} defined for {1} application".format(target_model_name, self.app_name))
            return

        self.stdout.write("Populating model {0} from database {1} with data taken from model {2} from database {3}"
                              .format(target_model_name, target_database, source_model_name, source_database))

        size = options.get('chunk_size')
        limit = options.get('limit')
        offset = options.get('offset')

        inchi_conversion = options.get('inchiConversion')
        if not inchi_conversion:
            self.stdout.write("No inchi conversion method given")
            return

        inchi_converter = inchi_converters.get(inchi_conversion)
        if not inchi_converter:
            self.stdout.write("No conversion method named {0} found.".format(inchi_conversion))
            return

        if inchi_converter == 'rdkit' and hasattr(Chem, 'INCHI_AVAILABLE'):
            if not Chem.INCHI_AVAILABLE:
                self.stdout.write("RDkit was compiled without inchi support and can't be used as inchi to "
                                  "ctab converter.")
                return
            if not rdkit_inchi:
                self.stdout.write("No RDkit inchi to ctab conversion method found.")
                return

        self.dispatch(target_model, target_database, source_model, source_database,
                      size, limit, offset, inchi_converter)

        self.stdout.write("Finished populating model {0}".format(target_model_name))
        self.stdout.write("{0} records out of {1} has been successfully populated"
                          .format(self.total_success, self.records_to_be_populated))
        self.stdout.write("{0} records out of {1} has failed to populate"
                          .format(self.total_failure, self.records_to_be_populated))

# ----------------------------------------------------------------------------------------------------------------------

    def check_table(self, target_model, target_database):

        target_conn = django.db.connections[target_database]
        target_cursor = target_conn.cursor()
        target_model_name = target_model._meta.object_name
        style = no_style()
        tables = target_conn.introspection.table_names()
        seen_models = target_conn.introspection.installed_models(tables)
        pending_references = {}
        show_traceback = self.verbosity > 1

        if target_model not in seen_models:

            self.stdout.write("The target model {0} is not installed.".format(target_model_name))
            self.stdout.write("Installed objects are: {0}."
                              .format(', '.join(model._meta.object_name for model in seen_models)))
            sql, references = target_conn.creation.sql_create_model(target_model, style, seen_models)
            seen_models.add(target_model)
            for refto, refs in references.items():
                pending_references.setdefault(refto, []).extend(refs)
                if refto in seen_models:
                    sql.extend(target_conn.creation.sql_for_pending_references(refto, style, pending_references))
            sql.extend(target_conn.creation.sql_for_pending_references(target_model, style, pending_references))
            if self.verbosity >= 1 and sql:
                self.stdout.write("Creating table %s\n" % target_model._meta.db_table)
            for statement in sql:
                target_cursor.execute(statement)

            django.db.transaction.commit_unless_managed(using=target_database)

            custom_sql = custom_sql_for_model(target_model, style, target_conn)
            if custom_sql:
                if self.verbosity >= 2:
                    self.stdout.write(
                        "Installing custom SQL for %s.%s model\n" % (self.app_name, target_model_name))
                try:
                    for sql in custom_sql:
                        target_cursor.execute(sql)
                except Exception as e:
                    self.stderr.write("Failed to install custom SQL for %s.%s model: %s\n" %
                                      (self.app_name, target_model_name, e))
                    if show_traceback:
                        traceback.print_exc()
                    django.db.transaction.rollback_unless_managed(using=target_database)
                else:
                    django.db.transaction.commit_unless_managed(using=target_database)
            else:
                if self.verbosity >= 3:
                    self.stdout.write("No custom SQL for %s.%s model\n" %
                                      (self.app_name, target_model_name))

            index_sql = target_conn.creation.sql_indexes_for_model(target_model, style)
            if index_sql:
                if self.verbosity >= 2:
                    self.stdout.write("Installing index for %s.%s model\n" %
                                      (self.app_name, target_model_name))
                try:
                    for sql in index_sql:
                        target_cursor.execute(sql)
                except Exception as e:
                    self.stderr.write("Failed to install index for %s.%s model: %s\n" %
                                      (self.app_name, target_model_name, e))
                    django.db.transaction.rollback_unless_managed(using=target_database)
                else:
                    django.db.transaction.commit_unless_managed(using=target_database)

# ----------------------------------------------------------------------------------------------------------------------

    def dispatch(self, target_model, target_database, source_model, source_database, size, limit, offset,
                 inchi_converter):

        self.check_table(target_model, target_database)

        self.stdout.write("Trying to populate {0} records, starting from record {1}".format(limit, offset))

        source_count = source_model.objects.using(source_database).count()
        target_count = target_model.objects.using(target_database).count()

        if offset > source_count:
            self.stdout.write("Offset {0} is bigger than the total number of records in source model's table ({1}). "
                              "There is nothing to populate".format(offset, source_count))
            return

        if source_count == 0:
            self.stdout.write("Source model's table is empty. There is nothing to populate")
            return

        if source_count - offset < limit:
            self.stdout.write("The actual number of records in the source model's table ({0}) is lower that the given "
                              "limit ({1}). Only {0} records will be populated".format(source_count, limit))

        self.records_to_be_populated = min(source_count - offset, limit)
        self.total_success = 0
        self.total_failure = 0

        if self.verbosity > 1:
            self.stdout.write("source count is {0}, target count is {1} limit is {2}, offset is {3}, total number of "
                              "records to populate is {4}".format(source_count, target_count, limit, offset,
                                                                  self.records_to_be_populated))

        if not self.concurrency_model:
            return self.populate_model(target_model, target_database, source_model, source_database, size,
                                       self.records_to_be_populated, offset, inchi_converter, 0)

        if not self.n_jobs:
            self.n_jobs = mp.cpu_count() * 2
        self.stdout.write("Splitting task into {0} jobs".format(self.n_jobs))

        ranges = range(offset, self.records_to_be_populated + offset,
                       (self.records_to_be_populated / self.n_jobs) or 1) + [self.records_to_be_populated + offset]
        ranges = [(ranges[i],ranges[i+1]) for i in range(0,len(ranges)-2)]

        self.stdout.write("ranges: {0}".format(ranges))

        if self.concurrency_model == 'greenlet':
            self.stdout.write("Using greenlets for concurrency")
            jobs = [gevent.spawn(self.populate_model, target_model, target_database, source_model, source_database,
                                 size, stop-start, start, inchi_converter, idx) for idx, (start, stop)
                    in enumerate(ranges)]
            gevent.joinall(jobs)

        elif self.concurrency_model == 'process':
            self.stdout.write("Using multiprocessing for concurrency")
            pool = mp.Pool(self.n_jobs)
            for idx, (start, stop) in enumerate(ranges):
                pool.apply_async(self.populate_model,
                                 args=(target_model, target_database, source_model, source_database,
                                       size, stop-start, start, inchi_converter, idx),
                                 callback=self.callback)
            pool.close()
            pool.join()

        else:
            self.stderr.write("Unrecognised concurrency model {0}".format(self.concurrency_model))


# ----------------------------------------------------------------------------------------------------------------------

    def callback(self, success, failure):
        self.total_success += success
        self.total_failure += failure

# ----------------------------------------------------------------------------------------------------------------------

    def populate_model(self, target_model, target_database, source_model, source_database, size, limit, offset,
                       inchi_converter, idx):

        target_model_name = target_model._meta.object_name
        connections = django.db.connections

        source_connection = connections[source_database]
        if not source_connection:
            self.stdout.write("Can't find connection named {0} in settings".format(source_database))
            return

        target_connection = connections[target_database]
        if not target_connection:
            self.stdout.write("Can't find connection named {0} in settings".format(target_database))
            return

        source_connection.close()
        target_connection.close()

        ret = None
        populated = False

        while not populated:
            if size < 1:
                self.stdout.write("Couldn't populate {0} using all sensible values of chunk size. Leaving model {0} "
                                  "not migrated".format(target_model_name))
                break
            try:
                ret = self.populate(target_model, target_database, source_model, source_database,
                                    size, limit, offset, inchi_converter, idx)
                populated = True
            except DatabaseError as e:
                if target_connection.vendor == 'mysql' and 'MySQL server has gone away' in str(e):
                    size /= 2
                    self.stdout.write("Populating model {0} failed. Retrying with reduced chunk size = {1}"
                                      .format(target_model_name, size))
                else:
                    self.stdout.write("Populating model {0} failed due to database error: {1}"
                                      .format(target_model_name, str(e)))
                    break
            # except Exception as e:
            #     self.stdout.write("Populating model {0} failed with unexpected error {1}, {2}"
            #                       .format(target_model_name, str(e), e.message))
            #     break

        return ret

# ----------------------------------------------------------------------------------------------------------------------

    def populate(self, target_model, target_database, source_model, source_database, size, limit, offset,
                 inchi_converter, idx):

        target_model_name = target_model._meta.object_name
        target_conn = django.db.connections[target_database]

        source_pk = source_model._meta.pk.name

        writer = Writer((0, idx))
        pbar = ProgressBar(widgets=[target_model_name + ': ', Percentage(), ' ',
                                    Bar(marker=RotatingMarker()), ' ', ETA()],
                           fd=writer, maxval=limit).start()

        total_success = 0
        total_failure = 0

        for i in range(offset, offset + limit, size):
            global indigoObj
            global indigo_inchiObj
            del indigoObj
            del indigo_inchiObj
            indigoObj = indigo.Indigo()
            indigo_inchiObj = indigo_inchi.IndigoInchi(indigoObj)
            success = 0
            failure = 0
            django.db.transaction.commit_unless_managed(using=target_database)
            django.db.transaction.enter_transaction_management(using=target_database)
            django.db.transaction.managed(True, using=target_database)
            with target_conn.constraint_checks_disabled():

                try:

                    if i < 0:
                        original_data = source_model.objects.using(source_database).order_by(source_pk)[:size]
                    else:
                        last_pk = source_model.objects.using(
                            source_database).order_by(source_pk).only(source_pk).values_list(source_pk)[i][0]

                        original_data = source_model.objects.using(source_database).order_by(
                            source_pk).values_list('pk', 'standardinchi').filter(pk__gt=last_pk)[:size]

                    target_data = []
                    for pk, inchi in original_data:
                        ctab = None
                        try:
                            ctab = inchi_converter(inchi)
                        except Exception as e:
                            self.stderr.write("ERROR: error ({0}) while calculating inchi for record ({1}, {2})"
                                              .format(e.message, pk, inchi))
                            failure += 1
                        target_data.append(target_model(pk=int(pk), molfile=ctab))
                        success += 1

                    target_model.objects.using(target_database).bulk_create(target_data)

                except IntegrityError as e:
                    self.stderr.write("ERROR: integrity error ({0}) occurred when processing chunk {1}-{2}".format(
                        e.message, i, i + size))
                    django.db.transaction.rollback(using=target_database)
                    django.db.transaction.leave_transaction_management(using=target_database)
                    continue

                except DatabaseError as e:
                    self.stderr.write("ERROR: database error ({0}) occurred when processing chunk {1}-{2}".format(
                        e.message, i, i + size))
                    django.db.transaction.rollback(using=target_database)
                    django.db.transaction.leave_transaction_management(using=target_database)
                    raise e

            pbar.update(i - offset + 1)
            django.db.transaction.commit(using=target_database)
            django.db.transaction.leave_transaction_management(using=target_database)

            if self.concurrency_model != 'process':
                self.total_success += success
                self.total_failure += failure

            else:
                total_success += success
                total_failure += failure

        pbar.update(limit)
        pbar.finish()

        return total_success, total_failure

# ----------------------------------------------------------------------------------------------------------------------
