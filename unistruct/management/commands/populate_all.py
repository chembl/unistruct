from __future__ import print_function

__author__ = 'mnowotka'

from django.core.management.base import BaseCommand
from optparse import make_option
from django.core.management.color import no_style
from django.core.management.sql import custom_sql_for_model
from blessings import Terminal
from django.conf import settings
import traceback
import multiprocessing as mp

M = 10**6
k = 10**3
DEFAULT_LIMIT_SIZE = 20*M
DEFAULT_CHUNK_SIZE = 10*k

# ----------------------------------------------------------------------------------------------------------------------


def populate(kwargs):
    import django.core.management
    try:
        success, failure, empty, ignored, records = django.core.management.call_command('populate', **kwargs)
    except:
        return
    return success, failure, empty, ignored, records


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
        self.total_empty = 0
        self.total_ignored = 0
        self.app_name = 'unistruct'
        self.verbosity = 0
        self.concurrency_model = None
        self.n_jobs = 0
        self.records_to_be_populated = 0

# ----------------------------------------------------------------------------------------------------------------------

    def handle(self, *args, **options):
        from django.db.models import get_model
        from django.db import reset_queries
        reset_queries()
        term = Terminal(stream=self.stdout)
        print(term.clear())
        self.verbosity = int(options.get('verbosity'))

        if settings.DEBUG:
            self.stderr.write("Django is in debug mode, which causes memory leak. "
                              "Set settings.DEBUG to False and run again.")
            return

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

        self.concurrency_model = options.get('concurrencyModel')
        self.n_jobs = int(options.get('numberOfJobs', 0))

        source_model_name = options.get('sourceModelName')
        if not source_model_name:
            if self.verbosity >= 1:
                self.stderr.write("No source model name given")
            return

        source_model = get_model(self.app_name, source_model_name)
        if not source_model:
            if self.verbosity >= 1:
                self.stderr.write("No model named {0} defined for {1} application"
                                  .format(source_model_name, self.app_name))
            return

        target_model_name = options.get('targetModelName')
        if not target_model_name:
            if self.verbosity >= 1:
                self.stderr.write("No target model name given")
            return

        target_model = get_model(self.app_name, target_model_name)
        if not target_model:
            if self.verbosity >= 1:
                self.stderr.write("No model named {0} defined for {1} application"
                                  .format(target_model_name, self.app_name))
            return

        if self.verbosity >= 2:
            self.stdout.write("Populating model {0} from database {1} with data taken from model {2} from database {3}"
                              .format(target_model_name, target_database, source_model_name, source_database))

        size = int(options.get('chunk_size'))
        limit = int(options.get('limit'))
        offset = int(options.get('offset'))

        inchi_conversion = options.get('inchiConversion')
        if not inchi_conversion:
            if self.verbosity >= 1:
                self.stderr.write("No inchi conversion method given")
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

# ----------------------------------------------------------------------------------------------------------------------

    def check_table(self, target_model_name, target_database):

        from django.db import connections
        from django.db import transaction
        from django.db.models import get_model

        target_model = get_model(self.app_name, target_model_name)

        target_conn = connections[target_database]
        target_cursor = target_conn.cursor()
        style = no_style()
        tables = target_conn.introspection.table_names()
        seen_models = target_conn.introspection.installed_models(tables)
        pending_references = {}
        show_traceback = self.verbosity > 1

        if target_model not in seen_models:

            if self.verbosity >= 2:
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

            transaction.commit_unless_managed(using=target_database)

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
                    transaction.rollback_unless_managed(using=target_database)
                else:
                    transaction.commit_unless_managed(using=target_database)
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
                    transaction.rollback_unless_managed(using=target_database)
                else:
                    transaction.commit_unless_managed(using=target_database)

# ----------------------------------------------------------------------------------------------------------------------

    def dispatch(self, target_model_name, target_database, source_model_name, source_database, size, limit, offset,
                 inchi_conversion):
        from django.db.models import get_model
        from django.db import connections

        display_offset = 10

        source_model = get_model(self.app_name, source_model_name)
        target_model = get_model(self.app_name, target_model_name)

        self.check_table(target_model_name, target_database)

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

        if source_count - offset < limit and self.verbosity > 2:
            self.stderr.write("The actual number of records in the source model's table ({0}) is lower that the given "
                              "limit ({1}). Only {0} records will be populated".format(source_count, limit))

        records_to_be_populated = min(source_count - offset, limit)
        self.total_success = 0
        self.total_failure = 0
        self.total_empty = 0
        self.total_ignored = 0

        if self.verbosity >= 1:
            self.stdout.write("source count is {0}, target count is {1} limit is {2}, offset is {3}, total number of "
                              "records to populate is {4}".format(source_count, target_count, limit, offset,
                                                                  records_to_be_populated))

        if not self.concurrency_model:
            success, failure, empty, ignored, records = populate({'chunkSize': size,
                                                  'targetDatabase': target_database,
                                                  'sourceDatabase': source_database,
                                                  'sourceModelName': source_model_name,
                                                  'targetModelName': target_model_name,
                                                  'offset': offset,
                                                  'verbosity': 0,
                                                  'limit': records_to_be_populated,
                                                  'inchiConversion': inchi_conversion})
            self.total_success += success
            self.total_failure += failure
            self.total_empty += empty
            self.total_ignored += ignored
            self.records_to_be_populated += records
            return

        if not self.n_jobs:
            self.n_jobs = mp.cpu_count()
        self.stdout.write("Splitting task into {0} jobs".format(self.n_jobs))

        ranges = range(offset, records_to_be_populated + offset,
                       (records_to_be_populated / self.n_jobs) or 1) + [records_to_be_populated + offset]
        ranges = [(ranges[i], ranges[i+1]) for i in range(0, len(ranges)-1)]

        self.stdout.write("ranges: {0}".format(ranges))

        if self.concurrency_model == 'process':
            connections[source_database].close()
            connections[target_database].close()
            self.stdout.write("Using multiprocessing for concurrency")
            pool = mp.Pool(self.n_jobs)
            for idx, (start, stop) in enumerate(ranges):
                pool.apply_async(populate,
                                 args=({'chunkSize': size,
                                        'targetDatabase': target_database,
                                        'sourceDatabase': source_database,
                                        'sourceModelName': source_model_name,
                                        'targetModelName': target_model_name,
                                        'offset': start,
                                        'verbosity': 0,
                                        'limit': stop-start,
                                        'inchiConversion': inchi_conversion,
                                        'displayOffset': display_offset,
                                        'displayIndex': display_offset + idx},),
                                 callback=self.callback)

            pool.close()
            pool.join()

        else:
            if self.verbosity >= 1:
                self.stderr.write("Unrecognised concurrency model {0}".format(self.concurrency_model))


# ----------------------------------------------------------------------------------------------------------------------

    def callback(self, (success, failure, empty, ignored, records)):
        self.total_success += success
        self.total_failure += failure
        self.total_empty += empty
        self.total_ignored += ignored
        self.records_to_be_populated += records
        return

# ----------------------------------------------------------------------------------------------------------------------

