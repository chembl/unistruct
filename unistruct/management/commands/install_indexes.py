from __future__ import print_function

__author__ = 'mnowotka'

import sys
from django.core.management.base import BaseCommand
from optparse import make_option
from django.core.management.color import no_style
from django.core.management.sql import custom_sql_for_model
from django.conf import settings
import traceback
from termcolor import colored
from unistruct.models import UcCtab, UcMols, UcFingerprints


M = 10**6
k = 10**3
DEFAULT_LIMIT_SIZE = 20*M
DEFAULT_CHUNK_SIZE = 10*k
LINE = '#' + '-' * 119

# ----------------------------------------------------------------------------------------------------------------------


def populate(kwargs):
    import django.core.management
    try:
        success, failure, records = django.core.management.call_command('populate', **kwargs)
    except:
        return
    return success, failure, records


# ----------------------------------------------------------------------------------------------------------------------


class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('--database', dest='database',
                    default=None, help='Target database'),
        make_option('--cartridge', dest='cartridge',
                    default=None, help='Which chemistry cartridge to use (rdkit or indigo)'),
        )

# ----------------------------------------------------------------------------------------------------------------------

    def __init__(self):
        super(Command, self).__init__()
        self.app_name = 'unistruct'
        self.verbosity = 0
        self.cursor = None
        self.sql_context = None
        self.cartridge = None
        self.fingerprints = {
            'rdkit': [
                {'function': 'torsionbv_fp', 'column':'torsionbv', 'index': 'torsionbv_idx', 'param': ''},
                {'function': 'morganbv_fp', 'column': 'morganbv', 'index': 'morganbv_idx', 'param': ''},
                {'function': 'featmorganbv_fp', 'column': 'featmorganbv', 'index': 'featmorganbv_idx', 'param': ''},
                {'function': 'rdkit_fp', 'column': 'rdkit', 'index': 'rdkit_idx', 'param': ''},
                {'function': 'atompairbv_fp', 'column': 'atompairbv', 'index': 'atompairbv_idx', 'param': ''},
                {'function': 'layered_fp', 'column': 'layered', 'index': 'layered_idx', 'param': ''},
                {'function': 'maccs_fp', 'column': 'maccs', 'index': 'maccs_idx', 'param': ''},
            ],
            'bingo': [
                {'function': 'Bingo.Fingerprint', 'column': 'full', 'index': None, 'param': ", 'full'"},
            ]
        }

# ----------------------------------------------------------------------------------------------------------------------

    def handle(self, *args, **options):
        from django.db import reset_queries
        reset_queries()
        self.verbosity = int(options.get('verbosity'))

        if settings.DEBUG:
            self.stderr.write("Django is in debug mode, which causes memory leak. "
                              "Set settings.DEBUG to False and run again.")
            return

        target_database = options.get('database')
        if not target_database:
            if self.verbosity >= 1:
                self.stderr.write("No target database given")
            return

        target_model_name = UcCtab._meta.object_name

        self.cartridge = options.get('cartridge')
        if not self.cartridge:
            if self.verbosity >= 1:
                self.stderr.write("No chemistry cartridge given")
            return

        if self.verbosity >= 2:
            self.stdout.write("Generating fingerprints and installing indexes for model {0} in database {1}"
                              .format(target_model_name, target_database))

        self.install_fingerprints_and_indexes(target_database)

        if self.verbosity >= 2:
            self.stdout.write("Finished preparing model {0}".format(target_model_name))

# ----------------------------------------------------------------------------------------------------------------------

    def install_fingerprints_and_indexes(self, target_database):

        from django.db import connections

        target_conn = connections[target_database]
        self.cursor = target_conn.cursor()
        self.sql_context = {'pk': UcCtab._meta.pk.name,
                       'mols_table': UcMols._meta.db_table,
                       'ctab_table': UcCtab._meta.db_table,
                       'fingerprints_table': UcFingerprints._meta.db_table,
                       'molfile': UcCtab._meta.fields[1].name,
                       'ctab': UcMols._meta.fields[1].name,
                      }

        self.activate_extension()
        self.create_binary_molfiles()
        self.add_primary_key(self.sql_context['pk'], self.sql_context['mols_table'])
        self.install_molecular_index()
        self.compute_fingerprints()
        self.install_fingerprints_indexes()
        self.add_primary_key(self.sql_context['pk'], self.sql_context['fingerprints_table'])

# ----------------------------------------------------------------------------------------------------------------------

    def install_fingerprints_indexes(self):
        if not self.fingerprints[self.cartridge]:
            return
        for names in self.fingerprints[self.cartridge]:
            if names.get('index'):
                self.create_index(names['index'], names['column'], self.sql_context['fingerprints_table'])

# ----------------------------------------------------------------------------------------------------------------------

    def compute_fingerprints(self):

        self.try_execute_sql('drop table if exists {fingerprints_table}'.format(**self.sql_context), {},
                             'Dropping table {fingerprints_table}...'.format(**self.sql_context))

        if not self.fingerprints[self.cartridge]:
            return
        fps = ['select {pk}'.format(**self.sql_context)]
        for names in self.fingerprints[self.cartridge]:
            fps.append('{function}({ctab}{param}) as {column}'.format(**dict(self.sql_context, **names)))
        sql = ', '.join(fps)
        sql += ' into {fingerprints_table} from {mols_table}'.format(**self.sql_context)
        self.try_execute_sql(sql, {}, 'Computing fingerprints...')

# ----------------------------------------------------------------------------------------------------------------------

    def activate_extension(self):
        if self.cartridge == 'rdkit':
            self.try_execute_sql('create extension if not exists rdkit', {}, 'Activating RDKit cartridge...')
        elif self.cartridge == 'bingo':
            self.try_execute_sql('SELECT Bingo.GetVersion()', {}, 'Verifying Bingo installation...')

# ----------------------------------------------------------------------------------------------------------------------

    def add_primary_key(self, column_name, table_name):

        sql_context = {
            'column_name': column_name,
            'table_name': table_name,
        }

        self.try_execute_sql('alter table {table_name} add primary key ({column_name})'
                             .format(**sql_context), {}, 'Installing primary key on {column_name}.{table_name}...'
                             .format(**sql_context))

# ----------------------------------------------------------------------------------------------------------------------

    def create_binary_molfiles(self):

        self.try_execute_sql('drop table if exists {mols_table}'.format(**self.sql_context), {},
                             'Dropping table {mols_table}...'.format(**self.sql_context))

        if self.cartridge == 'rdkit':
            self.try_execute_sql('select distinct {pk}, mol_from_ctab({molfile}::cstring) {ctab} '
                                 'into {mols_table} from {ctab_table} '
                                 'where is_valid_ctab({molfile}::cstring)'
                                 .format(**self.sql_context), {}, 'Creating binary molfile objects...')
        elif self.cartridge == 'bingo':
            self.try_execute_sql('select distinct {pk}, bingo.CompactMolecule({molfile}, false) {ctab} '
                                 'into {mols_table} from {ctab_table} '
                                 'where bingo.CheckMolecule({molfile}) is not null'
                                 .format(**self.sql_context), {}, 'Creating binary molfile objects...')

# ----------------------------------------------------------------------------------------------------------------------

    def install_molecular_index(self):
        if self.cartridge == 'rdkit':
            self.create_index('rdkit_mol_idx', self.sql_context['ctab'], self.sql_context['mols_table'])
        elif self.cartridge == 'bingo':
            self.create_index('bingo_mol_idx', self.sql_context['ctab'], self.sql_context['mols_table'], 'bingo_idx',
                              'bingo.bmolecule')

# ----------------------------------------------------------------------------------------------------------------------

    def create_index(self, index_name, column_name, table_name, index_type='gist', param=''):

        sql_context = {
            'index_name': index_name,
            'column_name': column_name,
            'table_name': table_name,
            'index_type': index_type,
            'param': param,
        }

        self.try_execute_sql('create index {index_name} on {table_name} using {index_type}({column_name} {param})'
                             .format(**sql_context), {}, 'Installing index {index_name} on {column_name}.{table_name}'
                             .format(**sql_context))

# ----------------------------------------------------------------------------------------------------------------------

    def try_execute_sql(self, sql, params, desc, ignores=()):

        if self.verbosity >= 2:
            self.stdout.write(LINE + '\n')
            self.stdout.write(desc)
        if self.verbosity >= 3:
            self.stdout.write(LINE + '\n')
            self.stdout.write(sql + '\n')
            self.stdout.write(str(params))
        try:
            self.cursor.execute(sql, params)
        except Exception as e:
            if any(ign in str(e.message) for ign in ignores):
                if self.verbosity >= 2:
                    self.stdout.write(colored('Warning: {0}'.format(e.message), 'yellow'))
                return
            if self.verbosity >= 1:
                self.stderr.write('Failed: {0}'.format(e.message))
            sys.exit()
        if self.verbosity >= 2:
            self.stdout.write(colored('Done.\n', 'green'))

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



