from datetime import datetime
from dotenv import load_dotenv
from logger import Logger
from os import listdir
from os.path import dirname, exists, isfile, join
import json
import math
import os
import pymssql
import re
import time

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

class PantheonController:
    def __init__(self):
        self.server = os.environ.get('SERVER')
        self.port = os.environ.get('PORT')
        self.database = os.environ.get('DATABASE')
        self.user = os.environ.get('USERNAME')
        self.password = os.environ.get('PASSWORD')
    
    def establish_connection(self):
        self.conn = pymssql.connect(
            server = self.server,
            port = self.port,
            user = self.user, 
            password = self.password,
            database = self.database)
        
        self.cursor = self.conn.cursor()
    
    def close_connection(self):
        self.conn.close()

    def load_procedures(self):
        self.establish_connection()
        self.cursor.execute('SELECT object_name(object_id) AS Name, object_definition(object_id) AS Definition FROM sys.objects WHERE type=\'P\';')

        for row in self.cursor:
            file = open(f'procedures/{row[0]}.sql', 'w')
            file.write(row[1])
            file.close()

        self.close_connection()

    def load_tables(self):
        self.establish_connection()
        self.cursor.execute('SELECT object_name(object_id) AS Name FROM SYS.TABLES;')
        tables = list()

        for table in self.cursor:
            tables.append(table[0])

        tables_json = json.dumps(tables, indent = 4)
        tables_file = open('files/tables.json', 'w')
        tables_file.write(tables_json)
        tables_file.close()

        self.close_connection()

    def __validate_files(self, files):
        validated = True

        for file in files:
            if not exists(file):
                file_name = file.split('/').pop()
                print(f'File {file_name} does not exist!')
                validated = False

        return validated
    
    def __generate_markdown(self, procedure_calls, procedure, indentation = 0, called_procedures = list()):
        markdown = '\t' * indentation + '- ' + procedure + (' [REPEATED]' if procedure in called_procedures else '') + '\n'
        index = 0

        for table in procedure_calls[procedure]['referenced_tables']:
            index += 1
            markdown += '\t' * (indentation + 1) + f'{index}. ' + table + '\n'

        if procedure in called_procedures:
            return markdown
        
        called_procedures.append(procedure)

        for called_procedure in procedure_calls[procedure]['referenced_procedures']:
            procedure_markdown = self.__generate_markdown(procedure_calls, called_procedure, indentation + 1, called_procedures)
            markdown += procedure_markdown
            procedure_markdown_file = open(f'files/procedure_calls/{called_procedure}.md', 'w')
            procedure_markdown_file.write(procedure_markdown)
            procedure_markdown_file.close()

        return markdown

    def load_procedure_calls(self):
        start = time.time()

        if not self.__validate_files(['files/tables.json']):
            return
        
        tables = None

        with open('files/tables.json') as json_file:
            tables = json.load(json_file)

        if tables is None:
            print('There was a problem with loading tables!')
            return
        
        path = 'procedures'
        files = [f for f in listdir(path) if isfile(join(path, f))]

        if len(files) == 0:
            print('No procedures loaded!')
            return
        
        call_log = ''
        procedure_calls = dict()

        for file_name in files:
            procedure_name = re.sub(r'\.sql', '', file_name)
            call_log += f"{procedure_name}\n"
            
            open_file = open(f'procedures/{file_name}', 'r')
            procedure_definition = open_file.read()
            open_file.close()

            procedure_calls[procedure_name] = { 'referenced_procedures': [], 'referenced_tables': [] }

            for called_file_name in files:
                if file_name == called_file_name:
                    continue

                called_procedure_name = re.sub(r'\.sql', '', called_file_name)
                
                if called_procedure_name in procedure_definition:
                    procedure_calls[procedure_name]['referenced_procedures'].append(called_procedure_name)

            for table in tables:
                if table in procedure_definition:
                    procedure_calls[procedure_name]['referenced_tables'].append(table)
            
            call_log += '\n'
        
        markdown_content = ''

        for procedure in procedure_calls:
            procedure_markdown_content = self.__generate_markdown(procedure_calls, procedure, 0, list())
            markdown_content += procedure_markdown_content

        markdown_file = open('files/procedure_calls.md', 'w')
        markdown_file.write(markdown_content)
        markdown_file.close()

        procedure_calls_json = json.dumps(procedure_calls, indent = 4)
        procedure_calls_file = open('files/procedure_calls_full.json', 'w')
        procedure_calls_file.write(procedure_calls_json)
        procedure_calls_file.close()

        end = time.time()
        executionSeconds = end - start
        executionMinutes = math.floor(executionSeconds / 60)
        executionSeconds -= (executionMinutes * 60)

        print(f'Finished in {executionMinutes}min {round(executionSeconds)}s')

    def analyze_table_usages(self):
        if not exists('files/procedure_calls.json'):
            print('You have to load procedures first!')
            return

        if not exists('files/tables.json'):
            print('You have to load tables first!')
            return

        procedure_calls = None
        tables = None

        with open('files/procedure_calls.json') as json_file:
            procedure_calls = json.load(json_file)

        if procedure_calls is None:
            print('There was a problem with loading procedures!')
            return

        with open('files/tables.json') as json_file:
            tables = json.load(json_file)

        if tables is None:
            print('There was a problem with loading tables!')
            return

    def __check_for_table_reference(self, procedure_calls, procedure_name, table_name, checked_procedures = list()):
        if procedure_name in checked_procedures:
            return
        
        checked_procedures.append(procedure_name)
        
        procedure = procedure_calls.get(procedure_name)

        if procedure is None:
            print(f'The procedure {procedure_name} was not found in the database!')
            return

        if table_name in procedure['referenced_tables']:
            Logger.success(f'Table {table_name} found is referenced in the procedure {procedure_name}')

        for referenced_procedure in procedure['referenced_procedures']:
            self.__check_for_table_reference(procedure_calls, referenced_procedure, table_name, checked_procedures)

    def find_table_in_procedure(self, procedure_name, table_name):
        if not exists(f'files/procedure_calls.json'):
            print('Load the procedures first!')
            return
        
        procedure_calls = None
        
        with open('files/procedure_calls_full.json') as json_file:
            procedure_calls = json.load(json_file)

        if procedure_calls is None:
            print('There was a problem with loading procedures!')
            return
        
        self.__check_for_table_reference(procedure_calls, procedure_name, table_name, list())
    
    def clone_database(self):
        today = datetime.now().strftime('%m_%d_%Y')
        folders = ['constraints', 'tables', 'views']

        for folder in folders:
            folder_path = f'backup/{today}/{folder}'

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

        self.establish_connection()
        self.cursor.execute(f'SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'dbo\' AND TABLE_CATALOG = \'{self.database}\';')
        Logger.success(f'Successfully loaded tables from Pantheon')
        tables = {}

        for row in self.cursor.fetchall():
            table_name, column_name, data_type, max_length, is_nullable = row
            
            if table_name not in tables:
                tables[table_name] = []

            tables[table_name].append([column_name, data_type, max_length, is_nullable])
        
        Logger.success('Reindexed Pantheon table data')

        for table_name in tables:
            file_path = f'backup/{today}/tables/{table_name}.sql'
            columns = []

            for row in tables[table_name]:
                column_name, data_type, max_length, is_nullable = row
                column = f'    {column_name} {data_type}'
                if max_length is not None:
                    column += f'({max_length})'
                if is_nullable == 'NO':
                    column += ' NOT NULL'
                columns.append(column)

            columns_str = ',\n'.join(columns)
            sql = f'CREATE TABLE {table_name} (\n{columns_str}\n);'
            file = open(file_path, 'w')
            file.write(sql)
            file.close()
        
        Logger.success(f'Saved {len(tables)} tables to SQL files')
        self.cursor.execute(f'SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = \'dbo\' AND TABLE_CATALOG = \'{self.database}\';')
        Logger.success(f'Successfully loaded views from Pantheon')
        counter = 0

        for row in self.cursor.fetchall():
            view_name, view_definition = row
            file_path = f'backup/{today}/views/{view_name}.sql'
            file = open(file_path, 'w')
            file.write(view_definition)
            file.close()
            counter += 1
        
        Logger.success(f'Saved {counter} views to SQL files')

        self.cursor.execute(f'''
            SELECT t.name                                  TableName,
                c.name                                  ConstraintName,
                (SELECT STRING_AGG(cols.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal)
                    FROM sys.index_columns ic
                            JOIN sys.columns cols ON ic.object_id = cols.object_id AND ic.column_id = cols.column_id
                    WHERE ic.object_id = c.parent_object_id
                    AND ic.index_id = c.unique_index_id) Keys,
                i.fill_factor AS                         'FillFactor'
            FROM sys.tables t
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    INNER JOIN sys.objects o ON t.object_id = o.object_id
                    INNER JOIN sys.key_constraints c ON o.object_id = c.parent_object_id
                    INNER JOIN sys.indexes i ON c.parent_object_id = i.object_id AND c.unique_index_id = i.index_id
            WHERE c.type = 'PK' AND s.name = 'dbo';
        ''')

        Logger.success('Successfully loaded primary key constraints')
        sql = ''
        counter = 0

        for row in self.cursor.fetchall():
            table_name, constraint_name, keys, fill_factor = row
            fill_factor_sql = f' WITH(FILLFACTOR = {fill_factor})' if fill_factor != 0 else ''
            sql += f'ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({keys}){fill_factor_sql};\n'
            counter += 1
        
        file = open(f'backup/{today}/constraints/primary_keys.sql', 'w')
        file.write(sql)
        file.close()

        Logger.success(f'Saved {counter} primary keys to the SQL file')

        self.cursor.execute('''
            SELECT OBJECT_NAME(fk.parent_object_id)     AS TableName,
                fk.name                                 AS ConstraintName,
                c.name                                  AS ColumnName,
                OBJECT_NAME(fk.referenced_object_id)    AS ReferencedTable,
                rc.name                                 AS ReferencedColumn
            FROM sys.foreign_keys fk
                    INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                    INNER JOIN sys.columns c ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
                    INNER JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id;
        ''')

        foreign_keys = {}

        for row in self.cursor.fetchall():
            table_name, constraint_name, column_name, referenced_table, referenced_column = row 

            if table_name not in foreign_keys:
                foreign_keys[table_name] = {}
            
            if constraint_name not in foreign_keys[table_name]:
                foreign_keys[table_name][constraint_name] = {
                    'column_name': column_name,
                    'referenced_table': referenced_table
                }

            if 'referenced_columns' not in foreign_keys[table_name][constraint_name]:
                foreign_keys[table_name][constraint_name]['referenced_columns'] = []
            
            foreign_keys[table_name][constraint_name]['referenced_columns'].append(referenced_column)

        
        sql = ''
        counter = 0

        for table_name in foreign_keys:
            for constraint_name in foreign_keys[table_name]:
                column_name = foreign_keys[table_name][constraint_name]['column_name']
                referenced_table = foreign_keys[table_name][constraint_name]['referenced_table']
                column_names = ', '.join(foreign_keys[table_name][constraint_name]['referenced_columns'])
                sql += f'ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_names}) REFERENCES {referenced_table};\n'
                counter += 1
        
        file = open(f'backup/{today}/constraints/foreign_keys.sql', 'w')
        file.write(sql)
        file.close()

        Logger.success(f'Saved {counter} foreign keys to the SQL file')
