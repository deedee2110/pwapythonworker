import json
import os.path
from pprint import pprint
from typing import List, Dict, Optional

from sqlalchemy.exc import ProgrammingError

from orchard.database import db_engine, connectivity
from pydantic import BaseModel
from sqlalchemy import text


class TableInfo(BaseModel):
    table_name: str
    fields: List[Dict]
    indexes: List[Dict]
    create_table_sql: Optional[str]


def dump_fields(conn, table_name):
    rows = conn.execute(text('SHOW COLUMNS FROM `' + table_name + '`'))
    dat = []
    for row in rows:
        obj = {}
        for k in rows.keys():
            obj[k] = row[k]
        dat.append(obj)
    return dat


def dump_indexes(conn, table_name):
    rows = conn.execute(text('SHOW INDEXES FROM `' + table_name + '`'))
    dat = []
    for row in rows:
        obj = {}
        for k in rows.keys():
            if k not in ('Cardinality', 'Ignored',):
                obj[k] = row[k]
        dat.append(obj)
    return dat


def dump_create_table(conn, table_name):
    # 'SHOW CREATE TABLE xx' may only work on MariaDB and MySQL
    tmp = list(conn.execute('SHOW CREATE TABLE `' + table_name+'`'))
    # Table Name, Create Table
    try:
        return tmp[0]['Create Table']
    except:
        # it might be a view
        return None


def get_create_table_sql(table_name, schema_version=-1):
    if schema_version == -1:
        # latest version
        with open('schemas/schema_info.json', 'rb') as f:
            info = json.load(f)
            schema_version = info['schema_version']
    with open(os.path.join('schemas', schema_version, table_name), 'r') as f1:
        table_info = TableInfo(**json.load(f))
    return table_info.create_table_sql


def db_structure_dump(schema_version='0000000000', table_names: List[str] = None):
    print('Dumping schemas..', schema_version)
    connector = connectivity(db_engine)
    with connector() as conn:
        if table_names is None:
            table_names = [tablename for (tablename, ) in list(conn.execute('SHOW TABLES'))]
        for table_name in table_names:
            fname = os.path.join('schemas', schema_version, table_name + '.json')
            print(table_name, ' > ', fname)
            table_info = TableInfo(
                table_name=table_name,
                fields=dump_fields(conn, table_name),
                indexes=dump_indexes(conn, table_name),
                create_table_sql=dump_create_table(conn, table_name)
            )
            os.makedirs(os.path.dirname(fname), mode=0o766, exist_ok=True)
            with open(fname, 'w') as f:
                json.dump(table_info.dict(), f, indent=4)


def db_structure_check(schema_version='0000000000'):
    print('Checking schemas..', schema_version)
    connector = connectivity(db_engine)
    all_valid = True
    invalid_list = []
    with connector() as conn:
        for d in os.scandir(os.path.join('schemas', schema_version)):
            with open(os.path.join('schemas', schema_version, d.name), 'r') as f:
                table_info = TableInfo(**json.load(f))
                try:
                    fields = dump_fields(conn, table_info.table_name)
                    indexes = dump_indexes(conn, table_info.table_name)
                except ProgrammingError as e:
                    print(' < Table does not exists.. ' + table_info.table_name)
                    invalid_list.append({'type': 'TABLE_MISSING', 'info': table_info.table_name})
                    all_valid |= False
                    continue
                field_valid = {f['Field']: False for f in table_info.fields}
                field_info = {f['Field']: f for f in table_info.fields}
                index_valid = {f['Key_name'] + '_' + str(f['Seq_in_index']): False for f in table_info.indexes}
                index_info = {f['Key_name'] + '_' + str(f['Seq_in_index']): f for f in table_info.indexes}
                print(' * Checking ' + table_info.table_name)
                for f in fields:
                    if f['Field'] not in field_info.keys():
                        print(' < missing field', f['Field'])
                        invalid_list.append({'type': 'FIELD_MISSING', 'info': f})
                        continue
                    v = (f == field_info[f['Field']])
                    field_valid[f['Field']] = v
                    all_valid &= v
                    if not v:
                        print(' < field mismatch', f['Field'])
                        invalid_list.append({'type': 'FIELD_MISMATCH', 'info': f})

                for i in indexes:
                    m_key = i['Key_name'] + '_' + str(i['Seq_in_index'])
                    if m_key not in index_info.keys():
                        print(' < missing index', m_key)
                        invalid_list.append({'type': 'INDEX_MISSING', 'info': m_key})
                        continue
                    k = i['Key_name'] + '_' + str(i['Seq_in_index'])
                    v = (i == index_info[k])
                    index_valid[k] = v
                    all_valid &= v
                    if not v:
                        print(' < index mismatch', k)
                        invalid_list.append({'type': 'INDEX_MISMATCH', 'info': i})
                table_valid = True
                for k in field_valid.keys():
                    table_valid &= field_valid[k]
                    if not field_valid[k]:
                        print(' > missing field ', k)
                        invalid_list.append({'type': 'FIELD_MISSING', 'info': k})
                for k in index_valid.keys():
                    table_valid &= index_valid[k]
                    if not index_valid[k]:
                        print(' > missing index ', k)
                        invalid_list.append({'type': 'INDEX_MISSING', 'info': k})
                if table_valid:
                    print('... matched')
                all_valid &= table_valid

    if not all_valid:
        print('Invalid schemas ..')
        pprint(invalid_list)
    else:
        print(' * schemas matched.')
    return all_valid, invalid_list
