from tempfile import NamedTemporaryFile
from sqlalchemy import select, distinct, func
import pandas as pd


def data_exporter(man, field_list, default_filter=None):
    table = man.table
    fields = []
    groups = []
    filters = []
    #pivot
    pivot_index=[]
    pivot_column=[]
    pivot_value=[]
    print('default_filter', default_filter)
    if default_filter is not None and len(default_filter) != 0:
        filters.append(*default_filter)
    field_list.sort(key=lambda itm: itm['ord'])
    for fld in field_list:
        fld_name = fld['name']
        alias = fld_name
        if fld['sel']:
            if fld['alias']:
                alias = fld['alias']
            if fld['function'] == 'count':
                fields.append(func.count(table.c[fld_name]).label(alias))
            if fld['function'] == 'count_distinct':
                fields.append(func.count(distinct(table.c[fld_name])).label(alias))
            if fld['function'] == 'min':
                fields.append(func.min(table.c[fld_name]).label(alias))
            if fld['function'] == 'max':
                fields.append(func.max(table.c[fld_name]).label(alias))
            if fld['function'] == 'avg':
                fields.append(func.avg(table.c[fld_name]).label(alias))
            if fld['function'] == 'sum':
                fields.append(func.sum(table.c[fld_name]).label(alias))
            if fld['function'] == 'value':
                fields.append(table.c[fld_name].label(alias))
        if fld['function'] == 'groupby':
            if fld['sel']:
                fields.append(table.c[fld_name].label(alias))
            groups.append(table.c[fld_name])
        if 'filter' in fld.keys() and fld['filter'] is not None:
            fld_filter = fld['filter']
            # print(fld_name, 'fld_filter', fld_filter)
            if isinstance(fld_filter, list):
                if len(fld_filter) > 0:
                    # print('filter list')
                    filters.append(table.c[fld_name].in_(fld_filter))
            else:
                fld_filter = fld_filter.strip()
                if fld_filter == '':
                    continue
                cond = 'like'
                if 'cond' in fld.keys():
                    cond = fld['cond']
                if cond == '=':
                    filters.append(table.c[fld_name].__eq__(fld_filter))
                if cond == '<':
                    filters.append(table.c[fld_name].__lt__(fld_filter))
                if cond == '<=':
                    filters.append(table.c[fld_name].__le__(fld_filter))
                if cond == '>':
                    filters.append(table.c[fld_name].__gt__(fld_filter))
                if cond == '>=':
                    filters.append(table.c[fld_name].__ge__(fld_filter))
                if cond == '!=':
                    filters.append(table.c[fld_name].__ne__(fld_filter))
                if cond == 'in':
                    filters.append(table.c[fld_name].in_(fld_filter.split(',')))
                if cond == 'like':
                    filters.append(table.c[fld_name].like("%" + fld_filter + "%"))
        if 'pivot' in fld.keys() and fld['pivot'] is not None:
            if fld['pivot'] == 'index':
                pivot_index.append(fld_name)
            if fld['pivot'] == 'column':
                pivot_column.append(fld_name)
            if fld['pivot'] == 'value':
                pivot_value.append(fld_name)
    sql = select(fields)
    # print('filters', filters)
    for f in filters:
        sql = sql.where(f)
    for g in groups:
        sql = sql.group_by(g)
    print('sql', sql)
    conn = man.db_engine.connect()
    res = conn.execute(sql)
    if len(pivot_index) ==0 and len(pivot_column) == 0 and len(pivot_value) == 0:
        return res
    df = pd.DataFrame(res)
    #print(df)
    print(pivot_index, pivot_column, pivot_value)
    dat= df.pivot(index=pivot_index,columns=pivot_column, values=pivot_value)
    print(dat)
    print('keys',dat.keys())
    return dat



def view_exporter(man, field_list, default_filter=None):
    """
    Select data from view and generate result
    :param man:
    :param field_list:
    :return:
    """
    res = data_exporter(man, field_list, default_filter)
    if isinstance(res, pandas.DataFrame):
        return {
            "type":"html",
            "html": res.to_html()
        }

    print(res.keys())
    result = {
        "type": "table",
        "columns": [
            {
                "key": k,
                "title": k,
                "dataIndex": k
            }
            for k in res.keys()],
        "data": [itm for itm in res]
    }
    return result


def xls_exporter(man, field_list, default_filter=None):
    """
    Select data from view and generate XLS sheet
    :param man:
    :param field_list:
    :return: filename (xlsx)
    """
    res = data_exporter(man, field_list, default_filter)
    if isinstance(res, pd.DataFrame):
        df=res
        temp_file = NamedTemporaryFile(delete=False, suffix=".xlsx")
        df.to_excel(temp_file.name)
    else:
        df = pd.DataFrame([itm for itm in res], columns=res.keys())
        temp_file = NamedTemporaryFile(delete=False, suffix=".xlsx")
        df.to_excel(temp_file.name, index=False)
    print(df)
    print('XLSX > ', temp_file.name)
    return temp_file.name
