"""
History:
- August 16, 2021 - Krerk Piromsopa,Ph.D.
    add and_criteria to support criteria generator for search
"""

import logging
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel
from sqlalchemy import MetaData, Table, create_engine, text, func, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql import Insert
from sqlalchemy.sql.expression import TextClause, UnaryExpression, select
import contextlib

from orchard import settings

DB_URI = settings.config.DB_URI

db_engine: Engine = create_engine(
    DB_URI,
    pool_size=20,
    max_overflow=0,
    pool_recycle=3600,
    #echo=True,
    #echo_pool="debug",
)

meta = MetaData()

#: T is a type variable that is a subtype of the BaseModel type
T = TypeVar("T", bound=BaseModel)

# logging
logger = logging.getLogger(__name__)

def connectivity(engine):
    # Wrapper method for creating a connection
    # This is provided to support transaction
    # This code is taken from SQLAclhemy
    # https://docs.sqlalchemy.org/en/14/core/connections.html
    connection = None

    @contextlib.contextmanager
    def connect():
        nonlocal connection

        if connection is None:
            connection = engine.connect()
            with connection:
                with connection.begin():
                    yield connection
        else:
            yield connection

    return connect


class DbManager:
    """
    A manager class for a table, similar to a data access object
    (DAO).

    When subclassing this class to create a new data mongo_doc class, do
    the following:

        1. Extends the sql_create_table property with the CREATE TABLE
           SQL statement that define the structure of the table;

        2. Define a constructor/instantiation method (__init__(self))
           in the subclass and call the DbManager's
           constructor/instantiation with the name of the table and
           the data mongo_doc class object as arguments.
    """

    #: a string of CREATE TABLE SQL statement used to create the table.
    sql_create_table = ""

    def __init__(self,
                 table_name: str,
                 data_model_class: Type[T],
                 engine: Engine = db_engine,
                 meta=meta,
                 auto_create_table: bool = True
                 ) -> None:
        """
        Initialize a manager class by opening a new connection and
        invoking reflect_table() method to reflect a table (and create
        it if it does not exist).

        :param table_name: the name of the table that this class
            manages
        :type table_name: str

        :param data_model_class: the data mongo_doc class object (that is
            the subclass of BaseModel) that the manager will convert
            the result rows into
        :type data_model_class: Type[T], where T is a subtype of
            BaseModel type
        """
        self.db_engine: Engine = engine
        self.meta = meta
        self.table_name = table_name
        self.data_model_class = data_model_class
        self.reflect_table()
        self.cache = {}
        logger.info("DbManager.init %s" % (table_name))

    def reflect_table(self) -> None:
        """
        Load information about the table that this class manages using
        reflection and store it in the table attribute.

        Also, if the table does not exist, invoke the table_create()
        method to create it.
        """
        try:
            # Check to see if we've already reflected the table
            self.table
        except AttributeError:
            try:
                self.table: Table = Table(
                    self.table_name, self.meta, autoload_with=self.db_engine
                )
            except NoSuchTableError:
                self.table_create()
                logger.info("Reflecting table '%s'" % self.table)

    def table_create(self) -> None:
        """
        Create a new table if none exists.

        Once the table has been created, invoke the reflect_table()
        method.
        """
        logger.info("Creating table '%s'" % self.table_name)
        sql = self.sql_create_table
        if sql=='':
            logger.info('-- NO CREATE TABLE SQL specified')
            return
        with self.db_engine.begin() as conn:
            conn.execute(text(sql))
        self.reflect_table()

    def row_to_obj(self, row: Any) -> T:
        """
        Return a new instance of a data mongo_doc class initialized from
        the given row.

        :param row: a row retrieved from the database
        :type row: Any

        :return: a new instance of a data mongo_doc class, T, initialized
                 with data from the given row
        :rtype: T, where T is a subtype of BaseModel type
        """
        if row is None:
            # Note that the following can potentially raise Pydantic's
            # ValidationError, depending on the data being instantiated.
            return self.data_model_class()
        return self.data_model_class(** row._asdict())

    def count(self, criteria: Optional[str] = None,
              bound_params: Optional[Dict[str, Any]] = None,
              connector=None) -> int:
        """
        Return the number of all rows in the table.

        If a transaction is required,
        shares connector=connectivity(db_engine).

        :return: the number of all rows in the table
        :rtype: int
        """
        table = self.table
        if criteria is None:
            sql = select([func.count("*")]).select_from(table)
        else:
            sql = select([func.count("*")]).select_from(table).where(text(criteria))
        # print(sql)
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            if bound_params is None:
                res = conn.execute(sql)
            else:
                logger.info(
                    "[api.core.database.DbManager.search]"
                    + f" [bound params] {bound_params}"
                )
                res = conn.execute(sql, bound_params)

        res = res.fetchone()
        return res["count_1"]

    # Get Object
    def get_with_key(
            self, key: Optional[str] = None,
            uid: Optional[str] = None,
            connector=None,
            cacheable=False
    ) -> Optional[T]:
        """
        Return a new instance of a data mongo_doc class initialized from
        the row with either the given primary key or UID.

        :param connector: use custom connector (default: None)
        :param cacheable: use cache (default: false)
        :param key: the primary key of the row to be retrieved
        :type key: Optional[str]

        :param uid: the string of 64 hexadecimal digits UID of the row
            to be retrieved
        :type uid: Optional[str]

        :return: a new instance of a data mongo_doc class, T, initialized
                 with data from the row with the given primary key, or
                 a None if the row with the given key is not found
        :rtype: Optional[T], where T is a subtype of BaseModel type
        """
        if cacheable:
            if key in self.cache.keys():
                logger.info('get_with_key.cache HIT %s', key)
                return self.cache[key]
            else:
                logger.info('get_with_key.cache MISS %s', key)
        table = self.table
        if key is not None:
            pkey = table.primary_key.columns.values()[0]
            sql = table.select().where(pkey == key)
        elif uid is not None and "sql" not in locals():
            uid_column = self.table_name + "_uid"
            sql = table.select().where(text(f"{uid_column} = '{uid}'"))
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            res = conn.execute(sql)
        rec = res.fetchone()
        if rec == None:
            return None
        obj: T = self.row_to_obj(rec)
        if cacheable:
            logger.info('get_with_key.cache LOAD %s', key)
            self.cache[key] = obj
        return obj

    def get_with_cond(self,
                      cond,
                      condIsText=True,
                      connector=None
                      ) -> Optional[T]:
        table = self.table
        if condIsText:
            m_cond = text(cond)
        else:
            m_cond = cond
        sql = table.select().where(m_cond)
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            res = conn.execute(sql)
        rec = res.fetchone()
        if rec == None:
            return None
        obj: T = self.row_to_obj(rec)
        return obj

    def search_with_cond(self,
                         cond,
                         order_by=None,
                         condIsText=True,
                         connector=None
                         ) -> Optional[List[T]]:
        table = self.table
        if condIsText:
            m_cond = text(cond)
        else:
            m_cond = cond
        sql = table.select().where(m_cond)
        if order_by is not None:
            if condIsText:
                m_order = text(order_by)
            else:
                m_order = order_by
            sql = sql.order_by(m_order)
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            res = conn.execute(sql)
        arr: List[T] = [self.row_to_obj(r) for r in res.fetchall()]
        return arr

    # Search
    def search(
            self,
            criteria: str,
            page: int = 0,
            page_size: Optional[int] = None,
            order_by: Optional[str] = None,
            bound_params: Optional[Dict[str, Any]] = None,
            connector=None
    ) -> List[T]:
        """
        Select/Search the table and return a list containing the
        results.

        :param criteria: the criteria to be used for
            searching/filtering
        :type criteria: str

        :param page: the page of the results, defaults to 0
        :type page: int

        :param page_size: the number of the results per page, defaults
            to None
        :type page_size: int, optional

        :param order_by: SQL ordering expressions used to order the
            results, defaults to None
        :type order_by: str, optional

        :return: a list containing the results of the search query as
                 instances of a data mongo_doc class, T
        :rtype: List[T], where T is a subtype of BaseModel type
        """
        table = self.table
        # Build the order-by clauses
        if order_by is None:
            pkey = table.primary_key.columns.values()[0]
            order_by_clauses: Union[UnaryExpression, TextClause] = pkey.asc()
        else:
            order_by_clauses = text(order_by)
        # Build the main SQL "search" statement
        sql = table.select().where(text(criteria)).order_by(order_by_clauses)
        if page_size is not None:
            sql = sql.limit(page_size).offset(page * page_size)
        # Execute the SQL statement
        logger.info(f"[api.core.database.DbManager.search] [SQL] {sql}")
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            if bound_params is None:
                res = conn.execute(sql)
            else:
                logger.info(
                    "[api.core.database.DbManager.search]"
                    + f" [bound params] {bound_params}"
                )
                res = conn.execute(sql, bound_params)

        # Use list comprehension to aggregate results into a list
        arr: List[T] = [self.row_to_obj(r) for r in res.fetchall()]
        return arr

    # Insert New Object
    def insert(self,
               objs: Union[T, List[T]],
               connector=None
               ) -> Union[int, List[Any]]:
        """
        Insert a single or multiple rows.

        :param objs: either an instance of a data mongo_doc class, T, to
            be inserted as a new row, or a list of instances of the
            data mongo_doc class T, to be inserted as multiple new rows
        :type objs: Union[T List[T]], where T is a subtype of
            BaesModel type

        :return: either the primary key of the row that was inserted,
                 or a list of the primary keys of the rows that were
                 inserted
        :rtype: Union[int, List[Any]]
        """
        table = self.table
        try:
            dat: Union[Dict[str, Any], List[Dict[str, Any]]] = objs.__dict__
            sql: Union[Insert, str] = table.insert().values(**dat)
            logger.info(f"[api.core.database.DbManager.insert] [SINGLE] [SQL] {sql} {sql.compile().params}")
            if connector is None:
                connector = connectivity(self.db_engine)
            with connector() as conn:
                res = conn.execute(sql)
            logger.debug('primary key '+str(res.inserted_primary_key))
            return res.inserted_primary_key[0]
        except AttributeError:
            dat = []
            for i in objs:
                dat.append(i.__dict__)
            pkey = table.primary_key.columns.values()[0]
            # column_names = [c.name for c in table.columns]
            column_names = [f"`{i}`" for i in dat[0].keys()]
            values = [list(i.values()) for i in dat]
            logger.debug(values)
            sql = f"""INSERT INTO `{self.table_name}` ({", ".join(column_names)})
VALUES ({", ".join([str(i) for i in values[0]])})"""
            logger.debug(sql)
            for v in values[1:]:
                sql += f""",
       ({", ".join([str(i) for i in v])})"""
            sql += f"""
RETURNING {pkey.name}"""
            logger.info(f"[api.core.database.DbManager.insert] [MULTI] [SQL] {sql}")
            if connector is None:
                connector = connectivity(self.db_engine)
            with connector() as conn:
                res = conn.execute(text(sql))
            return res.fetchall()

    # Update Object
    def update(
            self,
            obj: T,
            key: Optional[str] = None,
            uid: Optional[str] = None,
            criteria: Optional[str] = None,
            connector=None
    ) -> int:
        """
        Update a row.

        Can specify which row to update using either the primary key
        of the row/object, the UID of the row/object, or a generic
        WHERE clause criteria.

        :param obj: an instance of a data mongo_doc class, T, to be used
            to update the row
        :type obj: T, where T is a subtype of BaseModel type

        :param key: (Optional, default) the current key of the row
            that is to be updated
        :type key: Optional[str]

        :param uid: the string of 64 hexadecimal digits UID of the row
            to be retrieved
        :type uid: Optional[str]

        :param criteria: (Optional) the criteria for specifying which
            row to be deleted
        :type criteria: Optional[str]

        :return: a number of rows that were updated
        :rtype: int
        """
        dat = obj.__dict__
        table = self.table
        sql=''
        if key is not None:
            pkey = table.primary_key.columns.values()[0]
            sql = table.update().where(pkey == key).values(dat)
        elif uid is not None and "sql" not in locals():
            uid_column = self.table_name + "_uid"
            sql = table.update().where(text(f"{uid_column} = '{uid}'")).values(dat)
        elif criteria is not None and "sql" not in locals():
            sql = table.update().where(text(criteria)).values(dat)
        logger.info(sql)
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            res = conn.execute(sql)
        return res.rowcount

    # Delete Object
    def delete(
            self,
            key: Optional[str] = None,
            uid: Optional[str] = None,
            criteria: Optional[str] = None,
            connector=None
    ) -> int:
        """
        Delete a row.

        Can specify which row to delete using either the primary key
        of the row or a generic WHERE clause criteria.

        :param key: (Optional, default) the primary key of the row
            that is to be deleted
        :type key: Optional[str]

        :param uid: the string of 64 hexadecimal digits UID of the row
            to be retrieved
        :type uid: Optional[str]

        :param criteria: (Optional) the criteria for specifying which
            row to be deleted
        :type criteria: Optional[str]

        :return: a number of rows that were deleted
        :rtype: int
        """
        table = self.table
        if key is not None:
            pkey = table.primary_key.columns.values()[0]
            sql = table.delete().where(pkey == key)
        elif uid is not None and "sql" not in locals():
            uid_column = self.table_name + "_uid"
            sql = table.delete().where(text(f"{uid_column} = '{uid}'"))
        elif criteria is not None and "sql" not in locals():
            sql = table.delete().where(text(criteria))
        logger.info(sql)
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            res = conn.execute(sql)
        return res.rowcount

    def insert_with_get(self, obj, connector=None):
        key = self.insert(obj)
        return self.get_with_key(key)

    def update_with_get(self,
                        obj,
                        key,
                        criteria: Optional[str] = None,
                        connector=None):
        rows_update = self.update(obj=obj,
                                  key=key,
                                  criteria=criteria,
                                  connector=connector)
        if rows_update > 0:
            # Getting the primary key value from the obj if it is available
            if self.table.primary_key.columns.values()[0].name in obj.__fields_set__:
                key = obj.dict()[self.table.primary_key.columns.values()[0].name]

            return self.get_with_key(key, connector=connector)
        else:
            return None

    def select_distinct(self,
                        select_field,
                        reverse: Optional[bool] = False,
                        connector=None) -> List[Any]:
        """
        Select distinct value from a field.\f

        :param select_field:
        :param reverse:
        :param connector:
        :return:
        """

        order_by_clauses = text(select_field + (" ASC" if not reverse else " DESC"))
        sql = select([func.distinct(text(select_field))]).select_from(self.table).order_by(order_by_clauses)
        logger.info(sql)
        if connector is None:
            connector = connectivity(self.db_engine)
        with connector() as conn:
            res = conn.execute(sql)
        rec = res.fetchall()
        result = []
        [result.append(r["distinct_1"]) for r in rec]
        return result


    def cache_flush(self):
        self.cache = {}