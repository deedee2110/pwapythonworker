import logging
from typing import TypeVar, Type, Optional

from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from starlette import status

from orchard.database import DbManager
from orchard.reponse import ResponseException, SearchResponseModel, ReturnStatus, ResponseModel

#: R is a type variable that is a subtype of the BaseModel type
# R = TypeVar("R", bound=BaseModel)
# I = TypeVar("I", bound=BaseModel)
M = TypeVar("M", bound=DbManager)

# logging
logger = logging.getLogger(__name__)

class BaseResource():

    def __init__(self, manager_type: Type[M]):
        self.t_manager = manager_type
        self.table_manager: DbManager = self.t_manager()


    def gen_query_params(self, search_query: str = None, extra_params: dict = None) -> (str, dict):
        """
        This method should be overloaded to handle extra_params.

        :param search_query:
        :param extra_params:
        :return:
        """
        if search_query is None or search_query == '':
            return '1', dict()
        man = self.table_manager
        schema = man.data_model_class.schema()
        conds = []
        for k, v in schema['properties'].items():
            if v['type'] == 'string':
                conds.append(f'{k} REGEXP :query')
        if len(conds) > 0:
            query = '(' + ' OR '.join(conds) + ')'
        bound_params = {"query": search_query}
        return query, bound_params

    def gen_cond_with_uid(self, id):
        # assume primary key
        man = self.table_manager
        table = man.table
        pkey = table.primary_key.columns.values()[0]
        return pkey == id

    def format_search(self, format, data):
        """
        This method should be overloaded.
        It is called by get with format !=''.
        :param format:
        :param data:
        :return:
        """
        return data

    def format_record(self, rec):
        """
        This method should be overloaded.
        :return:
        """
        return rec

    def format_data(self, format, data):
        """
        This method should be overloaded.
        It is called by get.
        :param format:
        :param data:
        :return:
        """
        data = [self.format_record(itm) for itm in data]
        return data

    def get(self,
            search_query: Optional[str] = None,
            page: int = 0,
            page_size: int = 100,
            order_by: Optional[str] = None,
            format: Optional[str] = None,
            extra_params: Optional[dict] = None
            ):
        man = self.table_manager
        (query, bound_params) = self.gen_query_params(search_query, extra_params)
        if format == 'option':
            page_size = None
        try:
            count: int = man.count(criteria=query, bound_params=bound_params)
            data = man.search(criteria=query,
                              page=page,
                              page_size=page_size,
                              order_by=order_by,
                              bound_params=bound_params)
        except (SQLAlchemyError, DBAPIError) as e:
            classname = __class__
            print(e)
            logger.error(f"[{classname}] Failed to get search results: {e}")
            raise ResponseException(code=status.HTTP_400_BAD_REQUEST, info="Failed to get search results")
        if format is not None:
            return self.format_search(format, data)
        return SearchResponseModel(status=ReturnStatus.SUCCESS.value,
                                   count=count,
                                   data=self.format_data(format, data),
                                   page=page,
                                   page_size=page_size)

    def get_with_id(self, uid):
        man = self.t_manager()
        cond = self.gen_cond_with_uid(uid)
        try:
            data = man.get_with_cond(cond, condIsText=False)
        except (SQLAlchemyError, DBAPIError) as e:
            classname = __class__
            logger.error(f"[{classname}] Failed to get search results: {e}")
            raise ResponseException(code=status.HTTP_400_BAD_REQUEST, info="Failed to get result")
        return ResponseModel(status=ReturnStatus.SUCCESS.value,
                             content=self.format_record(data),
                             info=f'retrived {uid}')

    def post(self, res_in):
        man = self.table_manager
        try:
            res = man.insert_with_get(res_in)
        except (SQLAlchemyError, DBAPIError) as e:
            classname = __class__
            logger.error(f"[{classname}] Failed to add record: {e}")
            raise ResponseException(code=status.HTTP_400_BAD_REQUEST, info="Failed to insert")
        return ResponseModel(status=ReturnStatus.SUCCESS.value,
                             content=self.format_record(res),
                             info=str({"rows_inserted": 1}))

    def put_with_id(self, uid, res_in):
        man = self.table_manager
        try:
            res = man.update_with_get(res_in, uid)
            if res is None:
                raise ResponseException(code=status.HTTP_404_NOT_FOUND,
                                        info=f"could not find data {uid} to update")
        except (SQLAlchemyError, DBAPIError) as e:
            classname = __class__
            logger.error(f"[{classname}] Failed to update: {e}")
            raise ResponseException(code=status.HTTP_400_BAD_REQUEST, info="Failed to update")
        return ResponseModel(status=ReturnStatus.SUCCESS.value,
                             content=self.format_record(res),
                             info=str({"rows_updated": 1}))

    def delete_with_id(self, uid):
        man = self.table_manager
        try:
            rows_deleted: int = man.delete(uid)
        except (SQLAlchemyError, DBAPIError) as e:
            classname = __class__
            logger.error(f"[{classname}] Failed to delete user and group mapping: {e}")
            raise ResponseException(code=status.HTTP_400_BAD_REQUEST, info="Failed to delete data")

        if rows_deleted > 0:
            return ResponseModel(status=ReturnStatus.SUCCESS.value,
                                 content=uid,
                                 info=f"1 record(s) deleted")

        return ResponseModel(status=ReturnStatus.FAIL.value,
                             content=uid,
                             info=f"Could not find a record to delete")
