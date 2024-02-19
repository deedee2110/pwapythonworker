from enum import Enum
from typing import Any, List, Optional

from fastapi import HTTPException
from pydantic import BaseModel


class ReturnStatus(Enum):
    SUCCESS = "success"
    FAIL = "fail"


class ResponseBaseModel(BaseModel):
    status: str


class ResponseModel(ResponseBaseModel):
    content: Optional[Any]
    info: str


class SearchResponseModel(ResponseBaseModel):
    count: int = 0
    data: Optional[List[Any]]
    page: Optional[int]
    page_size: Optional[int]


class ResponseException(Exception):
    def __init__(self, code: int, info: str):
        self.code = code
        self.info = info


class ResponseHTTPException(HTTPException):
    def __init__(
            self, status: str, info: str, status_code: int, detail: Optional[Any] = None
    ) -> None:
        super().__init__(status_code=status_code, detail=detail)
        self.status = status
        self.info = info
