from typing import Optional
from datetime import datetime
from pydantic import BaseModel

from orchard.database import DbManager


class DocTemplateIn(BaseModel):
    template_name: str
    page_size: Optional[str]
    page_width: Optional[int]
    page_height: Optional[int]
    margin_top: Optional[int]
    margin_right: Optional[int]
    margin_bottom: Optional[int]
    margin_left: Optional[int]
    body: str
    data_connector: str
    data_query: Optional[str]


class DocTemplate(DocTemplateIn):
    template_id: int
    creation_time: datetime
    modification_time: datetime


class DocTemplateManager(DbManager):
    sql_create_table = """
CREATE TABLE `doc_template` (
  `template_id` int NOT NULL AUTO_INCREMENT,
  `template_name` varchar(50) NOT NULL,
  `page_size` varchar(5) NOT NULL,
  `page_width` int(10) unsigned NOT NULL,
  `page_height` int(10) unsigned NOT NULL,
  `margin_top` int(10) unsigned NOT NULL,
  `margin_right` int(10) unsigned NOT NULL,
  `margin_bottom` int(10) unsigned NOT NULL,
  `margin_left` int(10) unsigned NOT NULL,
  `body` mediumtext NOT NULL,
  `data_connector` varchar(128) NOT NULL,
  `data_query` mediumtext NOT NULL,
  `creation_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modification_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`template_id`),
  KEY `template_name` (`template_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

    def __init__(self):
        super().__init__('doc_template', DocTemplate)
