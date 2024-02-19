from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from orchard.database import DbManager


class ExpPresetIn(BaseModel):
    preset_name: str
    preset_description: Optional[str]
    meta: Optional[str]
    note: Optional[str]


class ExpPreset(ExpPresetIn):
    preset_id: int
    update_time: datetime
    create_time: datetime


class ExpPresetManager(DbManager):
    sql_create_table = """
CREATE TABLE `export_preset` (
  `preset_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `preset_name` varchar(200) DEFAULT NULL,
  `preset_description` tinytext DEFAULT NULL,
  `meta` mediumtext DEFAULT NULL,
  `note` mediumtext DEFAULT NULL,
  `update_time` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `create_time` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`preset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

    def __init__(self):
        super().__init__("export_preset", ExpPreset)
