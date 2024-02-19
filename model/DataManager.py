from datetime import datetime

from pydantic import BaseModel

from orchard.database import DbManager
class DataIn(BaseModel):
    data_time: datetime
    UV: float
    light: float
    temp: float
    air_humidity: float
    soil_humidity: float
class Data(DataIn):
    data_id: int

class DataManager(DbManager):

    sql_create_table = """
CREATE TABLE `data` (
  `data_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `data_time` datetime DEFAULT NULL,
  `UV` float DEFAULT NULL,
  `light` float DEFAULT NULL,
  `temp` float DEFAULT NULL,
  `air_humidity` float DEFAULT NULL,
  `soil_humidity` float DEFAULT NULL,
  PRIMARY KEY (`data_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """

    def __init__(self):
        super().__init__("data", DataIn)