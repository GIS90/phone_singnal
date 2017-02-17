# -*- coding: utf-8 -*-

"""
------------------------------------------------
describe:
    data server be used to run server, query database, write file and stop server about data source
    it need run to modify config file that contains
    database host, database port, database user, database password, database default,
    data folder
    data type

        +------------+
        | Run server |
        +------------+
              |
              v
        +------------+
        | Query data |
        +------------+
              |
              v
        +------------+
        | Write data |
        +------------+
              |
              v
        +------------+
        | Stop server|
        +------------+
------------------------------------------------
"""

from core.define import *
from core.generator import *
from core.log import *
from core.update import *

__version__ = "v.10"
__author__ = "PyGo"
__time__ = "2016/12/7"


def main():
    generator = Generaor()
    run_data = get_run_data_config()
    for rd in run_data:
        if rd == DataType.population:
            generator.population()
        elif rd == "2.1":
            generator.population_hour()
        elif rd == "2.2":
            generator.population_hour_avg()
        elif rd == "2.3":
            generator.population_period()
        elif rd == "2.4":
            generator.population_period_avg()
        elif rd == "2.5":
            generator.population_density()
        elif rd == "3.1":
            generator.trips_day_zl()
        elif rd == "3.2":
            generator.trips_day_ffx()
        elif rd == "3.3":
            generator.trips_period()
        elif rd == "4.1":
            generator.rw_work_total()
        elif rd == "4.2":
            generator.rw_work_density()
        elif rd == "4.3":
            generator.rw_live_total()
        elif rd == "4.4":
            generator.rw_live_density()
        elif rd == "4.5":
            generator.rw_noworker_total()
        elif rd == "4.6":
            generator.rw_noworker_density()
        elif rd == "4.7":
            generator.rw_rw_value()
        elif rd == "5.1":
            generator.outlander_total()
        elif rd == "5.2":
            generator.outlander_density()
        elif rd == "5.3":
            generator.outlander_city()
        elif rd == "5.4":
            generator.outlander_china()
        elif rd == "6.1":
            generator.road_population()
        elif rd == "6.2":
            generator.road_worker()
        elif rd == "6.3":
            generator.road_attract()
        elif rd == "7":
            generator.area()
        else:
            pass

    import time
    time.sleep(2)
    is_update_txt = get_is_update_txt()
    source_folder = get_source_folder_config()
    target_folder = get_target_folder_config()
    if is_update_txt:
        UpdateTxt.transfer(source_folder, target_folder)
    else:
        pass


if __name__ == "__main__":
    msg = "Data server start run......"
    log.info(msg)
    main()
