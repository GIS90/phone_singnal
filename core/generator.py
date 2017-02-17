# -*- coding: utf-8 -*-

"""
------------------------------------------------
describe:
    data generator

demo:
    generator = Generaor()
    generator.population()
------------------------------------------------
"""

import codecs

from config import *
from dbhandler import *
from define import *
from log import *

reload(sys)
sys.setdefaultencoding("utf8")

__version__ = "v.10"
__author__ = "PyGo"
__time__ = "2016/12/7"


class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args)
        return cls._instance


class Generaor(Singleton):
    def __init__(self):
        try:
            host, port, user, password, database = get_mssdb_config()
            self._data_folder = get_source_folder_config()
        except Exception as e:
            emsg = "Generaor read config file is error: %s, system exit" % e.message
            log.error(emsg)
            sys.exit(0)
        try:
            self._dbhandler = DBHandler(host, port, user, password, database)
            self._dbhandler.open()
        except Exception as e:
            emsg = "Generaor DBHandler open db is error: %s, system exit" % e.message
            log.error(emsg)
            sys.exit(0)
        else:
            msg = "Generaor DBHandler db is open"
            log.info(msg)

    def _check_folder(self, ckfolder):
        """
        判断文件夹是否存在，不存在新建
        :param ckfolder:
        :return:
        """
        assert isinstance(ckfolder, basestring)

        if not os.path.exists(ckfolder):
            try:
                os.makedirs(ckfolder)
                msg = "Folder %s is not exist and create" % ckfolder
                log.info(msg)
            except Exception as e:
                emsg = "Generaor check folder %s is error: %s" % (ckfolder, e.message)
                log.error(emsg)
                sys.exit(0)

    def _check_file(self, ckfile):
        """
        判断文件是否存在，存在则删除进行数据更新
        :param ckfile:
        :return:
        """
        assert isinstance(ckfile, basestring)

        if os.path.exists(ckfile):
            try:
                msg = "File %s is exist and update" % ckfile
                log.info(msg)
                os.unlink(ckfile)
            except Exception as e:
                emsg = "Generaor check file %s is error: %s" % (ckfile, e.message)
                log.error(emsg)
                sys.exit(0)

    def _check_table(self, table):
        """
        检查表是否为空
        :param table:
        :return:
        """
        assert isinstance(table, basestring)

        sql = "select count(*) from %s" % table
        try:
            rlt = self._dbhandler.query(sql, 1)
        except Exception as e:
            emsg = "Generaor _check_table %s is error: %s" % (table, e.message)
            log.error(emsg)
            return 0
        else:
            return rlt

    def population(self):
        """
        数据概况-人口分类统计的数据
        记录不同时间，不同类型的人口数据
        :return:
        """
        msg = "Generaor population is run"
        log.info(msg)

        population_folder = os.path.abspath(os.path.join(self._data_folder, "population"))
        population_file = os.path.abspath(os.path.join(population_folder, "populationDay.js"))
        self._check_folder(population_folder)
        self._check_file(population_file)

        population_type = ["population", "resident", "noworker", "workder", "outlander",
                           "passer", "outlandresident", "tourist"]
        population_date_sql = "select day from population group by day"
        try:
            population_date = self._dbhandler.query(population_date_sql, 2)
        except Exception as e:
            emsg = "Generaor population_date query date is error: %s" % e.message
            log.error(emsg)
            pass

        fw = codecs.open(population_file, 'w', 'utf-8')
        fw.write('var populationDayData={')
        date_count = 1
        for sql_date in population_date:
            fw.write("\n\t")
            fw.write('"%s":{' % sql_date[0])
            type_count = 1
            for sql_type in population_type:
                sql = "select %s from population where day = '%s'" % (sql_type, sql_date[0])
                try:
                    rlt = self._dbhandler.query(sql, 1)
                except Exception as e:
                    emsg = "Generaor population is error: %s" % e.message
                    log.error(emsg)
                    pass
                line = '"%s":"%d"' % (sql_type, int(rlt))
                fw.write(line)
                fw.write(',') if type_count < len(population_type) else fw.write("}")
                type_count += 1
            fw.write(',') if date_count < len(population_date) else 0
            date_count += 1
        fw.write("\n")
        fw.write('}')
        fw.close()

        msg = "Generaor population is finish data export"
        log.info(msg)

    def population_hour(self):
        """
        人口密度-人口动态分布的数据
        :return:
        """
        msg = "Generaor population_hour is run"
        log.info(msg)

        population_hour_hour = [i for i in range(0, 24, 1)]
        population_hour_type = ['population', 'noworker', 'workder', 'outlander']
        for unit in UnitType.type:
            population_hour_folder = os.path.abspath(os.path.join(self._data_folder, "population_density/population_hour/" + unit))
            self._check_folder(ckfolder=population_hour_folder)
            if unit is "big":
                population_hour_date_sql = "select day from population_hour_big group by day"
            elif unit is "middle":
                population_hour_date_sql = "select day from population_hour_middle group by day"
            elif unit is "small":
                population_hour_date_sql = "select day from population_hour_small group by day"
            try:
                population_hour_date = self._dbhandler.query(population_hour_date_sql, 2)
            except Exception as e:
                emsg = "Generaor population_hour %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in population_hour_date:
                for sql_type in population_hour_type:
                    population_hour_file = os.path.join(population_hour_folder, sql_date[0] + sql_type + '.js')
                    self._check_file(ckfile=population_hour_file)
                    fw = codecs.open(population_hour_file, 'w', 'utf-8')
                    fw.write('var peopleNum={')
                    fw.write('"peopleD":{')
                    for sql_hour in population_hour_hour:
                        fw.write('"%d":[' % sql_hour)
                        if unit is "big":
                            population_hour_sql = "select dqsm, %s from population_hour_big where day = '%s' and hour = %d" \
                                                  % (sql_type, sql_date[0], sql_hour)
                        elif unit is "middle":
                            population_hour_sql = "select zqsm, %s from population_hour_middle where day = '%s' and hour = %d" \
                                                  % (sql_type, sql_date[0], sql_hour)
                        elif unit is "small":
                            population_hour_sql = "select xqsm, %s from population_hour_small where day = '%s' and hour = %d" \
                                                  % (sql_type, sql_date[0], sql_hour)
                        try:
                            rows = self._dbhandler.query(population_hour_sql, 2)
                        except Exception as e:
                            emsg = "Generaor population_hour %s query data is error: %s" % (unit, e.message)
                            log.error(emsg)
                            continue
                        row_num = len(rows)
                        n = 1
                        for row in rows:
                            name = row[0]
                            density = int(row[1])
                            line = '{"name":"%s","value":%d}' % (name, density)
                            fw.write(line)
                            fw.write(',') if n < row_num else 0
                            n += 1
                        fw.write(']')
                        fw.write(',') if sql_hour < 23 else 0
                    fw.write('}}')
                    fw.close()
        msg = "Generaor population_hour is finish data export"
        log.info(msg)

    def population_hour_avg(self):
        """
        人口密度-平均人口动态分布的源数据
        :return:
        """
        msg = "Generaor population_hour_avg is run"
        log.info(msg)

        population_hour_avg_week = ['workday', 'weekend']
        population_hour_avg_hour = [i for i in range(0, 24, 1)]
        population_hour_avg_type = ['population', 'noworker', 'workder', 'outlander']
        for unit in UnitType.type:
            population_hour_avg_folder = os.path.abspath(os.path.join(self._data_folder, "population_density/population_hour_avg/" + unit))
            self._check_folder(ckfolder=population_hour_avg_folder)
            if unit is "big":
                population_hour_avg_date_sql = "select month from population_hour_avg_big group by month"
            elif unit is "middle":
                population_hour_avg_date_sql = "select month from population_hour_avg_middle group by month"
            elif unit is "small":
                population_hour_avg_date_sql = "select month from population_hour_avg_small group by month"
            try:
                population_hour_avg_date = self._dbhandler.query(population_hour_avg_date_sql, 2)
            except Exception as e:
                emsg = "Generaor population_hour_avg %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in population_hour_avg_date:
                for sql_type in population_hour_avg_type:
                    for sql_week in population_hour_avg_week:
                        sql_date_new = sql_date[0][:4] + "-" + sql_date[0][4:]
                        population_hour_avg_file = os.path.join(population_hour_avg_folder, sql_week + sql_date_new + sql_type + '.js')
                        self._check_file(ckfile=population_hour_avg_file)
                        fw = codecs.open(population_hour_avg_file, 'w', 'utf-8')
                        fw.write('var peopleNum={')
                        fw.write('"peopleD":{')
                        for sql_hour in population_hour_avg_hour:
                            fw.write('"%d":[' % sql_hour)
                            if unit is "big":
                                population_hour_avg_sql = "select dqsm, %s from population_hour_avg_big where month = '%s' and hour = %d and workday = '%s'" \
                                                          % (sql_type, sql_date[0], sql_hour, sql_week)
                            elif unit is "middle":
                                population_hour_avg_sql = "select zqsm, %s from population_hour_avg_middle where month = '%s' and hour = %d and workday = '%s'" \
                                                          % (sql_type, sql_date[0], sql_hour, sql_week)
                            elif unit is "small":
                                population_hour_avg_sql = "select xqsm, %s from population_hour_avg_small where month = '%s' and hour = %d and workday = '%s'" \
                                                          % (sql_type, sql_date[0], sql_hour, sql_week)
                            try:
                                rows = self._dbhandler.query(population_hour_avg_sql, 2)
                            except Exception as e:
                                emsg = "Generaor population_hour_avg %s query data is error: %s" % (unit, e.message)
                                log.error(emsg)
                                continue
                            row_num = len(rows)
                            n = 1
                            for row in rows:
                                name = row[0]
                                density = int(row[1])
                                line = '{"name":"%s","value":%d}' % (name, density)
                                fw.write(line)
                                fw.write(',') if n < row_num else 0
                                n += 1
                            fw.write(']')
                            fw.write(',') if sql_hour < 23 else 0
                        fw.write('}}')
                        fw.close()
        msg = "Generaor population_hour_avg is finish data export"
        log.info(msg)

    def population_period(self):
        """
        人口密度-分时段人口动态分布的源数据
        :return:
        """
        msg = "Generaor population_period is run"
        log.info(msg)

        population_period_type = ['population', 'noworker', 'workder', 'outlander']
        population_period_period = {'yj': '%夜间%',
                                    'cgf': '%早高峰%',
                                    'wj': '%晚间%',
                                    'wgf': '%晚高峰%',
                                    'bt': '%白天%'}
        for unit in UnitType.type:
            population_period_folder = os.path.abspath(os.path.join(self._data_folder, "population_density/population_period/" + unit))
            self._check_folder(ckfolder=population_period_folder)
            if unit is "big":
                population_period_date_sql = "select day from population_period_big group by day"
            elif unit is "middle":
                population_period_date_sql = "select day from population_period_middle group by day"
            elif unit is "small":
                population_period_date_sql = "select day from population_period_small group by day"
            try:
                population_period_date = self._dbhandler.query(population_period_date_sql, 2)
            except Exception as e:
                emsg = "Generaor population_period %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in population_period_date:
                for sql_type in population_period_type:
                    for (sql_period_key, sql_period_value) in population_period_period.items():
                        population_period_file = os.path.join(population_period_folder, sql_date[0] + sql_period_key + sql_type + '.js')
                        self._check_file(ckfile=population_period_file)
                        fw = codecs.open(population_period_file, 'w', 'utf-8')
                        fw.write('var peopleNum=[')
                        if unit is "big":
                            population_period_sql = "select dqsm, %s from population_period_big where day = '%s' and period like '%s'" \
                                                    % (sql_type, sql_date[0], sql_period_value)
                        elif unit is "middle":
                            population_period_sql = "select zqsm, %s from population_period_middle where day = '%s' and period like '%s'" \
                                                    % (sql_type, sql_date[0], sql_period_value)
                        elif unit is "small":
                            population_period_sql = "select xqsm, %s from population_period_small where day = '%s' and period like '%s'" \
                                                    % (sql_type, sql_date[0], sql_period_value)
                        try:
                            rows = self._dbhandler.query(population_period_sql, 2)
                        except Exception as e:
                            emsg = "Generaor population_period %s query data is error: %s" % (unit, e.message)
                            log.error(emsg)
                            continue
                        row_num = len(rows)
                        n = 1
                        for row in rows:
                            name = row[0]
                            density = int(row[1])
                            line = '{"name":"%s","value":%d}' % (name, density)
                            fw.write(line)
                            fw.write(',') if n < row_num else 0
                            n += 1
                        fw.write(']')
                        fw.close()
        msg = "Generaor population_period is finish data export"
        log.info(msg)

    def population_period_avg(self):
        """
        人口密度-分时段平均人口动态分布的源数据
        :return:
        """
        msg = "Generaor population_period_avg is run"
        log.info(msg)

        population_period_avg_type = ['population', 'noworker', 'workder', 'outlander']
        population_period_avg_period = {'yj': '%夜间%',
                                        'cgf': '%早高峰%',
                                        'wj': '%晚间%',
                                        'wgf': '%晚高峰%',
                                        'bt': '%白天%'}
        population_period_avg_weekend = ['workday', 'weekend']
        for unit in UnitType.type:
            population_period_avg_folder = os.path.abspath(os.path.join(self._data_folder, "population_density/population_period_avg/" + unit))
            self._check_folder(ckfolder=population_period_avg_folder)
            if unit is "big":
                population_period_avg_date_sql = "select month from population_period_avg_big group by month"
            elif unit is "middle":
                population_period_avg_date_sql = "select month from population_period_avg_middle group by month"
            elif unit is "small":
                population_period_avg_date_sql = "select month from population_period_avg_small group by month"
            try:
                population_period_avg_date = self._dbhandler.query(population_period_avg_date_sql, 2)
            except Exception as e:
                emsg = "Generaor population_period %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in population_period_avg_date:
                for sql_type in population_period_avg_type:
                    for sql_week in population_period_avg_weekend:
                        for (sql_period_avg_key, sql_period_avg_value) in population_period_avg_period.items():
                            population_period_avg_file = os.path.join(population_period_avg_folder, sql_week + sql_date[0] + sql_period_avg_key + sql_type + '.js')
                            self._check_file(ckfile=population_period_avg_file)
                            fw = codecs.open(population_period_avg_file, 'w', 'utf-8')
                            fw.write('var peopleNum=[')
                            if unit is "big":
                                population_period_avg_sql = "select dqsm, %s from population_period_avg_big where month = '%s' and period like '%s' and workday='%s'" \
                                                            % (sql_type, sql_date[0], sql_period_avg_value, sql_week)
                            elif unit is "middle":
                                population_period_avg_sql = "select zqsm, %s from population_period_avg_middle where month = '%s' and period like '%s' and workday='%s'" \
                                                            % (sql_type, sql_date[0], sql_period_avg_value, sql_week)
                            elif unit is "small":
                                population_period_avg_sql = "select xqsm, %s from population_period_avg_small where month = '%s' and period like '%s' and workday='%s'" \
                                                            % (sql_type, sql_date[0], sql_period_avg_value, sql_week)
                            try:
                                rows = self._dbhandler.query(population_period_avg_sql, 2)
                            except Exception as e:
                                emsg = "Generaor population_period_avg %s query data is error: %s" % (unit, e.message)
                                log.error(emsg)
                                continue
                            row_num = len(rows)
                            n = 1
                            for row in rows:
                                name = row[0]
                                density = int(row[1])
                                line = '{"name":"%s","value":%d}' % (name, density)
                                fw.write(line)
                                fw.write(',') if n < row_num else 0
                                n += 1
                            fw.write(']')
                            fw.close()
        msg = "Generaor population_period_avg is finish data export"
        log.info(msg)

    def population_density(self):
        """
        人口+岗位密度分布
        :return:
        """
        msg = "Generaor population_density is run"
        log.info(msg)

        for unit in UnitType.type:
            population_density_folder = os.path.abspath(os.path.join(self._data_folder, "population_density/population_density/" + unit))
            population_density_file = os.path.abspath(os.path.join(population_density_folder, "popdens.js"))
            self._check_folder(population_density_folder)
            self._check_file(population_density_file)
            fw = codecs.open(population_density_file, 'w', 'utf-8')
            fw.write('var peopleNum=[')
            if unit == 'small':
                sql = "select xqsm, population from population_density_work_small"
            elif unit == 'middle':
                sql = "select zqsm, population from population_density_work_middle"
            elif unit == 'big':
                sql = "select dqsm, population from population_density_work_big"
            rows = self._dbhandler.query(sql, 2)
            rows_num = len(rows)
            n = 1
            for row in rows:
                name = row[0]
                num = row[1]
                line = '{"name":"%s","value":%s}' % (name, num)
                fw.write(line)
                fw.write(',') if n < rows_num else 0
                n += 1
            fw.write(']')
            fw.close()
        msg = "Generaor area is finish data export"
        log.info(msg)

    def trips_day_zl(self):
        """
        出行分布-日出行OD总量的源数据
        :return:
        """
        msg = "Generaor trips_day_zl is run"
        log.info(msg)

        for unit in UnitType.type:
            trips_day_zl_folder = os.path.abspath(os.path.join(self._data_folder, "trips/trips_day/zongliang/" + unit))
            self._check_folder(ckfolder=trips_day_zl_folder)
            if unit is "big":
                trips_day_zl_date_sql = "select day from trips_day_zl_big group by day"
            elif unit is "middle":
                trips_day_zl_date_sql = "select day from trips_day_zl_middle group by day"
            elif unit is "small":
                trips_day_zl_date_sql = "select day from trips_day_zl_small group by day"
            try:
                trips_day_zl_date = self._dbhandler.query(trips_day_zl_date_sql, 2)
            except Exception as e:
                emsg = "Generaor population_period %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in trips_day_zl_date:
                ptrips_day_zl_file = os.path.join(trips_day_zl_folder, sql_date[0] + '.js')
                self._check_file(ckfile=ptrips_day_zl_file)
                fw = codecs.open(ptrips_day_zl_file, 'w', 'utf-8')
                fw.write('var tripsData=[')
                fw.write('\n')
                if unit is "big":
                    trips_day_zl_sql = "select leftDQSM, arrDQSM, population from trips_day_zl_big where day = '%s'" % sql_date[0]
                elif unit is "middle":
                    trips_day_zl_sql = "select leftZQSM, arrZQSM, population from trips_day_zl_middle where day = '%s'" % sql_date[0]
                elif unit is "small":
                    trips_day_zl_sql = "select leftXQSM, arrXQSM, population from trips_day_zl_small where day = '%s'" % sql_date[0]
                try:
                    rows = self._dbhandler.query(trips_day_zl_sql, 2)
                except Exception as e:
                    emsg = "Generaor trips_day_zl %s query data is error: %s" % (unit, e.message)
                    log.error(emsg)
                    continue
                row_num = len(rows)
                n = 0
                for row in rows:
                    leftname = row[0]
                    arrname = row[1]
                    num = int(row[2])
                    line = '[{"name":"%s"},{"name":"%s","value":%s}]' % (leftname, arrname, num)
                    fw.write('\t')
                    fw.write(line)
                    fw.write(',') if n < row_num - 1 else 0
                    fw.write('\n')
                    n += 1
                fw.write(']')
                fw.close()

        msg = "Generaor trips_day_zl is finish data export"
        log.info(msg)

    def trips_day_ffx(self):
        """
        出行分布-日出行OD分方向的源数据
        :return:
        """
        msg = "Generaor trips_day_ffx is run"
        log.info(msg)

        for unit in UnitType.type:
            trips_day_ffx_folder = os.path.abspath(os.path.join(self._data_folder, "trips/trips_day/fenfangxiang/" + unit))
            self._check_folder(ckfolder=trips_day_ffx_folder)
            if unit is "big":
                trips_day_ffx_date_sql = "select day from trips_day_ffx_big group by day"
            elif unit is "middle":
                trips_day_ffx_date_sql = "select day from trips_day_ffx_middle group by day"
            elif unit is "small":
                trips_day_ffx_date_sql = "select day from trips_day_ffx_small group by day"
            try:
                trips_day_ffx_date = self._dbhandler.query(trips_day_ffx_date_sql, 2)
            except Exception as e:
                emsg = "Generaor trips_day_ffx %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            if unit is "big":
                trips_day_ffx_qysm_sql = "select leftDQSM from trips_day_ffx_big where len(leftDQSM)>0 group by leftDQSM"
            elif unit is "middle":
                trips_day_ffx_qysm_sql = "select leftZQSM from trips_day_ffx_middle where len(leftZQSM)>0 group by leftZQSM"
            elif unit is "small":
                trips_day_ffx_qysm_sql = "select leftXQSM from trips_day_ffx_small where len(leftXQSM)>0 group by leftXQSM"
            try:
                trips_day_ffx_qysm = self._dbhandler.query(trips_day_ffx_qysm_sql, 2)
            except Exception as e:
                emsg = "Generaor trips_day_ffx %s query leftqysm is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in trips_day_ffx_date:
                trips_day_ffx_file = os.path.join(trips_day_ffx_folder, sql_date[0] + '.js')
                self._check_file(ckfile=trips_day_ffx_file)
                fw = codecs.open(trips_day_ffx_file, 'w', 'utf-8')
                fw.write('var tripsData_O=[')
                fw.write('\n')
                for i in range(0, len(trips_day_ffx_qysm), 1):
                    for j in range(i + 1, len(trips_day_ffx_qysm), 1):
                        if unit is "big":
                            trips_day_ffx_sql = "select population from trips_day_ffx_big where leftDQSM = '%s' and arrDQSM = '%s' and day = '%s'" % \
                                            (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), sql_date[0])
                        elif unit is "middle":
                            trips_day_ffx_sql = "select population from trips_day_ffx_middle where leftZQSM = '%s' and arrZQSM = '%s' and day = '%s'" % \
                                            (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), sql_date[0])
                        elif unit is "small":
                            trips_day_ffx_sql = "select population from trips_day_ffx_small where leftXQSM = '%s' and arrXQSM = '%s' and day = '%s'" % \
                                            (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), sql_date[0])
                        rlt = self._dbhandler.query(trips_day_ffx_sql, 1)
                        line = '[{"name":"%s"},{"name":"%s","value":%s}]' % (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), rlt)
                        fw.write('\t')
                        fw.write(line)
                        fw.write(',') if i != len(trips_day_ffx_qysm) - 2 else 0
                        fw.write('\n')
                fw.write(']')
                fw.write('\n')
                fw.write('var tripsData_D=[')
                fw.write('\n')
                for i in range(len(trips_day_ffx_qysm) - 1, 0, -1):
                    for j in range(i - 1, -1, -1):
                        if unit is "big":
                            trips_day_ffx_sql = "select population from trips_day_ffx_big where leftDQSM = '%s' and arrDQSM = '%s' and day = '%s'" % \
                                            (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), sql_date[0])
                        elif unit is "middle":
                            trips_day_ffx_sql = "select population from trips_day_ffx_middle where leftZQSM = '%s' and arrZQSM = '%s' and day = '%s'" % \
                                            (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), sql_date[0])
                        elif unit is "small":
                            trips_day_ffx_sql = "select population from trips_day_ffx_small where leftXQSM = '%s' and arrXQSM = '%s' and day = '%s'" % \
                                            (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), sql_date[0])

                        rlt = self._dbhandler.query(trips_day_ffx_sql, 1)
                        line = '[{"name":"%s"},{"name":"%s","value":%s}]' % (trips_day_ffx_qysm[i][0].encode("utf8"), trips_day_ffx_qysm[j][0].encode("utf8"), rlt)
                        fw.write('\t')
                        fw.write(line)
                        fw.write(',') if i != 1 else 0
                        fw.write('\n')
                fw.write(']')
                fw.close()

        msg = "Generaor trips_day_ffx is finish data export"
        log.info(msg)

    def trips_period(self):
        """
        出行分布-时段出行OD的源数据
        :return:
        """
        msg = "Generaor trips_period is run"
        log.info(msg)

        trips_period_period = {'yj': '%夜间%',
                               'cgf': '%早高峰%',
                               'wj': '%晚间%',
                               'wgf': '%晚高峰%',
                               'bt': '%白天%'}
        for unit in UnitType.type:
            trips_period_folder = os.path.abspath(os.path.join(self._data_folder, "trips/trips_period/" + unit))
            self._check_folder(ckfolder=trips_period_folder)
            if unit is "big":
                trips_period_date_sql = "select day from trips_period_big group by day"
            elif unit is "middle":
                trips_period_date_sql = "select day from trips_period_middle group by day"
            elif unit is "small":
                trips_period_date_sql = "select day from trips_period_small group by day"
            try:
                trips_period_date = self._dbhandler.query(trips_period_date_sql, 2)
            except Exception as e:
                emsg = "Generaor trips_period %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in trips_period_date:
                for (sql_trips_period_key, sql_trips_period_value) in trips_period_period.items():
                    trips_period_file = os.path.join(trips_period_folder, sql_trips_period_key + sql_date[0] + '.js')
                    self._check_file(ckfile=trips_period_file)
                    fw = codecs.open(trips_period_file, 'w', 'utf-8')
                    fw.write('var tripsData=[')
                    fw.write('\n')
                    if unit is "big":
                        trips_period_sql = "select leftdqsm, arrdqsm, population from trips_period_big where day = '%s' and period like '%s'" \
                                           % (sql_date[0], sql_trips_period_value)
                    elif unit is "middle":
                        trips_period_sql = "select leftzqsm, arrzqsm, population from trips_period_middle where day = '%s' and period like '%s'" \
                                           % (sql_date[0], sql_trips_period_value)
                    elif unit is "small":
                        trips_period_sql = "select leftxqsm, arrxqsm, population from trips_period_small where day = '%s' and period like '%s'" \
                                           % (sql_date[0], sql_trips_period_value)
                    try:
                        rows = self._dbhandler.query(trips_period_sql, 2)
                    except Exception as e:
                        emsg = "Generaor trips_period %s query data is error: %s" % (unit, e.message)
                        log.error(emsg)
                        continue
                    row_num = len(rows)
                    n = 0
                    for row in rows:
                        leftname = row[0]
                        arrname = row[1]
                        num = int(row[2])
                        line = '[{"name":"%s"},{"name":"%s","value":%d}]' % (leftname, arrname, num)
                        fw.write('\t')
                        fw.write(line)
                        fw.write(',') if n < row_num - 1 else 0
                        fw.write('\n')
                        n += 1
                    fw.write(']')
                    fw.close()

        msg = "Generaor trips_period is finish data export"
        log.info(msg)

    def rw_work_total(self):
        """
        ͨ通勤出行-工作人口总量住地分布
        """
        msg = "Generaor rw_work_total is run"
        log.info(msg)

        for unit in UnitType.type:
            rw_work_total_folder = os.path.abspath(os.path.join(self._data_folder, "work/workRW/" + unit))
            self._check_folder(ckfolder=rw_work_total_folder)
            rw_work_total_file = os.path.abspath(os.path.join(rw_work_total_folder, "workRWtotal.js"))
            self._check_file(ckfile=rw_work_total_file)

            fw = codecs.open(rw_work_total_file, 'w', 'utf-8')
            fw.write('var workRWData={')
            fw.write('\r\n')
            fw.write('\t')
            fw.write("'Data':[")
            fw.write("\n")
            if unit is 'big':
                rw_work_total_sql = "select dqsm, worker from workerRW_big"
            elif unit is 'middle':
                rw_work_total_sql = "select zqsm, worker from workerRW_middle"
            elif unit is 'small':
                rw_work_total_sql = "select xqsm, worker from workerRW_small"
            try:
                rows = self._dbhandler.query(rw_work_total_sql, 2)
            except Exception as e:
                emsg = "Generaor rw_work_total %s query data is error: %s" % (unit, e.message)
                log.error(emsg)
                continue
            row_num = len(rows)
            n = 1
            for row in rows:
                name = str(row[0])
                num = int(row[1])
                fw.write('\t')
                line = '{"name":"%s","value":%d}' % (name, num)
                fw.write(line)
                fw.write(',') if n < row_num else fw.write(']')
                n += 1
                fw.write('\r\n')
            fw.write('}')
            fw.close()

        msg = "Generaor rw_work_total is finish data export"
        log.info(msg)

    def rw_work_density(self):
        """
        ͨ通勤出行-工作人口工密度住地分布
        :return:
        """
        msg = "Generaor rw_work_density is run"
        log.info(msg)

        for unit in UnitType.type:
            rw_work_density_folder = os.path.abspath(os.path.join(self._data_folder, "work/workRWdensity/" + unit))
            self._check_folder(ckfolder=rw_work_density_folder)
            rw_work_density_file = os.path.abspath(os.path.join(rw_work_density_folder, "workRWdensity.js"))
            self._check_file(ckfile=rw_work_density_file)

            fw = codecs.open(rw_work_density_file, 'w', 'utf-8')
            fw.write('var workRWData={')
            fw.write('\r\n')
            fw.write('\t')
            fw.write("'Data':[")
            fw.write("\n")
            if unit is 'big':
                rw_work_density_sql = "select dqsm, workerdensity from workerRW_big"
            elif unit is 'middle':
                rw_work_density_sql = "select zqsm, workerdensity from workerRW_middle"
            elif unit is 'small':
                rw_work_density_sql = "select xqsm, workerdensity from workerRW_small"
            try:
                rows = self._dbhandler.query(rw_work_density_sql, 2)
            except Exception as e:
                emsg = "Generaor rw_work_density %s query data is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            row_num = len(rows)
            n = 1
            for row in rows:
                name = str(row[0])
                num = int(row[1])
                fw.write('\t')
                line = '{"name":"%s","value":%d}' % (name, num)
                fw.write(line)
                fw.write(',') if n < row_num else fw.write(']')
                n += 1
                fw.write('\r\n')
            fw.write('}')
            fw.close()

        msg = "Generaor rw_work_density is finish data export"
        log.info(msg)

    def rw_live_total(self):
        """
        ͨ通勤出行-居住人口总量住地分布
        :return:
        """
        msg = "Generaor rw_live_total is run"
        log.info(msg)

        for unit in UnitType.type:
            rw_live_total_folder = os.path.abspath(os.path.join(self._data_folder, "work/liveRW/" + unit))
            self._check_folder(ckfolder=rw_live_total_folder)
            rw_live_total_file = os.path.abspath(os.path.join(rw_live_total_folder, "livekRWtotal.js"))
            self._check_file(ckfile=rw_live_total_file)

            fw = codecs.open(rw_live_total_file, 'w', 'utf-8')
            fw.write('var workRWData={')
            fw.write('\r\n')
            fw.write('\t')
            fw.write("'Data':[")
            fw.write("\n")
            if unit is 'big':
                rw_live_total_sql = "select dqsm, liver from workerRW_big"
            elif unit is 'middle':
                rw_live_total_sql = "select zqsm, liver from workerRW_middle"
            elif unit is 'small':
                rw_live_total_sql = "select xqsm, liver from workerRW_small"
            try:
                rows = self._dbhandler.query(rw_live_total_sql, 2)
            except Exception as e:
                emsg = "Generaor rw_live_total %s query data is error: %s" % (unit, e.message)
                log.error(emsg)
                continue
            row_num = len(rows)
            n = 1
            for row in rows:
                name = str(row[0])
                num = int(row[1])
                fw.write('\t')
                line = '{"name":"%s","value":%d}' % (name, num)
                fw.write(line)
                fw.write(',') if n < row_num else fw.write(']')
                n += 1
                fw.write('\r\n')
            fw.write('}')
            fw.close()

        msg = "Generaor rw_live_total is finish data export"
        log.info(msg)

    def rw_live_density(self):
        """
        ͨ通勤出行-居住人口工密度住地分布
        :return:
        """
        msg = "Generaor rw_live_density is run"
        log.info(msg)

        for unit in UnitType.type:
            rw_live_density_folder = os.path.abspath(os.path.join(self._data_folder, "work/liveRWdensity/" + unit))
            self._check_folder(ckfolder=rw_live_density_folder)
            rw_live_density_file = os.path.abspath(os.path.join(rw_live_density_folder, "liveRWdensity.js"))
            self._check_file(ckfile=rw_live_density_file)

            fw = codecs.open(rw_live_density_file, 'w', 'utf-8')
            fw.write('var workRWData={')
            fw.write('\r\n')
            fw.write('\t')
            fw.write("'Data':[")
            fw.write("\n")
            if unit is 'big':
                rw_live_density_sql = "select dqsm, liverdensity from workerRW_big"
            elif unit is 'middle':
                rw_live_density_sql = "select zqsm, liverdensity from workerRW_middle"
            elif unit is 'small':
                rw_live_density_sql = "select xqsm, liverdensity from workerRW_small"
            try:
                rows = self._dbhandler.query(rw_live_density_sql, 2)
            except Exception as e:
                emsg = "Generaor rw_live_density %s query data is error: %s" % (unit, e.message)
                log.error(emsg)
                continue
            row_num = len(rows)
            n = 1
            for row in rows:
                name = str(row[0])
                num = int(row[1])
                fw.write('\t')
                line = '{"name":"%s","value":%d}' % (name, num)
                fw.write(line)
                fw.write(',') if n < row_num else fw.write(']')
                n += 1
                fw.write('\r\n')
            fw.write('}')
            fw.close()

        msg = "Generaor rw_live_density is finish data export"
        log.info(msg)

    def rw_noworker_total(self):
        """
        ͨ通勤出行-非工作人口数量分布
        :return:
        """
        msg = "Generaor rw_noworker_total is run"
        log.info(msg)

        for unit in UnitType.type:
            rw_noworker_total_folder = os.path.abspath(os.path.join(self._data_folder, "work/noworker/" + unit))
            self._check_folder(ckfolder=rw_noworker_total_folder)
            rw_noworker_total_file = os.path.abspath(os.path.join(rw_noworker_total_folder, "noworkerR2.js"))
            self._check_file(ckfile=rw_noworker_total_file)

            fw = codecs.open(rw_noworker_total_file, 'w', 'utf-8')
            fw.write('var workRWData={')
            fw.write('\r\n')
            fw.write('\t')
            fw.write("'Data':[")
            fw.write("\n")
            if unit is 'big':
                rw_noworker_total_sql = "select dqsm, noworker from workerRW_big"
            elif unit is 'middle':
                rw_noworker_total_sql = "select zqsm, noworker from workerRW_middle"
            elif unit is 'small':
                rw_noworker_total_sql = "select xqsm, noworker from workerRW_small"
            try:
                rows = self._dbhandler.query(rw_noworker_total_sql, 2)
            except Exception as e:
                emsg = "Generaor rw_noworker_total %s query data is error: %s" % (unit, e.message)
                log.error(emsg)
                continue
            row_num = len(rows)
            n = 1
            for row in rows:
                name = str(row[0])
                num = int(row[1])
                fw.write('\t')
                line = '{"name":"%s","value":%d}' % (name, num)
                fw.write(line)
                fw.write(',') if n < row_num else fw.write(']')
                n += 1
                fw.write('\r\n')
            fw.write('}')
            fw.close()

        msg = "Generaor rw_noworker_total is finish data export"
        log.info(msg)

    def rw_noworker_density(self):
        """
        ͨ通勤出行-非工作人口密度分布
        :return:
        """
        msg = "Generaor rw_noworker_density is run"
        log.info(msg)

        for unit in UnitType.type:
            rw_noworker_density_folder = os.path.abspath(os.path.join(self._data_folder, "work/noworker_density/" + unit))
            self._check_folder(ckfolder=rw_noworker_density_folder)
            rw_noworker_density_file = os.path.abspath(os.path.join(rw_noworker_density_folder, "noworkerR2Density.js"))
            self._check_file(ckfile=rw_noworker_density_file)

            fw = codecs.open(rw_noworker_density_file, 'w', 'utf-8')
            fw.write('var workRWData={')
            fw.write('\r\n')
            fw.write('\t')
            fw.write("'Data':[")
            fw.write("\n")
            if unit is 'big':
                rw_noworker_density_sql = "select dqsm, noworkerdensity from workerRW_big"
            elif unit is 'middle':
                rw_noworker_density_sql = "select zqsm, noworkerdensity from workerRW_middle"
            elif unit is 'small':
                rw_noworker_density_sql = "select xqsm, noworkerdensity from workerRW_small"

            try:
                rows = self._dbhandler.query(rw_noworker_density_sql, 2)
            except Exception as e:
                emsg = "Generaor rw_noworker_density %s query data is error: %s" % (unit, e.message)
                log.error(emsg)
                continue
            row_num = len(rows)
            n = 1
            for row in rows:
                name = str(row[0])
                num = int(row[1])
                fw.write('\t')
                line = '{"name":"%s","value":%d}' % (name, num)
                fw.write(line)
                fw.write(',') if n < row_num else fw.write(']')
                n += 1
                fw.write('\r\n')
            fw.write('}')
            fw.close()

        msg = "Generaor rw_noworker_density is finish data export"
        log.info(msg)

    def rw_rw_value(self):
        """
        ͨ通勤出行-工作与居住比值
        :return:
        """
        msg = "Generaor rw_rw_value is run"
        log.info(msg)

        for unit in UnitType.type:
            rw_rw_value_folder = os.path.abspath(os.path.join(self._data_folder, "work/workRW_rw/" + unit))
            self._check_folder(ckfolder=rw_rw_value_folder)
            rw_rw_value_file = os.path.abspath(os.path.join(rw_rw_value_folder, "workRW.js"))
            self._check_file(ckfile=rw_rw_value_file)

            fw = codecs.open(rw_rw_value_file, 'w', 'utf-8')
            fw.write('var workRWData={')
            fw.write('\r\n')
            fw.write('\t')
            fw.write("'Data':[")
            fw.write("\n")
            if unit is 'big':
                rw_rw_value_sql = "select dqsm, wr from workerRW_big"
            elif unit is 'middle':
                rw_rw_value_sql = "select zqsm, wr from workerRW_middle"
            elif unit is 'small':
                rw_rw_value_sql = "select xqsm, wr from workerRW_small"
            try:
                rows = self._dbhandler.query(rw_rw_value_sql, 2)
            except Exception as e:
                emsg = "Generaor rw_rw_value %s query data is error: %s" % (unit, e.message)
                log.error(emsg)
                continue
            row_num = len(rows)
            n = 1
            for row in rows:
                name = str(row[0])
                num = row[1]
                fw.write('\t')
                line = '{"name":"%s","value":%s}' % (name, num)
                fw.write(line)
                fw.write(',') if n < row_num else fw.write(']')
                n += 1
                fw.write('\r\n')
            fw.write('}')
            fw.close()

        msg = "Generaor rw_rw_value is finish data export"
        log.info(msg)

    def outlander_total(self):
        """
        外来人口-外来人口分布(总量)
        :return:
        """
        msg = "Generaor outlander_total is run"
        log.info(msg)

        for unit in UnitType.type:
            outlander_total_folder = os.path.abspath(os.path.join(self._data_folder, "outlander/outlander_total/" + unit))
            self._check_folder(ckfolder=outlander_total_folder)
            if unit is "big":
                outlander_total_date_sql = "select month from outlander_big group by month"
            elif unit is "middle":
                outlander_total_date_sql = "select month from outlander_middle group by month"
            elif unit is "small":
                outlander_total_date_sql = "select month from outlander_small group by month"
            try:
                outlander_total_date = self._dbhandler.query(outlander_total_date_sql, 2)
            except Exception as e:
                emsg = "Generaor outlander_total %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in outlander_total_date:
                sql_date_new = sql_date[0][:4] + "-" + sql_date[0][4:]
                outlander_total_file = os.path.join(outlander_total_folder, sql_date_new + 'total.js')
                self._check_file(ckfile=outlander_total_file)
                fw = codecs.open(outlander_total_file, 'w', 'utf-8')
                fw.write('var outlanderData={')
                fw.write('\r\n')
                fw.write('\t')
                fw.write("'Data':[")
                fw.write("\n")
                if unit is "big":
                    outlander_total_sql = "select dqsm, outlander from outlander_big where month = '%s'" % sql_date[0]
                elif unit is "middle":
                    outlander_total_sql = "select zqsm, outlander from outlander_middle where month = '%s'" % sql_date[0]
                elif unit is "small":
                    outlander_total_sql = "select xqsm, outlander from outlander_small where month = '%s'" % sql_date[0]
                try:
                    rows = self._dbhandler.query(outlander_total_sql, 2)
                except Exception as e:
                    emsg = "Generaor outlander_total %s query data is error: %s" % (unit, e.message)
                    log.error(emsg)
                    continue
                row_num = len(rows)
                n = 1
                for row in rows:
                    name = row[0]
                    num = int(row[1])
                    fw.write('\t')
                    line = '{"name":"%s","value":%s}' % (name, num)
                    fw.write(line)
                    fw.write(',') if n < row_num else fw.write(']')
                    n += 1
                    fw.write('\r\n')
                fw.write("}")
                fw.close()

        msg = "Generaor outlander_total is finish data export"
        log.info(msg)

    def outlander_density(self):
        """
        外来人口-外来人口分布(密度)
        :return:
        """
        msg = "Generaor outlander_density is run"
        log.info(msg)

        for unit in UnitType.type:
            outlander_density_folder = os.path.abspath(os.path.join(self._data_folder, "outlander/outlander_density/" + unit))
            self._check_folder(ckfolder=outlander_density_folder)
            if unit is "big":
                outlander_density_date_sql = "select month from outlander_big group by month"
            elif unit is "middle":
                outlander_density_date_sql = "select month from outlander_middle group by month"
            elif unit is "small":
                outlander_density_date_sql = "select month from outlander_small group by month"
            try:
                outlander_density_date = self._dbhandler.query(outlander_density_date_sql, 2)
            except Exception as e:
                emsg = "Generaor outlander_density %s query date is error: %s" % (unit, e.message)
                log.error(emsg)
                continue

            for sql_date in outlander_density_date:
                sql_date_new = sql_date[0][:4] + "-" + sql_date[0][4:]
                outlander_density_file = os.path.join(outlander_density_folder, sql_date_new + 'density.js')
                self._check_file(ckfile=outlander_density_file)
                fw = codecs.open(outlander_density_file, 'w', 'utf-8')
                fw.write('var outlanderData={')
                fw.write('\r\n')
                fw.write('\t')
                fw.write("'Data':[")
                fw.write("\n")
                if unit is "big":
                    outlander_density_sql = "select dqsm, outlanderdensity from outlander_big where month = '%s'" % sql_date[0]
                elif unit is "middle":
                    outlander_density_sql = "select zqsm, outlanderdensity from outlander_middle where month = '%s'" % sql_date[0]
                elif unit is "small":
                    outlander_density_sql = "select xqsm, outlanderdensity from outlander_small where month = '%s'" % sql_date[0]
                try:
                    rows = self._dbhandler.query(outlander_density_sql, 2)
                except Exception as e:
                    emsg = "Generaor outlander_density %s query data is error: %s" % (unit, e.message)
                    log.error(emsg)
                    continue
                row_num = len(rows)
                n = 1
                for row in rows:
                    name = row[0]
                    num = int(row[1])
                    fw.write('\t')
                    line = '{"name":"%s","value":%s}' % (name, num)
                    fw.write(line)
                    fw.write(',') if n < row_num else fw.write(']')
                    n += 1
                    fw.write('\r\n')
                fw.write("}")
                fw.close()

        msg = "Generaor outlander_density is finish data export"
        log.info(msg)

    def outlander_city(self):
        """
        外来人口- 外来人口来源地(city)
        :return:
        """
        msg = "Generaor outlander_city is run"
        log.info(msg)

        outlander_city_folder = os.path.abspath(os.path.join(self._data_folder, "outlander/outlander_city"))
        outlander_city_file = os.path.abspath(os.path.join(outlander_city_folder, "outlandershandong.js"))
        self._check_folder(outlander_city_folder)
        self._check_file(outlander_city_file)

        fw = codecs.open(outlander_city_file, 'w', 'utf-8')
        fw.write('var outlanderData={')
        fw.write("\n\r")

        outlander_city_sql = "select city,sum(outlander) from province_outlander_city  group by city"
        try:
            rows = self._dbhandler.query(outlander_city_sql, 2)
        except Exception as e:
            emsg = "Generaor outlander_city is error: %s" % e.message
            log.error(emsg)
        row_num = len(rows)
        n = 1
        for row in rows:
            name = row[0]
            num = int(row[1])
            fw.write('\t')
            line = '{"name":"%s","value":%s}' % (name, num)
            fw.write(line)
            fw.write(',') if n < row_num else 0
            fw.write("\n")
            n += 1
        fw.write('\r\n')
        fw.write("]")
        fw.close()

        msg = "Generaor outlander_city is finish data export"
        log.info(msg)

    def outlander_china(self):
        """
        外来人口- 外来人口来源地(china)
        :return:
        """
        msg = "Generaor outlander_china is run"
        log.info(msg)

        outlander_china_folder = os.path.abspath(os.path.join(self._data_folder, "outlander/outlander_region"))
        outlander_china_file = os.path.abspath(os.path.join(outlander_china_folder, "outlanderChina.js"))
        self._check_folder(outlander_china_folder)
        self._check_file(outlander_china_file)

        fw = codecs.open(outlander_china_file, 'w', 'utf-8')
        fw.write('var outlanderData={')
        fw.write("\n\t")

        outlander_china_sql = "select region,sum(outlander) from province_outlander_region  group by region"
        try:
            rows = self._dbhandler.query(outlander_china_sql, 2)
        except Exception as e:
            emsg = "Generaor outlander_china is error: %s" % e.message
            log.error(emsg)
            pass
        row_num = len(rows)
        n = 1
        for row in rows:
            name = row[0]
            num = int(row[1])
            fw.write('\t')
            line = '{"name":"%s","value":%s}' % (name, num)
            fw.write(line)
            fw.write(',') if n < row_num else 0
            fw.write("\n")
            n += 1
        fw.write('\r\n')
        fw.write("]")
        fw.close()

        msg = "Generaor outlander_china is finish data export"
        log.info(msg)

    def road_worker(self):
        """
        轨道交通-1公里范围岗位数量
        :return:
        """
        msg = "Generaor road_worker is run"
        log.info(msg)

        road_worker_folder = os.path.abspath(os.path.join(self._data_folder, "road"))
        road_worker_file = os.path.abspath(os.path.join(road_worker_folder, "roadWorker.js"))
        self._check_folder(road_worker_folder)
        self._check_file(road_worker_file)

        fw = codecs.open(road_worker_file, 'w', 'utf-8')
        fw.write('var workerData=[')
        fw.write('\r\n')

        road_worker_sql = "select xqsm, worker from trainlinetotal"
        try:
            rows = self._dbhandler.query(road_worker_sql, 2)
        except Exception as e:
            emsg = "Generaor road_worker is error: %s" % e.message
            log.error(emsg)
            pass
        row_num = len(rows)
        n = 1
        for row in rows:
            name = row[0]
            num = int(row[1])
            fw.write('\t')
            line = '{"name":"%s","value":%s}' % (name, num)
            fw.write(line)
            fw.write(',') if n < row_num else 0
            fw.write("\n")
            n += 1
        fw.write('\r\n')
        fw.write("]")
        fw.close()

        msg = "Generaor road_worker is finish data export"
        log.info(msg)

    def road_population(self):
        """
        轨道交通-1公里范围人口数量
        :return:
        """
        msg = "Generaor road_population is run"
        log.info(msg)

        road_population_folder = os.path.abspath(os.path.join(self._data_folder, "road"))
        road_population_file = os.path.abspath(os.path.join(road_population_folder, "roadPopulation.js"))
        self._check_folder(road_population_folder)
        self._check_file(road_population_file)

        fw = codecs.open(road_population_file, 'w', 'utf-8')
        fw.write('var populationData=[')
        fw.write('\r\n')

        road_worker_sql = "select xqsm, population from trainlinetotal"
        try:
            rows = self._dbhandler.query(road_worker_sql, 2)
        except Exception as e:
            emsg = "Generaor road_population is error: %s" % e.message
            log.error(emsg)

        row_num = len(rows)
        n = 1
        for row in rows:
            name = row[0]
            num = int(row[1])
            fw.write('\t')
            line = '{"name":"%s","value":%s}' % (name, num)
            fw.write(line)
            fw.write(',') if n < row_num else 0
            fw.write("\n")
            n += 1
        fw.write('\r\n')
        fw.write("]")
        fw.close()

        msg = "Generaor road_population is finish data export"
        log.info(msg)

    def road_attract(self):
        """
        轨道交通-1公里范围吸引范围
        :return:
        """
        msg = "Generaor road_attract is run"
        log.info(msg)

        road_attract_folder = os.path.abspath(os.path.join(self._data_folder, "road"))
        road_attract_file = os.path.abspath(os.path.join(road_attract_folder, "roadAttract.js"))
        self._check_folder(road_attract_folder)
        self._check_file(road_attract_file)

        fw = codecs.open(road_attract_file, 'w', 'utf-8')
        fw.write('var attractData=[')
        fw.write('\r\n')

        road_worker_sql = "select xqsm, attract from trainlinetotal"
        try:
            rows = self._dbhandler.query(road_worker_sql, 2)
        except Exception as e:
            emsg = "Generaor road_attract is error: %s" % e.message
            log.error(emsg)
            pass
        row_num = len(rows)
        n = 1
        for row in rows:
            name = row[0]
            num = int(row[1])
            fw.write('\t')
            line = '{"name":"%s","value":%s}' % (name, num)
            fw.write(line)
            fw.write(',') if n < row_num else 0
            fw.write("\n")
            n += 1
        fw.write('\r\n')
        fw.write("]")
        fw.close()

        msg = "Generaor road_attract is finish data export"
        log.info(msg)

    def area(self):
        """
        区域面积
        :return:
        """
        msg = "Generaor area is run"
        log.info(msg)

        for unit in UnitType.type:
            area_folder = os.path.abspath(os.path.join(self._data_folder, "area/" + unit))
            area_file = os.path.abspath(os.path.join(area_folder, unit + ".js"))
            self._check_folder(area_folder)
            self._check_file(area_file)
            fw = codecs.open(area_file, 'w', 'utf-8')
            fw.write('var areaNum=[')
            if unit == 'small':
                sql = "select xqsm, xqmj from unit GROUP BY xqsm, xqmj"
            elif unit == 'middle':
                sql = "select zqsm, zqmj from unit GROUP BY zqsm, zqmj"
            elif unit == 'big':
                sql = "select dqsm, dqmj from unit GROUP BY dqsm, dqmj"
            rows = self._dbhandler.query(sql, 2)
            rows_num = len(rows)
            n = 1
            for row in rows:
                name = row[0]
                mj = float(row[1]) / 1000000
                line = '{"name":"%s","value":%f}' % (name, mj)
                fw.write("\n")
                fw.write("\t")
                fw.write(line)
                fw.write(',') if n < rows_num else 0
                n += 1
            fw.write("\n")
            fw.write(']')
            fw.close()
        msg = "Generaor area is finish data export"
        log.info(msg)

