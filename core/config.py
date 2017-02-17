# -*- coding: utf-8 -*-

"""
------------------------------------------------
describe:
    get config.yaml file informations about
    database host, database port, database user, database password, database default,
    data folder
    run data type
    it need run this process to modify config file information only

demo:

    database informations:
        host, port, user, password, database = get_mssdb_config()
    data folder informations:
        gps_folder = get_source_folder_config()
------------------------------------------------
"""
import inspect
import os
import sys

import yaml

__version__ = "v.10"
__author__ = "PyGo"
__time__ = "2016/12/9"
__method__ = ["get_run_data_config", "get_mssdb_config", "get_data_folder_config"]


# get current folder, solve is or not frozen of the script
def _get_cur_folder():
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(__file__))
    else:
        cur_folder = os.path.dirname(inspect.getfile(inspect.currentframe()))
        return os.path.abspath(cur_folder)


_cur_folder = _get_cur_folder()
_config_folder = os.path.abspath(os.path.join(_get_cur_folder(), "../config"))
_config_file = os.path.abspath(os.path.join(_config_folder, "config.yaml"))

_config_info = yaml.load(file(_config_file))


def get_mssdb_config():
    """
    get database config informations
    :return: database host, database port, database user,
    database password, database default
    """
    host = _config_info["Database"]["host"]
    port = _config_info["Database"]["port"]
    user = _config_info["Database"]["user"]
    password = _config_info["Database"]["password"]
    database = _config_info["Database"]["database"]

    if isinstance(port, basestring):
        port = int(port)
    if isinstance(password, int):
        password = str(password)
    return host, port, user, password, database


def get_source_folder_config():
    """
    get js data stored folder
    :return: data folder
    """
    return _config_info["JSDataFolder"]


def get_target_folder_config():
    """
    get js data stored folder
    :return: data folder
    """
    return _config_info["TXTDataFolder"]


def get_is_update_txt():
    """
    get is update js file to txt file
    :return: True or False
    """
    val = _config_info["IsUpdateTxt"]
    return val


def get_run_data_config():
    """
    get run data
    :return: run data list
    """
    data = _config_info["RunData"]
    if isinstance(data, int) or isinstance(data, float):
        data = str(_config_info["RunData"])
    run_data = []
    if data == "0":
        run_data = ['1',
                    '2.1', '2.2', '2.3', '2.4', '2.5',
                    '3.1', '3.2', '3.3',
                    '4.1', '4.2', '4.3', '4.4', '4.5', '4.6', '4.7',
                    '5.1', '5.2', '5.3', '5.4',
                    '6.1', '6.2', '6.3',
                    '7']
    datas = data.split(",")
    if len(datas) > 1:
        for dt in datas:
            if dt:
                run_data.append(dt.strip())
        return run_data
    else:
        run_data.append(data)
        return run_data
