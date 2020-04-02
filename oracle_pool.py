import configparser
import os
#!usr/bin/python
#-*-coding:UTF-8 -*-
import datetime
import cx_Oracle
from DBUtils.PooledDB import PooledDB


class Config(object):
    """
    # Config().get_content("user_information")

    配置文件里面的参数
    [Oracle]
    host = 192.168.1.101
    port = 3306
    user = root
    password = python123
    service_name = xxxxxx
    """

    def __init__(self, config_filename="oracle.ini"):
        file_path = os.path.join(os.path.dirname(__file__), config_filename)
        self.cf = configparser.ConfigParser()
        self.cf.read(file_path)

    def get_sections(self):
        return self.cf.sections()

    def get_options(self, section):
        return self.cf.options(section)

    def get_content(self, section):
        result = {}
        for option in self.get_options(section):
            value = self.cf.get(section, option)
            result[option] = int(value) if value.isdigit() else value
        return result


class BasePool(object):
    def __init__(self, host, port, user, password, sid=None, service_name=None):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = str(password)
        self.sid = sid
        self.service_name = service_name
        self.conn = None
        self.cursor = None
        os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.UTF8"


class OraclePool(BasePool):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：
    conn = Oracle.get_conn()
    释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self, conf_name=None):
        self.conf = Config().get_content(conf_name)
        super(OraclePool, self).__init__(**self.conf)
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = self.__get_conn()
        self._cursor = self._conn.cursor()

    def __del__(self):
        """
        @summary: 释放连接池资源
        """
        self._cursor.close()
        self._conn.close()

    def __get_conn(self):
        """
        @summary: 从连接池中取出连接
        @return Oraceldb.connection
        """
        if OraclePool.__pool is None:
            dsn = None
            if self.sid:
                dsn = cx_Oracle.makedsn(self.host, self.port, sid=self.sid)
            elif self.service_name:
                dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
            __pool = PooledDB(creator=cx_Oracle,
                              mincached=6,
                              maxcached=20,
                              user=self.user,
                              password=self.password,
                              dsn=dsn)
            return __pool.connection()

    @staticmethod
    def conn(conf_name):
        """
        @summary: 原生连接
        @return Oracledb.conn
        """
        conf = Config().get_content(conf_name)
        user = conf.get('user')
        password = conf.get('password')
        host = conf.get('host')
        port = conf.get('port')
        service_name = conf.get('service_name')
        conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(user, password, host, port, service_name))
        return conn

    def get_all(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        if param is None:
            self._cursor.execute(sql)
            result = self._cursor.fetchall()
            return result
        else:
            self._cursor.execute(sql, param)
            result = self._cursor.fetchall()
            return result

    def get_one(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count:
            result = self._cursor.fetchone()
        else:
            result = False
        return result

    def get_many(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        return result

    def insert_many(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self._cursor.executemany(sql, values)
        return count

    def __query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
            self._conn.commit()
        else:
            count = self._cursor.execute(sql, param)
            self._conn.commit()
        return count

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def insert(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)
