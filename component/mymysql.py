from contextlib import closing

import pymysql
from DBUtils.PooledDB import PooledDB

from exception import MyServiceException

"""
https://webwareforpython.github.io/DBUtils/main.html
"""


def init(mysql_config):
    mysql_config["cursorclass"] = pymysql.cursors.DictCursor
    return PooledDB(pymysql, **mysql_config)


def execute(db_pool, sql, parameters=None):
    # print("sql: \n %s \n parameters: %s" % (sql, parameters))
    execute_result = None
    if not parameters:
        parameters = {}
    try:
        with closing(db_pool.connection()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(sql, parameters)
                execute_result = cursor.fetchall()
    except Exception:
        import traceback, sys
        traceback.print_exc()  # 打印异常信息
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error = str(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        raise MyServiceException("execute sql error: " + error)
    return execute_result


def extra_sql_template(sql_content):
    """
    抽取SQL模板(指纹)
    :param sql_content: SQL内容
    :return:
    """
    """
    抽取SQL模板的思路: 
    替换
        > < = ! 
            'xxx'                        -> ?
            特殊字符+数字开头的+特殊字符   -> 特殊字符+?+特殊字符  
        in (xxx)                         -> in(?)
    """
    # print("=" * 200)
    # print("sql_content: %s" % sql_content)
    sql_template = ''
    is_quote_start = False  # 是否开始引号
    is_digit_start = False  # 是否开始数字
    last_sql_content_item = ''
    for item in sql_content:
        # 从字符串中抽取引号部分为模板
        if item == '\'':
            is_quote_start = not is_quote_start
            if not is_quote_start:
                sql_template += '?'
            continue
        if is_quote_start:
            continue
        # 从字符串中抽取数字部分为模板
        if item.isdigit():
            if not last_sql_content_item.isdigit() and not last_sql_content_item.isalpha():
                is_digit_start = True
        else:
            if is_digit_start:
                is_digit_start = False
                sql_template += '?'
        if is_digit_start:
            continue
        sql_template += item
        last_sql_content_item = item
    if is_digit_start:
        sql_template += '?'
    # 从字符串中抽取in()部分中多个模板为单个模板
    is_i_start = False
    is_n_start = False
    is_question_start = False
    sql_template_back = ''
    for item in sql_template:
        if item.lower() == 'i':
            is_i_start = True
        else:
            if is_i_start:
                if item.lower() == 'n':
                    is_i_start = False
                    is_n_start = True
                else:
                    is_i_start = False
            else:
                if is_n_start:
                    # 是否应该考虑 hint段? 暂时不考虑
                    if item.isalpha():
                        is_n_start = False
                    elif item == '?':
                        is_n_start = False
                        is_question_start = True
                else:
                    if is_question_start:
                        if item in ['?', ',', ' ']:
                            continue
                        elif item == ")":
                            is_question_start = False
        sql_template_back += item
    sql_template = sql_template_back
    # print("sql_template: %s" % sql_template)

    return sql_template
