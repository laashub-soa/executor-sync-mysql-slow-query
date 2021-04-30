import datetime
import logging
import time

from __init__ import init
from component import mymysql, my_async, request_dingding_webhook
from config import app_conf

init()
target_db_pool = mymysql.init(app_conf["mysql"]["target"])
self_db_pool = mymysql.init(app_conf["mysql"]["self"])
logger = logging.getLogger('sync_polardb_slow_log')
logger.setLevel(logging.DEBUG)

local_cache_sql_template_id_2_text = {}
grafana_base_url = app_conf["grafana"]["base_url"]


def post_alarm(msg_content, at_mobiles):
    dingding_webhook_access_token = app_conf["dingding_webhook_access_token"][0]
    dingding_resp = request_dingding_webhook.request_dingding_webhook(dingding_webhook_access_token, "慢SQL",
                                                                      msg_content,
                                                                      at_mobiles)
    logger.debug(dingding_resp)


def select_sql_template_id_by_text(db_name, sql_template_text):
    # 在本地查询
    global local_cache_sql_template_id_2_text
    if local_cache_sql_template_id_2_text.__contains__(db_name):
        local_cache_sql_template_id_2_text_db_level = local_cache_sql_template_id_2_text[db_name]
        if local_cache_sql_template_id_2_text_db_level.__contains__(sql_template_text):
            return local_cache_sql_template_id_2_text_db_level[sql_template_text]
    # 在数据库中查询
    sql_template_id = mymysql.execute(self_db_pool, """
    SELECT ID FROM polardb_slow_log_template WHERE db_name=%s and convert(sql_text using utf8) = %s
    """, [db_name, sql_template_text])
    sql_template_id = list(sql_template_id)
    if len(sql_template_id) < 1:
        insert_sql_result = mymysql.execute(self_db_pool, """
        INSERT INTO `polardb_slow_log_template`(`db_cluster_id`, `db_name`, `db_node_id`, `sql_text`)
        VALUES (%s, %s, %s, %s)
            """, [["", db_name, "", sql_template_text]])
        logger.debug("insert_sql_result: %s" % insert_sql_result)
        sql_template_id = insert_sql_result[0]
    else:
        sql_template_id = sql_template_id[0]["ID"]

    # 更新本地缓存
    logger.debug("sql_template_id: %s" % sql_template_id)
    if not local_cache_sql_template_id_2_text.__contains__(db_name):
        local_cache_sql_template_id_2_text[db_name] = {}
    local_cache_sql_template_id_2_text[db_name][sql_template_text] = sql_template_id
    # 每天凌晨清理缓存(防止缓存内存溢出)
    if datetime.datetime.now().hour == 1:
        local_cache_sql_template_id_2_text = {}
    return sql_template_id


# 发送告警
@my_async.async_call
def deal_with_to_send_alarm(processlist):
    if len(processlist) <= 0:
        return
    total_count = len(processlist)
    db_count = {}  # {$db: xxx}
    db_sql_id_time = {}  # {db: {sql_id: []}}
    # 这要转换成{db, id, time}
    """
    $total_count
    $db: $db_count [$id: [$time]]
    """
    for processlist_item in processlist:
        db_name = processlist_item["db"]
        process_time = processlist_item["time"]
        sql_content = mymysql.extra_sql_template(processlist_item["info"])
        sql_template = mymysql.extra_sql_template(sql_content)
        sql_id = select_sql_template_id_by_text(db_name, sql_template)
        process_time_str = str(process_time) + "s"
        # db_count
        if not db_count.__contains__(db_name):
            db_count[db_name] = 1
        else:
            db_count[db_name] += 1
        # db_sql_id_time
        if not db_sql_id_time.__contains__(db_name):
            db_sql_id_time[db_name] = {}
        if not db_sql_id_time[db_name].__contains__(sql_id):
            db_sql_id_time[db_name][sql_id] = [process_time_str]
        else:
            db_sql_id_time[db_name][sql_id].append(process_time_str)
    # 生成告警字符串
    alarm_content = "最近一秒中有%s条超慢查询, 各库的详细信息如下:" % total_count
    sorted_db_count = sorted(db_count.items(), key=lambda kv: (kv[0], kv[1]))
    for sorted_db_count_item in sorted_db_count:
        db_name = sorted_db_count_item[0]
        alarm_content += "\n%s: %s次 " % (db_name, db_count[db_name])
        alarm_content += "{"
        for sql_id_key, sql_id_value in db_sql_id_time[db_name].items():
            link_grafana_url = grafana_base_url.format(db_name=db_name, sql_template_id=str(sql_id_key))
            display_db_name__sql_template_id = "[%s](%s)" % (str(sql_id_key), link_grafana_url)
            alarm_content += "%s: [%s]; " % (display_db_name__sql_template_id, ", ".join(sql_id_value))
        alarm_content += "}"
    print(alarm_content)
    post_alarm(alarm_content, [])


def query_slow_query(node_name):
    server_db_user_name = app_conf["server_db_user_name"]
    db_maximum_tolerance_time = app_conf["db_maximum_tolerance_time"]
    processlist = mymysql.execute(target_db_pool, """
/*force_node='%s'*/
select id, db, time, info  from information_schema.processlist where user ='%s' and command != 'Sleep' and time > %s
    """ % (node_name, server_db_user_name, db_maximum_tolerance_time))
    return processlist


if __name__ == '__main__':
    print("setup")
    next_delay_fix = 1
    last_start_query_seconds = time.time()
    polardb_node_name = app_conf["polardb_node_name"]
    while True:
        start_query_seconds = time.time()
        diff_last_start_query_seconds = start_query_seconds - last_start_query_seconds
        if diff_last_start_query_seconds < 1:
            time.sleep(1.0 - diff_last_start_query_seconds)
        try:
            processlist = query_slow_query(polardb_node_name)
            deal_with_to_send_alarm(processlist)
        except Exception as e:
            logger.debug("发生了异常: " + str(e))
        last_start_query_seconds = start_query_seconds
