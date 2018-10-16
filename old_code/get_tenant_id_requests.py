import copy
import json
import logging
import os
import threading
import time

import config
from connect_db import connect_mysql
from old_code.downloader_requests import Downloader
from url_queue import Queue

queue = Queue()
downloader = Downloader(queue)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('get_tenant_id')

post_data = {
    "page_index": 0,
}
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544102&lng=113.947104',
}
cookies = {
    'w_latlng': '22555969,113893232'
}

dir_name = 'json'


# ************************MYSQL


def create_table():
    conn, cur = connect_mysql()
    cur.execute('set names utf8')
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meituan_tenant_id
            (id BIGINT PRIMARY KEY AUTO_INCREMENT, tenant_id VARCHAR(30),
            mt_poi_id VARCHAR(30),name VARCHAR(200))
            """)
    except Exception as e:
        logger.info('表已经存在', e)
    cur.close()
    conn.close()


def save_to_mysql(items):
    conn, cur = connect_mysql()
    try:
        cur.execute("set names utf8")
        cur.executemany("INSERT INTO meituan_tenant_id(tenant_id, mt_poi_id, name) VALUES (%s,%s,%s)", items)
        cur.connection.commit()
    except Exception as e:
        logger.error('保存到mysql出错', e)
    cur.close()
    conn.close()


# **********************************download_page
def put_url_latlng():
    url_item = {
        'url': 'http://i.waimai.meituan.com/ajax/v6/poi/filter?lat=22.555366&lng=113.976517',
        'headers': headers,
        'cookies': cookies,
        'post_data': post_data
    }
    lnglats = get_lnglat()
    if dir_name not in (os.listdir(os.curdir)):
        os.mkdir(dir_name)
    for lnglat in lnglats:
        lat = "{0:0<8}".format((lnglat[0]).replace('.', ''))[:8]
        lng = "{0:0<9}".format((lnglat[1]).replace('.', ''))[:9]
        url_item['cookies']['w_latlng'] = lat + ',' + lng
        queue.put(copy.deepcopy(url_item))


def spider():
    while True:
        url_item = queue.get()
        if url_item:
            r = downloader.download(url_item)
            print(r.data)
            if not r:
                continue
            file_name = url_item['cookies']['w_latlng'] + '_' + str(url_item['post_data']['page_index']) + ".json"
            with open(dir_name + '\\' + file_name, 'wb') as f:
                f.write(r.content)
            try:
                jsn = json.loads(r.content)
                if jsn['data']['poi_has_next_page']:
                    url_item['post_data']['page_index'] += 1
                    queue.put(copy.deepcopy(url_item))
            except Exception as e:
                logger.error('json加载失败重试，原因：%s' % e)
                downloader.retry(url_item)
        else:
            break
    return None


def get_lnglat():
    conn, cur = connect_mysql()
    province = config.province
    city = config.city
    region = config.region
    sql = """
              SELECT latitude,longitude
              FROM meituan_validlnglat
              WHERE sign=0
              """
    if province != '':
        sql += " and province like '%" + province + "%'"
    if city != '':
        sql += " and city like '%" + city + "%'"
    if region != '':
        sql += " and region like '%" + region + "%'"
    # sql = """
    #           SELECT latitude,longitude
    #           FROM meituan_tenantinfo
    #           """

    sql += 'LIMIT 10'

    cur.execute(sql)
    lnglats = cur.fetchall()
    conn.close()
    cur.close()
    return lnglats


# ********************************process_json_file
def process_jsn():
    create_table()
    os.chdir(os.curdir + '\\' + dir_name + '\\')
    jsn_files = os.listdir(os.curdir)
    count = 0
    items = set()
    count_dump = 0
    file_num = 1
    file_count = len(jsn_files)
    for jsn_file in jsn_files:
        logger.info('正在处理第%d/%d个文件%s' % (file_num, file_count, jsn_file))
        with open(jsn_file, 'rb') as f:
            jsn_str = f.read()
        try:
            jsn_dic = json.loads(jsn_str)
        except Exception as e:
            logger.error("发生错误：%s" % e)
            continue
        tenants = jsn_dic['data']['poilist']
        for tenant in tenants:
            # print(tenant['id'], tenant['mt_poi_id'], tenant['name'])
            count += 1
            item = (tenant['id'], tenant['mt_poi_id'], tenant['name'])
            # item = tenant['name']
            if item not in items:
                items.add(item)
            else:
                count_dump += 1
        file_num += 1
    logger.info('count: %d\ncount_dump: %d' % (count, count_dump))
    create_table()
    save_to_mysql(items)
    # with open('..\\id_list.txt', 'w') as f:
    #     for i in items:
    #         f.write(str(i) + '\n')
    return None


def format_json(text):
    text = text.replace(',', ',\n')
    text = text.replace('{', '\n{\n')
    text = text.replace('[', '\n[\n')
    text = text.replace('}', '\n}\n')
    text = text.replace(']', '\n]\n')
    text = text.replace('\n\n', '\n')
    return text


if __name__ == '__main__':
    start_time = time.time()

    put_url_latlng()
    threads = []
    for i in range(3):
        threads.append(threading.Thread(target=spider))
    for i in threads:
        i.start()
    for i in threads:
        i.join()

    # process_jsn()

    logging.info('程序耗时： %f分' % ((time.time() - start_time) / 60))
