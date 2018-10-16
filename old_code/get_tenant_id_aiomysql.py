import json
from connect_db import connect_mysql
import config
import os
import threading
import time
from url_queue import Queue
from downloader import Downloader
import copy
import logging
import asyncio
import get_tenant_food
import get_tenant_info
import aiomysql
# import objgraph

queue = Queue()
downloader = Downloader(queue)

conn, cur = connect_mysql()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('get_tenant_id')

post_data = {
    "page_index": 0,
}
headers = {
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544102&lng=113.947104',
}
cookies = {
    'w_latlng': '22555969,113893232'
}

dir_name = 'json_id'
request_delay = 2.5
done_count = 0


# ************************MYSQL


def create_table():
    try:
        cur.execute("""
            DROP TABLE IF EXISTS `meituan_tenantid`;
            CREATE TABLE `meituan_tenantid`
            (id BIGINT PRIMARY KEY AUTO_INCREMENT, tenant_id VARCHAR(30),
            mt_poi_id VARCHAR(30),name VARCHAR(200),UNIQUE(mt_poi_id), INDEX (tenant_id), INDEX (mt_poi_id))
            """)
    except Exception as e:
        logger.info('表已经存在', e)


async def save_to_mysql(items, cur_io):
    print('save_to_mysql')
    print(items)
    await cur_io.execute("INSERT INTO meituan_tenantid(tenant_id, mt_poi_id, name) "
                    "VALUES (%s,%s,%s)", items)
    print('execute')
    await cur_io.connection.commit()
    print('commit')


async def conn_mysql():
    conn_io = await aiomysql.connect(host=config.ip, port=3306, user=config.user, password=config.passwd,
                                  db=config.db, loop=asyncio.get_event_loop())
    cur_io = await conn_io.cursor()
    await cur_io.execute("set names utf8")
    print('conn_mysql end')
    return cur_io


def get_lnglat():
    """
    Get longtitude and latitude from mysqlDB
    :return:
    """
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

    # sql += 'LIMIT 1'

    cur.execute(sql)
    lnglats = cur.fetchall()
    return lnglats


# **********************************download_page
def crawler(loop):
    """
    异步爬取店铺ID页，并保存爬取到的json文件至本地。
    :return:
    """
    logger.debug('crrawler')
    put_url_latlng()

    id_loop = loop
    asyncio.set_event_loop(id_loop)

    while not queue.is_empty():
        while get_tenant_info.queue_info.size() > 20 or get_tenant_food.queue_food.size() > 20:
            # print(get_tenant_info.queue_info.size(), get_tenant_food.queue.size())
            time.sleep(1)
            # print('sleep 1s')
        logger.debug('queue is not empty')
        asyncio.run_coroutine_threadsafe(spider(), loop=id_loop)
        time.sleep(request_delay)
        # objgraph.show_growth()

    logger.info("\n\n==============================END======================\n\n")
    time.sleep(100)


def put_url_latlng():
    """
    Put url_item into queue
    :return:
    """
    url_item = {
        'url': 'http://i.waimai.meituan.com/ajax/v6/poi/filter?lat=22.555366&lng=113.976517',
        'headers': headers,
        'cookies': cookies,
        'post_data': post_data
    }
    lnglats = get_lnglat()
    for lnglat in lnglats:
        lat = "{0:0<8}".format((lnglat[0]).replace('.', ''))[:8]
        lng = "{0:0<9}".format((lnglat[1]).replace('.', ''))[:9]
        url_item['cookies']['w_latlng'] = lat + ',' + lng
        queue.put(copy.deepcopy(url_item))


async def spider():
    """
    Fetch page and save to local path
    :return:
    """
    logger.debug('spider')
    global done_count
    dupefilter = set()
    count_dump = 0

    url_item = queue.get()
    if url_item:
        r, text = await downloader.download(url_item)
        # logger.debug("header:%s\nraw_header:%s\ncookies:%s" % (r.headers, r.raw_headers, r.cookies))
        if not r:
            return
        try:
            jsn = json.loads(text)
        except Exception as e:
            logger.error('json加载失败重试，原因：%s' % e)
            downloader.retry(url_item)
            return False
        if jsn['code'] == 801:
            logger.warning('访问太频繁，请调整爬取延时。')
            # time.sleep(8)
            downloader.retry(url_item)
        elif jsn['data']['poi_has_next_page']:
            url_item['post_data']['page_index'] += 1
            queue.put(copy.deepcopy(url_item))
        tenants = jsn['data']['poilist']
        cur_io = await conn_mysql()
        for tenant in tenants:
            if tenant['mt_poi_id'] not in dupefilter:
                print(1)
                await save_to_mysql([tenant['id'], tenant['mt_poi_id'], tenant['name']], cur_io)
                print(2)
                get_tenant_info.put_url_id(tenant['id'])
                get_tenant_food.put_url_in_queue(tenant['id'])
                dupefilter.add(tenant['mt_poi_id'])
            else:
                count_dump += 1
    done_count += 1
    logger.info('已经下载完成%s个页面' % done_count)


if __name__ == '__main__':
    start_time = time.time()

    main_loop = asyncio.get_event_loop()

    def start_loop(loop):
        loop.run_forever()
    id_thread = threading.Thread(target=start_loop, args=(main_loop,))
    id_thread.setDaemon(True)  # 设置为守护线程
    id_thread.start()
    # thread_loop = threading.Thread(target=start_loop, args=(info_food_loop,))
    # thread_loop.start()

    get_id = threading.Thread(target=crawler, args=(main_loop,))
    get_info = threading.Thread(target=get_tenant_info.crawler, args=(main_loop,))
    get_food = threading.Thread(target=get_tenant_food.crawler, args=(main_loop,))
    get_id.start()
    logger.info('get_id started')
    get_info.start()
    logger.info('get_info started')
    get_food.start()
    logger.info('get_food started')
    get_id.join()
    # crawler()

    logger.info('店铺ID页已爬取完成，等待店铺信息与商品信息爬取完成')
    time.sleep(100)

    logger.info('程序耗时： %f分' % ((time.time() - start_time) / 60))

    cur.close()
    conn.close()

    time.sleep(3600)
