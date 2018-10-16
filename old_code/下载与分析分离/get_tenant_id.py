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
# import objgraph

queue = Queue()
downloader = Downloader(queue)

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
    conn, cur = connect_mysql()
    cur.execute('set names utf8')
    try:
        cur.execute("""
            DROP TABLE IF EXISTS `meituan_tenantid`;
            CREATE TABLE `meituan_tenantid`
            (id BIGINT PRIMARY KEY AUTO_INCREMENT, tenant_id VARCHAR(30),
            mt_poi_id VARCHAR(30),name VARCHAR(200),UNIQUE(mt_poi_id), INDEX (tenant_id), INDEX (mt_poi_id))
            """)
    except Exception as e:
        logger.info('表已经存在', e)
    cur.close()
    conn.close()


def save_to_mysql(items):
    conn, cur = connect_mysql()
    try:
        cur.execute("set names utf8")
        cur.executemany("INSERT INTO meituan_tenantid(tenant_id, mt_poi_id, name) "
                        "VALUES (%s,%s,%s)", items)
        cur.connection.commit()
    except Exception as e:
        logger.error('保存到mysql出错', e)
    cur.close()
    conn.close()


# **********************************download_page
def crawler():
    """
    异步爬取店铺ID页，并保存爬取到的json文件至本地。
    :return:
    """
    put_url_latlng()

    def start_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
    main_loop = asyncio.get_event_loop()
    thread_loop = threading.Thread(target=start_loop, args=(main_loop,))
    thread_loop.setDaemon(True)  # 设置为守护线程
    thread_loop.start()
    while not queue.is_empty():
        asyncio.run_coroutine_threadsafe(spider(), loop=main_loop)
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
    if dir_name not in (os.listdir(os.curdir)):
        os.mkdir(dir_name)
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
    global done_count
    url_item = queue.get()
    if url_item:
        r, text = await downloader.download(url_item)
        # logger.debug("header:%s\nraw_header:%s\ncookies:%s" % (r.headers, r.raw_headers, r.cookies))
        if not r:
            return
        file_name = url_item['cookies']['w_latlng'] + '_' + str(url_item['post_data']['page_index']) + ".json"
        with open(dir_name + '\\' + file_name, 'wb') as f:
            f.write(text)
        try:
            jsn = json.loads(text)
            if jsn['code'] == 801:
                logger.warning('访问太频繁，请调整爬取延时。')
                # time.sleep(8)
                downloader.retry(url_item)
            elif jsn['data']['poi_has_next_page']:
                url_item['post_data']['page_index'] += 1
                queue.put(copy.deepcopy(url_item))
        except Exception as e:
            logger.error('json加载失败重试，原因：%s' % e)
            downloader.retry(url_item)
    done_count += 1
    print('已经下载完成%s个页面' % done_count)


def get_lnglat():
    """
    Get longtitude and latitude from mysqlDB
    :return:
    """
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

    # sql += 'LIMIT 1'

    cur.execute(sql)
    lnglats = cur.fetchall()
    conn.close()
    cur.close()
    return lnglats


# ********************************process_json_file
def process_jsn():
    """
    Process json file and save data to mysqlDB after crawler
    :return:
    """
    create_table()
    os.chdir(os.curdir + '\\' + dir_name + '\\')
    jsn_files = os.listdir(os.curdir)
    count = 0
    items = set()
    dupefilter = set()
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
            if tenant['mt_poi_id'] not in dupefilter:
                items.add(item)
                dupefilter.add(tenant['mt_poi_id'])
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


if __name__ == '__main__':
    start_time = time.time()

    crawler()

    # process_jsn()

    logger.info('程序耗时： %f分' % ((time.time() - start_time) / 60))

    time.sleep(3600)
