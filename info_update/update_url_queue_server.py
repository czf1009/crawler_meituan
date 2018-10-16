import pymysql
import pandas as pd
import time
import connect_db
from url_queue import Queue
import threading
import coord_transform
import copy
from downloader import Downloader
import asyncio
import json
from log import Logger

date_now = time.strftime('%Y-%m-%d', time.localtime(time.time()))
log_name = date_now + '_' + 'update_url_queue_server.log'
logger = Logger(logname=log_name, loglevel=1, logger="update_url_queue_server").getlog()

pymysql.install_as_MySQLdb()

queue = Queue()
downloader = Downloader(queue)

global num
global count


def create_table():
    conn, cur = connect_db.connect_mysql()
    cur.execute('set names utf8')
    cur.execute("""
            DROP TABLE IF EXISTS `meituan_tenantinfo`;
            CREATE TABLE `meituan_tenantinfo` (
              `id` BIGINT(20) PRIMARY KEY  AUTO_INCREMENT,
              `business_id` VARCHAR(50) DEFAULT NULL,
              `name` VARCHAR(155) DEFAULT NULL,
              `address` VARCHAR(200) DEFAULT NULL,
              `telephone` VARCHAR(100) DEFAULT NULL,
              `month_saled` INT(50) DEFAULT NULL,
              `shop_announcement` TEXT DEFAULT NULL,
              `latitude` VARCHAR(50) DEFAULT NULL,
              `longitude` VARCHAR(50) DEFAULT NULL,
              `geohash` VARCHAR(20) DEFAULT NULL,
              `avg_rating` VARCHAR(50) DEFAULT NULL,
              `business_url` VARCHAR(255) DEFAULT NULL,
              `photo_url` VARCHAR(255) DEFAULT NULL,
              `float_minimum_order_amount` VARCHAR(50) DEFAULT NULL,
              `float_delivery_fee` VARCHAR(50) DEFAULT NULL,
              `minus` VARCHAR(255) DEFAULT NULL,
              `delivery_consume_time` VARCHAR(100) DEFAULT NULL,
              `work_time` VARCHAR(100) DEFAULT NULL,
              `md5` VARCHAR(100) DEFAULT NULL,
              `mt_poi_id` VARCHAR(50) DEFAULT NULL,
              UNIQUE KEY `mt_poi_id` (`mt_poi_id`),
              INDEX(`business_id`),
              INDEX(`name`)
            ) ENGINE=InnoDB AUTO_INCREMENT=31150 DEFAULT CHARSET=utf8mb4;
            """)
    cur.close()
    conn.close()


def init_url():
    conn, cur = connect_db.connect_mysql()
    cur.execute('set names utf8')
    cur.execute('''UPDATE `meituan_tenantinfo`
                   SET `business_url`=null;''')
    logger.info('初始化url成功')
    conn.commit()
    logger.info('初始化url commit 成功')
    cur.close()
    conn.close()


headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544102&lng=113.947104',
}
url = 'http://i.waimai.meituan.com/search/ajax/v7/poi'


def get_datas():
    conn, cur = connect_db.connect_mysql()

    sql = """
            SELECT business_id, longitude, latitude, name FROM meituan_tenantinfo AS mt
            WHERE mt.business_id IN (SELECT M_business_id FROM test_merge_tenantinfo)
            """
    datas = pd.read_sql(sql, conn, index_col='business_id')

    cur.close()
    conn.close()
    # server.stop()
    return datas


def create_put_url_item(datas):
    for i in datas.index:
        # logger.info(type(datas['longitude']), datas['longitude'])
        lng, lat = coord_transform.bd09_to_gcj02(float(datas.loc[i, 'longitude']), float(datas.loc[i, 'latitude']))
        lng = str(lng).replace('.', '')[:9]
        lat = str(lat).replace('.', '')[:8]
        post_data = {
            "keyword": datas.loc[i, 'name'],
            "page_index": "0",
            "page_size": "100"
        }
        cookies = {
            'w_latlng': lat + ',' + lng
        }
        url_item = copy.deepcopy({'url': url, 'cookies': cookies, 'headers': headers, 'post_data': post_data,
                                  'index': i})
        # r = requests.post(url, headers=headers, data=post_data, cookies=cookies, allow_redirects=False)

        queue.put(url_item)
    return


def start_async_loop():
    def start_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
    main_loop = asyncio.get_event_loop()
    thread_loop = threading.Thread(target=start_loop, args=(main_loop,))
    thread_loop.setDaemon(True)  # 设置为守护线程
    thread_loop.start()
    return


async def update_db(business_id, business_url):
    global cur
    global conn
    await cur.execute('''UPDATE `meituan_tenantinfo`
                   SET `business_url`=%s
                   WHERE `business_id`=%s;''', (business_url, business_id))


async def update_url():
    global num
    global count
    global conn
    global cur
    url_item = queue.get()
    logger.info('url_item:%s' % url_item)
    r, text = await downloader.download(copy.deepcopy(url_item))
    if r.status == 200:
        jsn = json.loads(text)
        search_poi_list = jsn['data']['search_poi_list']
        for tenant in search_poi_list:
            if tenant['name'] == url_item['post_data']['keyword']:
                logger.info('http://i.waimai.meituan.com/restaurant/' + str(tenant['id']))
                new_url = 'http://i.waimai.meituan.com/restaurant/' + str(tenant['id'])
                await update_db(url_item['index'], new_url)
                if num % 10 == 0 or num == count:
                    await conn.commit()
                    logger.info('已完成%s/%s' % (num, count))
                num += 1
                return
        logger.info('搜索不到该店铺:%s' % url_item)
        downloader.retry(url_item)
        return

global conn
global cur
async def get_conn(loop):
    global conn
    global cur
    conn, cur = await connect_db.connect_aiomysql(loop)


def start_update():
    global num
    global count
    main_loop = asyncio.get_event_loop()
    # main_loop.run_until_complete(get_conn(main_loop))
    if main_loop.is_running():
        asyncio.run_coroutine_threadsafe(get_conn(main_loop), loop=main_loop)
    else:
        main_loop.run_until_complete(get_conn(main_loop))
        start_async_loop()
    num = 1
    logger.info('get_datas()')
    datas = get_datas()
    count = len(datas)
    logger.info('create_put_url_item()')
    create_put_url_item(datas)
    logger.info('init_url')
    init_url()

    while not queue.is_empty():
        asyncio.run_coroutine_threadsafe(update_url(), loop=main_loop)
        time.sleep(0.1)

    time.sleep(10)
    cur.close()
    conn.close()


if __name__ == '__main__':
    start = time.time()

    start_update()

    logger.info('耗时%s分' % ((time.time() - start) / 60))
