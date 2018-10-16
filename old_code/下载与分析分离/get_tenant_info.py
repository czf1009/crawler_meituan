import hashlib
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
from geohash import encode
import re

queue = Queue()
downloader = Downloader(queue)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('get_tenant_info')

info_url = 'http://i.waimai.meituan.com/wxi/ajax/v6/poi/info'
post_data = {}
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544102&lng=113.947104',
}
cookies = {}

dir_name = 'json_info'

done_count = 0


# ************************MYSQL


def create_table():
    conn, cur = connect_mysql()
    cur.execute('set names utf8')
    cur.execute("""
            DROP TABLE IF EXISTS `meituan_tenantinfo`;
            CREATE TABLE `meituan_tenantinfo` (
              `Id` BIGINT(20) PRIMARY KEY  AUTO_INCREMENT,
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


info = {}


def save_to_mysql(items):
    start = time.time()
    conn, cur = connect_mysql()
    try:
        cur.execute("set names utf8mb4")
        # cur.executemany("INSERT INTO meituan_tenantid(tenant_id, mt_poi_id, name) VALUES (%s,%s,%s)", items)
        cur.executemany("""
                        INSERT INTO `new_hudong_db`.`meituan_tenantinfo` (`business_id`, `name`, `address`, 
                        `telephone`, `month_saled`, `shop_announcement`, `latitude`, `longitude`, `geohash`, 
                        `avg_rating`, `business_url`, `photo_url`, `float_minimum_order_amount`, `float_delivery_fee`, 
                        `delivery_consume_time`, `work_time`, `md5`, `mt_poi_id`) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, items)
        # ON DUPLICATE KEY UPDATE
        # `business_id`=%s,
        # `month_saled`=%s,
        # `shop_announcement`=%s,
        # `avg_rating`=%s,
        # `business_url`=%s,
        # `photo_url`=%s,
        # `float_minimum_order_amount`=%s,
        # `float_delivery_fee`=%s,
        # `delivery_consume_time`=%s,
        # `work_time`=%s,
        # `md5`=%s ;
        cur.connection.commit()
    except Exception as e:
        logger.error('保存到mysql出错', e)
    cur.close()
    conn.close()
    print(time.time() - start)


def get_tenant_ids():
    conn, cur = connect_mysql()
    sql = """
              SELECT tenant_id
              FROM meituan_tenantid
              WHERE mt_poi_id != 0
              """
    # sql += 'LIMIT 1'

    cur.execute(sql)
    tenant_ids = cur.fetchall()
    conn.close()
    cur.close()
    return tenant_ids
    # return [["353189028939398"]]


def get_mt_poi_ids():
    conn, cur = connect_mysql()
    sql = """
              SELECT tenant_id, mt_poi_id
              FROM meituan_tenantid
              """
    # sql += 'LIMIT 1'
    cur.execute(sql)
    tenant_ids = cur.fetchall()
    # cur.executemany("UPDATE `new_hudong_db`. `meituan_tenantinfo` SET `mt_poi_id` = %s "
    #                 "WHERE `business_id` = %s and `mt_poi_id` = '';",
    #                 tenant_ids)
    # cur.connection.commit()
    conn.close()
    cur.close()
    id_dic = dict()
    for i, j in tenant_ids:
        id_dic[i] = j
    return id_dic


# **********************************download_page
def crawler():
    """
    异步爬取店铺ID页，并保存爬取到的json文件至本地。
    :return:
    """
    put_url_id()

    def start_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    main_loop = asyncio.get_event_loop()
    thread_loop = threading.Thread(target=start_loop, args=(main_loop,))
    thread_loop.setDaemon(True)  # 设置为守护线程
    thread_loop.start()
    while not queue.is_empty():
        asyncio.run_coroutine_threadsafe(spider(), loop=main_loop)
        time.sleep(0.1)

    logger.info("\n\n==============================END======================\n\n")
    time.sleep(100)


def put_url_id():
    """
    Put url_item into queue
    :return:
    """
    url_item = {
        'url': info_url,
        'headers': headers,
        'cookies': cookies,
        'post_data': post_data
    }
    # lnglats = get_lnglat()
    ids = get_tenant_ids()
    if dir_name not in (os.listdir(os.curdir)):
        os.mkdir(dir_name)
    for tenant_id in ids:
        # lat = "{0:0<8}".format((lnglat[0]).replace('.', ''))[:8]
        # lng = "{0:0<9}".format((lnglat[1]).replace('.', ''))[:9]
        # url_item['cookies']['w_latlng'] = lat + ',' + lng
        url_item['post_data']['wmpoiid'] = tenant_id[0]
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
        file_name = url_item['post_data']['wmpoiid'] + ".json"
        with open(dir_name + '\\' + file_name, 'wb') as f:
            f.write(text)
    done_count += 1
    print('已经下载完成%s个页面' % done_count)


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
    file_num = 1
    file_count = len(jsn_files)
    id_dic = get_mt_poi_ids()
    for jsn_file in jsn_files:
        logger.info('正在处理第%d/%d个文件%s' % (file_num, file_count, jsn_file))
        with open(jsn_file, 'rb') as f:
            jsn_str = f.read()
        try:
            jsn_dic = json.loads(jsn_str)
        except Exception as e:
            logger.error("发生错误：%s" % e)
            continue
        info_jsn = jsn_dic['data']
        if not info_jsn:
            continue
        item = load_item(info_jsn, id_dic)
        items.add(item)

        file_num += 1
    logger.info('count: %d\n' % count)
    create_table()
    save_to_mysql(items)
    return None


def load_item(info_jsn, id_dic):
    item = dict()
    item['business_id'] = info_jsn['id']
    item['name'] = info_jsn['name']
    item['address'] = info_jsn['address']
    item['telephone'] = info_jsn['call_center']
    item['month_saled'] = info_jsn['month_sale_num']
    item['shop_announcement'] = info_jsn['bulletin']
    longitude = str(info_jsn['longitude'])
    latitude = str(info_jsn['latitude'])
    item['latitude'] = latitude[:2] + '.' + latitude[2:]
    item['longitude'] = longitude[:3] + '.' + longitude[3:]
    item['geohash'] = encode(float(item['latitude']), float(item['longitude'])),
    item['geohash'] = item['geohash'][0]
    item['avg_rating'] = info_jsn['wm_poi_score']
    item['business_url'] = 'http://i.waimai.meituan.com/wxi/restaurant/%s' % info_jsn['id']
    item['photo_url'] = info_jsn['pic_url']
    item['float_minimum_order_amount'] = info_jsn['min_price']
    item['float_delivery_fee'] = info_jsn['shipping_fee']

    item['delivery_consume_time'] = info_jsn['avg_delivery_time']
    item['work_time'] = info_jsn['shipping_time']

    md5 = ''
    for k, j in item.items():
        md5 += str(j)
    item['md5'] = hashlib.md5(md5.encode('utf8')).hexdigest()

    item['mt_poi_id'] = id_dic[str(item['business_id'])]

    item = (
        item['business_id'],
        item['name'],
        item['address'],
        item['telephone'],
        item['month_saled'],
        item['shop_announcement'],
        item['latitude'],
        item['longitude'],
        item['geohash'],
        item['avg_rating'],
        item['business_url'],
        item['photo_url'],
        item['float_minimum_order_amount'],
        item['float_delivery_fee'],
        item['delivery_consume_time'],
        item['work_time'],
        item['md5'],
        item['mt_poi_id']
    )
    return item


if __name__ == '__main__':
    start_time = time.time()

    # crawler()

    process_jsn()

    logger.info('程序耗时： %f分' % ((time.time() - start_time) / 60))
    time.sleep(1000)
