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
import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
from multiprocessing.dummy import Pool
import get_tenant_info
import re

global file_num
global jsn_count

queue = Queue()
downloader = Downloader(queue)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('get_tenant_food')

info_url = 'http://i.waimai.meituan.com/wxi/ajax/v8/poi/food'
post_data = {
}
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544102&lng=113.947104',
}
cookies = {
}

dir_name = 'json_food'

done_count = 0


# ************************MYSQL
def create_table():
    conn, cur = connect_mysql()
    cur.execute('set names utf8')
    cur.execute("""
            DROP TABLE IF EXISTS `meituan_goodsinfo`;
            CREATE TABLE `meituan_goodsinfo` (
              `id` bigint(20) NOT NULL AUTO_INCREMENT,
              `food_id` varchar(30) DEFAULT NULL,
              `business_id` varchar(30) DEFAULT NULL,
              `price` varchar(20) DEFAULT NULL,
              `name` varchar(150) DEFAULT NULL,
              `image_path` varchar(150) DEFAULT NULL,
              `month_saled` int(10) DEFAULT '0',
              `description` text DEFAULT NULL,
              `specs_value` text DEFAULT NULL,
              `specs_features` text DEFAULT NULL,
              `md5` varchar(50) DEFAULT NULL,
              `mt_poi_id` varchar(50) DEFAULT NULL,
              PRIMARY KEY (`id`),
              UNIQUE KEY `food_id` (`food_id`),
              INDEX (`business_id`)
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
            
            """)
    cur.close()
    conn.close()


def save_to_mysql(items):
    conn, cur = connect_mysql()
    cur.execute("set names utf8mb4")
    logger.info("正在插入商品的数据，共有%s条" % len(items))
    start = time.time()
    try:
        count = 0
        item_li = []
        for item in items:
            item_li.append(item)
            count += 1
            if count % 10000 == 0:
                cur.executemany("""
                        INSERT INTO `new_hudong_db`.`meituan_goodsinfo` (`food_id`, `business_id`, `price`, 
                        `name`, `image_path`, `month_saled`, `description`, `specs_value`, `specs_features`, `md5`, 
                        `mt_poi_id`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, item_li)
                conn.commit()
                print('已插入%s/%s条数据, 本次插入耗时%sS' % (count, len(items), time.time()-start))
                start = time.time()
                item_li.clear()
        cur.connection.commit()
    except Exception as e:
        logger.error('保存到mysql出错', e)
    cur.close()
    conn.close()


def update_minus(minus_dic):
    conn, cur = connect_mysql()
    engine = create_engine(
        "mysql+mysqldb://%s:%s@%s/%s?charset=utf8mb4" % (config.user, config.passwd, config.ip, config.db))

    sql = """SELECT `business_id`, `name`, `address`,
                `telephone`, `month_saled`, `shop_announcement`, `latitude`, `longitude`, `geohash`,
                `avg_rating`, `business_url`, `photo_url`, `float_minimum_order_amount`, `float_delivery_fee`,
                `delivery_consume_time`, `work_time`, `md5`, `mt_poi_id`, `minus` FROM new_hudong_db.meituan_tenantinfo
                """
    items = pd.read_sql(sql, conn, index_col='business_id')
    cur.close()
    conn.close()

    minus_num = 0
    minus_count = len(minus_dic)
    for i, j in minus_dic.items():
        items.loc[i, 'minus'] = j
        logger.info('正在整合第%s/%s条数据' % (minus_num, minus_count))
        minus_num += 1

    items = items.drop_duplicates(['mt_poi_id'])
    get_tenant_info.create_table()
    logger.info('开始存入数据库。')
    items.to_sql('meituan_tenantinfo', con=engine, if_exists='append', index=True, index_label=None)
    return


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
    # return [["438757662434992"]]


def get_mt_poi_ids():
    conn, cur = connect_mysql()
    sql = """
              SELECT tenant_id, mt_poi_id
              FROM meituan_tenantid
              """
    # sql += 'LIMIT 1'
    cur.execute(sql)
    tenant_ids = cur.fetchall()
    conn.close()
    cur.close()
    id_dic = dict()
    for i, j in tenant_ids:
        id_dic[i] = j
    return id_dic


# **********************************download_page
def crawler():
    """
    爬虫主函数，异步爬取店铺ID页，并保存爬取到的json文件至本地。
    两个线程：异步爬取线程、添加任务线程
    :return:
    """
    put_url_in_queue()

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


def put_url_in_queue():
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
    ids = get_tenant_ids()
    if dir_name not in (os.listdir(os.curdir)):
        os.mkdir(dir_name)
    for tenant_id in ids:
        url_item['post_data']['wm_poi_id'] = tenant_id[0]
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
        file_name = url_item['post_data']['wm_poi_id'] + ".json"
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
    global file_num
    global jsn_count
    create_table()
    os.chdir(os.curdir + '\\' + dir_name + '\\')
    jsn_files = os.listdir(os.curdir)
    count = 0
    file_num = 0
    items = set()
    minus_dic = dict()
    file_count = len(jsn_files)
    id_dic = get_mt_poi_ids()
    pool = Pool(10)
    read_file_args = []
    for jsn_file in jsn_files:
        read_file_args.append([jsn_file, id_dic, minus_dic, items])
    pool.map(read_file, read_file_args)
    pool.close()
    pool.join()
    logger.info('count: %d\n' % count)
    create_table()
    print(1)
    update_minus(minus_dic)
    print(2)
    save_to_mysql(items)
    print(3)
    return None


def read_file(args):
    jsn_file = args[0]
    id_dic = args[1]
    minus_dic = args[2]
    items = args[3]
    global file_num
    global jsn_count
    logger.info('正在处理第%d/%d个文件%s' % (file_num, file_count, jsn_file))
    file_num += 1
    with open(jsn_file, 'rb') as f:
        jsn_str = f.read()
    try:
        jsn_dic = json.loads(jsn_str)
    except Exception as e:
        logger.error("发生错误：%s" % e)
        return
    info_jsn = jsn_dic['data']
    if not info_jsn:
        return

    minus_str = ''
    for minus in info_jsn['poi_info']['discounts2']:
        minus_str += minus['info']
        minus_str += ' '
    # minus_li.append((minus_str, info_jsn['poi_info']['id']))
    minus_dic[str(info_jsn['poi_info']['id'])] = minus_str

    food_set = set()
    for tag in info_jsn['food_spu_tags']:
        for food in tag['spus']:
            if food['id'] not in food_set:
                food_set.add(food['id'])
                item = load_item(food, id_dic, info_jsn['poi_info']['id'])
                items.add(item)
    return


def load_item(food, id_dic, tenant_id):
    item = dict()
    item['food_id'] = food['id']
    item['business_id'] = tenant_id
    item['price'] = food['min_price']
    item['name'] = food['name']
    item['image_path'] = food['picture']
    item['month_saled'] = food['month_saled']
    item['description'] = food['description']

    specs_values = dict()
    for specs in food['attrs']:
        name = specs['name']
        specs_values[name] = []
        for spec_value in specs['values']:
            specs_values[name].append(spec_value['value'])
    if not specs_values:
        item['specs_value'] = ''
    else:
        item['specs_value'] = str([specs_values]).replace('\'', '\"')

    specs_features = []
    for spec in food['skus']:
        specs_features.append({
            "name": spec['description'],
            "price": spec['price'],
            "specs_value": spec['spec']
        })
    if not specs_features:
        item['specs_features'] = ''
    else:
        item['specs_features'] = str(specs_features).replace('\'', '\"')

    md5 = ''
    for k, j in item.items():
        md5 += str(j)
    item['md5'] = hashlib.md5(md5.encode('utf8')).hexdigest()

    item['mt_poi_id'] = id_dic[str(item['business_id'])]

    item = (
        item['food_id'],
        item['business_id'],
        item['price'],
        item['name'],
        item['image_path'],
        item['month_saled'],
        item['description'],
        item['specs_value'],
        item['specs_features'],
        item['md5'],
        item['mt_poi_id']
    )
    return item


if __name__ == '__main__':
    start_time = time.time()

    # crawler()

    process_jsn()

    logger.info('程序耗时： %f分' % ((time.time() - start_time) / 60))
    # time.sleep(1000)
