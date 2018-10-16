import hashlib
import json
from connect_db import connect_mysql
import config
import os
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
import get_tenant_info
from pymongo import MongoClient

queue_food = Queue()
downloader = Downloader(queue_food)

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
    logger.info("正在插入商品的数据，共有%s条" % len(items))
    start = time.time()
    print('开始转化为dataframe')
    items = pd.DataFrame(items)
    print('转换完毕，耗时%s' % (time.time() - start))
    engine = create_engine(
        "mysql+mysqldb://%s:%s@%s/%s?charset=utf8mb4" % (config.user, config.passwd, config.ip, config.db))
    items = items.drop_duplicates(['food_id'])
    create_table()
    items = items.set_index('business_id')
    items.to_sql('meituan_goodsinfo', con=engine, if_exists='append', index=True, index_label=None)
    print('已插入完成, 本次插入耗时%sS' % (time.time()-start))


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

    cur.execute(sql)
    tenant_ids = cur.fetchall()
    conn.close()
    cur.close()
    return tenant_ids


def get_mt_poi_ids():
    conn, cur = connect_mysql()
    sql = """
              SELECT tenant_id, mt_poi_id
              FROM meituan_tenantid
              """
    cur.execute(sql)
    tenant_ids = cur.fetchall()
    conn.close()
    cur.close()
    id_dic = dict()
    for i, j in tenant_ids:
        id_dic[i] = j
    return id_dic


# **********************************download_page
def crawler(loop):
    """
    爬虫主函数，异步爬取店铺ID页，并保存爬取到的json文件至本地。
    两个线程：异步爬取线程、添加任务线程
    :return:
    """
    main_loop = loop
    asyncio.set_event_loop(main_loop)

    client = MongoClient(config.mongo_host, config.mongo_port)
    db = client.meituan

    while True:
        if not queue_food.is_empty():
            asyncio.run_coroutine_threadsafe(spider(db), loop=main_loop)
            time.sleep(2)
        else:
            time.sleep(1)

    logger.info("\n\n==============================END======================\n\n")
    time.sleep(100)


def put_url_in_queue(wm_poi_id):
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
    url_item['post_data']['wm_poi_id'] = wm_poi_id
    queue_food.put(copy.deepcopy(url_item))


async def spider(db):
    """
    Fetch page and save to local path
    :return:
    """
    global done_count
    url_item = queue_food.get()
    logger.debug('spider')
    if url_item:
        r, text = await downloader.download(url_item)
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
            downloader.retry(url_item)
        db.food.insert_one(jsn)
    done_count += 1
    logger.info('已经下载完成%s个页面' % done_count)


# ********************************process_json_file
def process_jsn():
    """
    Process json file and save data to mysqlDB after crawler
    :return:
    """
    create_table()
    count = 0
    file_num = 0
    items = []
    minus_dic = dict()

    client = MongoClient(config.mongo_host, config.mongo_port)
    db = client.meituan

    jsns = db.food.find({})
    jsn_count = db.food.count()

    id_dic = get_mt_poi_ids()

    for jsn in jsns:
        logger.info('正在处理第%d/%d个页面' % (file_num, jsn_count))
        info_jsn = jsn['data']
        if not info_jsn:
            continue

        if 'poi_info' not in info_jsn.keys():
            continue
        minus_str = ''
        for minus in info_jsn['poi_info']['discounts2']:
            minus_str += minus['info']
            minus_str += ' '
        minus_dic[str(info_jsn['poi_info']['id'])] = minus_str

        food_set = set()
        for tag in info_jsn['food_spu_tags']:
            for food in tag['spus']:
                if food['id'] not in food_set:
                    food_set.add(food['id'])
                    item = load_item(food, id_dic, info_jsn['poi_info']['id'])
                    if item:
                        items.append(item)
        file_num += 1

    logger.info('count: %d\n' % count)
    create_table()
    print(1)
    update_minus(minus_dic)
    print(2)
    save_to_mysql(items)
    print(3)
    return None


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

    try:
        item['mt_poi_id'] = id_dic[str(item['business_id'])]
    except Exception as e:
        pass

    return item


if __name__ == '__main__':
    start_time = time.time()

    # crawler()

    process_jsn()

    logger.info('程序耗时： %f分' % ((time.time() - start_time) / 60))
    # time.sleep(1000)
