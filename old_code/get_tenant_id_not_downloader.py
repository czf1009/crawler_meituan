import json
import requests
from connect_db import connect_mysql
import config
import os
from multiprocessing.dummy import Pool as ThreadPool
import time
from url_queue import Queue
import logging
import random
from user_agents import agents

logger = logging.getLogger('get_tenant_id_not_downloader')
session = requests.Session()  # 创建一个会话接口
requests.adapters.DEFAULT_RETRIES = 5
session.keep_alive = False  # 访问完后关闭会话

queue = Queue()

post_data = {
    "page_index": "0",
}
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
}
cookies = {
    'w_latlng': '22555969,113893232'
}

dir_name = 'json_id'

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
        print('表已经存在', e)
    cur.close()
    conn.close()


def save_to_mysql(items):
    conn, cur = connect_mysql()
    try:
        cur.execute("set names utf8")
        cur.executemany("INSERT INTO meituan_tenant_id(tenant_id, mt_poi_id, name) VALUES (%s,%s,%s)", items)
        cur.connection.commit()
    except Exception as e:
        print('保存到mysql出错', e)
    cur.close()
    conn.close()


# **********************************download_page
def home_downloader():
    lnglats = get_lnglat()
    if dir_name not in (os.listdir(os.curdir)):
        os.mkdir(dir_name)
    lnglats_arg = []
    for lnglat in lnglats:
        lat = "{0:0<8}".format((lnglat[0]).replace('.', ''))[:8]
        lng = "{0:0<9}".format((lnglat[1]).replace('.', ''))[:9]
        lnglats_arg.append((lat, lng))
    pool = ThreadPool(1)
    pool.starmap(download_home, lnglats_arg)
    pool.close()
    pool.join()


def download_home(lat, lng):
    home_url = 'http://i.waimai.meituan.com/ajax/v6/poi/filter?lat=22.555366&lng=113.976517'
    cookies['w_latlng'] = lat + ',' + lng
    # page_num = get_page_num(home_url)
    page_index = 0
    while True:
        post_data['page_index'] = page_index
        headers['user-agent'] = random.choice(agents)
        try:
            r = session.post(home_url, headers=headers, data=post_data, cookies=cookies,
                             allow_redirects=False, proxies=config.proxies)
            print("正在爬取经纬度：%s, %s 第%s页  状态码：%s" % (lat, lng, page_index, r.status_code))
            if r.status_code != requests.codes.ok:
                for i in range(10):
                    r = session.post(home_url, headers=headers, data=post_data, cookies=cookies,
                                     allow_redirects=False, proxies=config.proxies)
                    if r.status_code == requests.codes.ok:
                        break
            if r.status_code != requests.codes.ok:
                print("页面下载失败：%s, %s 第%s页  状态码：%s" % (lat, lng, page_index, r.status_code))
                break
        except Exception as e:
            print('发生错误：%s' % e)

        file_name = lat + "_" + lng + '_' + str(page_index) + ".json"
        with open(dir_name + '\\' + file_name, 'wb') as f:
            f.write(r.content)
        jsn = json.loads(r.content)
        if jsn['code'] == 801:
            logger.warning('访问太频繁，请调整爬取延时。')
            time.sleep(10)
        elif jsn['data']['poi_has_next_page']:
            page_index += 1
        else:
            break
        time.sleep(3)
    return None


def get_page_num(url):
    post_data['page_index'] = '999'
    r = session.post(url, headers=headers, data=post_data, cookies=cookies,
                     allow_redirects=False, proxies=config.proxies)
    jsn = json.loads(r.content)
    if (jsn['data']['poi_total_num']/20) > jsn['data']['poi_total_num']//20:
        return (jsn['data']['poi_total_num']//20) + 1
    else:
        return jsn['data']['poi_total_num'] // 20


def get_lnglat():
    conn, cur = connect_mysql()
    province = config.province
    city = config.city
    region = config.region
    sql = """
              SELECT latitude,longitude
              FROM meituan_validlnglat
              where sign=0
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
    create_table()
    os.chdir(os.curdir + '\\'+dir_name+'\\')
    jsn_files = os.listdir(os.curdir)
    count = 0
    items = set()
    count_dump = 0
    file_num = 1
    file_count = len(jsn_files)
    for jsn_file in jsn_files:
        print('正在处理第%d/%d个文件%s' % (file_num, file_count, jsn_file))
        with open(jsn_file, 'rb') as f:
            jsn_str = f.read()
        try:
            jsn_dic = json.loads(jsn_str)
        except Exception as e:
            print("发生错误：%s" % e)
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
    print('count: %d\ncount_dump: %d' % (count, count_dump))
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
    home_downloader()
    # process_jsn()
    print('\n\n程序耗时： %f分' % ((time.time() - start_time)/60))
