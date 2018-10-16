import json
import requests
from connect_db import connect_mysql
import config
import os
from multiprocessing.dummy import Pool as ThreadPool
import time

post_data = {
    "keyword": "",
    "page_index": "0",
    "page_size": "100"
}
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/search/init',
}
cookies = {}


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
    count_faild = 0
    conn, cur = connect_mysql()
    try:
        cur.execute("set names utf8")
        # cur.executemany("INSERT INTO meituan_tenant_id(tenant_id, mt_poi_id, name) VALUES (%s,%s,%s)", items)
        for item in items:
            sql = "INSERT INTO meituan_tenant_id(tenant_id, mt_poi_id, name) VALUES('%s','%s','%s')" % \
                  (item[0], item[1], item[2])
            print(sql)
            if not is_exist(item[0]):
                cur.execute(sql)
                cur.connection.commit()
            else:
                count_faild += 1
    except Exception as e:
        print('\033[31;1m保存到mysql出错%s\033[0m' % e)
    finally:
        cur.close()
        conn.close()
    print('重复数据量：%s' % count_faild)


def is_exist(tenant_id, cur):
    if cur.execute('SELECT tenant_id FROM meituan_tenant_id WHERE tenant_id = %s;' % tenant_id):
        return True
    else:
        return False



def home_downloader():
    lnglats = get_lnglat()
    if 'json' not in (os.listdir(os.curdir)):
        os.mkdir('json')
    lnglats_arg = []
    for lnglat in lnglats:
        lat = "{0:0<8}".format((lnglat[0]).replace('.', ''))[:8]
        lng = "{0:0<9}".format((lnglat[1]).replace('.', ''))[:9]
        lnglats_arg.append((lat, lng))
    pool = ThreadPool(4)
    pool.starmap(download_home, lnglats_arg)
    pool.close()
    pool.join()


def download_home(lat, lng):
    home_url = 'http://i.waimai.meituan.com/search/ajax/v7/poi'
    latlng = lat + ',' + lng
    cookies['w_latlng'] = latlng
    r = requests.post(home_url, headers=headers, data=post_data, cookies=cookies, proxies=config.proxies, allow_redirects=False)

    print("正在爬取经纬度：%s, %s  状态码：%s" % (lat, lng, r.status_code))
    file_name = lat + "_" + lng + ".json"
    with open('json\\' + file_name, 'wb') as f:
        f.write(r.content)
    return None


def process_jsn():
    create_table()
    os.chdir(os.curdir + '\\json_2\\')
    jsn_files = os.listdir(os.curdir)
    count = 0
    items = set()
    count_dump = 0
    file_num = 1
    file_count = len(jsn_files)
    for jsn_file in jsn_files:
        print('正在处理第%d/%d个文件' % (file_num, file_count))
        with open(jsn_file, 'rb') as f:
            jsn_str = f.read()
        jsn_dic = json.loads(jsn_str)
        tenants = jsn_dic['data']['recommend_poi_list']
        for tenant in tenants:
            # print(tenant['id'], tenant['mt_poi_id'], tenant['name'])
            count += 1
            item = (tenant['id'], tenant['mt_poi_id'], transcode(tenant['name']))
            # item = tenant['name']
            if item not in items:
                items.add(item)
            else:
                count_dump += 1
        file_num += 1
    create_table()
    save_to_mysql(items)
    # with open('..\\id_list.txt', 'w') as f:
    #     for i in items:
    #         f.write(i[2] + '\n')
    print('count: %d\ncount_dump: %d' % (count, count_dump))
    return None


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

    sql += 'LIMIT 1'

    cur.execute(sql)
    lnglats = cur.fetchall()
    conn.close()
    cur.close()
    return lnglats


def transcode(text):
    text = text.replace('\'', '\\\'')
    text = text.replace('\"', '\\\"')
    text = text.replace('\n', '\\n')
    return text


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
