import json
import os
import time

import requests

import config
from connect_db import connect_mysql

session = requests.Session()  # 创建一个会话接口
requests.adapters.DEFAULT_RETRIES = 5
session.keep_alive = False  # 访问完后关闭会话

info_url = 'http://i.waimai.meituan.com/ajax/v6/poi/info'
post_data = {
}
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544098&lng=113.947144',
}
cookies = {
     # "poiid": "353189028939398"
}


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


def info_downloader():
    tenant_ids = get_tenant_ids()
    if 'json_info' not in (os.listdir(os.curdir)):
        os.mkdir('json_info')
    # download_info(tenant_ids[0][0])
    # pool = ThreadPool(1)
    # pool.starmap(download_info, tenant_ids)
    # pool.close()
    # pool.join()
    download_info(tenant_ids[0][0])


def download_info(tenant_id):
    post_data['wmpoiid'] = tenant_id
    # token = get_token.get_token('100010')
    token = ''
    # url = info_url + '?_token=' + token
    url = info_url
    try:
        r = session.post(url, headers=headers, data=post_data,
                         proxies=config.proxies, cookies=cookies, allow_redirects=False)
        print("正在爬取ID：%s  状态码：%s" % (tenant_id, r.status_code))
    except Exception as e:
        print(e)
    file_name = tenant_id + ".json"
    with open('json_info\\' + file_name, 'wb') as f:
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


def get_tenant_ids():
    conn, cur = connect_mysql()
    sql = """
              SELECT tenant_id
              FROM meituan_tenant_id
              """
    # sql = """
    #           SELECT latitude,longitude
    #           FROM meituan_tenantinfo
    #           """

    sql += 'LIMIT 1'

    cur.execute(sql)
    tenant_ids = cur.fetchall()
    conn.close()
    cur.close()
    # return tenant_ids
    return [["353189028939398"]]


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
    info_downloader()

    # process_jsn()
    print('\n\n程序耗时： %f分' % ((time.time() - start_time)/60))
