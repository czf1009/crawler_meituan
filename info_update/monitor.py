import connect_db
import requests
import json
import coord_transform
import time
import update_url_queue_server
import sync_formal_test_db
from log import Logger

logger = Logger(logname='monitor.log', loglevel=1, logger="monitor").getlog()

max_faild_times = 2

post_data = {
    "keyword": "a",
    "page_index": "0",
    "page_size": "100"
}
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544102&lng=113.947104',
}
cookies = {
    'w_latlng': '22544107,113947112'
}
url = 'http://i.waimai.meituan.com/search/ajax/v7/poi'


def get_url(lng, lat, name):
    lng, lat = coord_transform.bd09_to_gcj02(lng, lat)
    lng = str(lng).replace('.', '')[:9]
    lat = str(lat).replace('.', '')[:8]
    cookies['w_latlng'] = lat + ',' + lng
    post_data['keyword'] = name
    try:
        r = requests.post(url, headers=headers, data=post_data, cookies=cookies, allow_redirects=False)
        if r.status_code == 200:
            jsn = json.loads(r.text)
            return 'http://i.waimai.meituan.com/restaurant/' + str(jsn['data']['search_poi_list'][0]['id'])
    except Exception as e:
        logger.error('发生错误：%s' % e)


def api_is_updated(is_update):
    api_url_test = 'https://dev.qiyiniu.net/v1.1/change_url'
    api_url_formal = 'https://dev.qiyiniu.net/v1.1/change_url'
    api_post = {"is_updated": is_update}
    r = requests.post(api_url_test, data=api_post)
    jsn = json.loads(r.text)
    logger.info(r.status_code)
    logger.info(jsn)
    r = requests.post(api_url_formal, data=api_post)
    jsn = json.loads(r.text)
    logger.info(r.status_code)
    logger.info(jsn)
    return


def monitor():
    conn, cur = connect_db.connect_mysql()

    sql = """
        select t1.longitude, t1.latitude, t1.name, t1.business_url
        from meituan_tenantinfo as t1 join (select rand()*(select max(id) 
        from meituan_tenantinfo) as id ) as t2 on t1.id>t2.id 
        where business_url is not null
        limit 1;
          """
    faild_times = 1
    while True:
        # data = pd.read_sql(sql, conn, index_col='business_id')
        cur.execute(sql)
        data = cur.fetchone()
        if not data:
            continue
        if (get_url(float(data[0]), float(data[1]), data[2])) == data[3]:
            logger.info('未改变')
            faild_times = 0
        else:
            logger.info('无法获取url')
            if faild_times >= max_faild_times:
                logger.info('url已变化，开始更新')
                cur.close()
                conn.close()
                api_is_updated(0)
                start = time.time()
                update_url_queue_server.start_update()
                logger.info('主数据库更新完毕，更新至测试数据库。')
                sync_formal_test_db.sync_db()
                logger.info('更新完成，耗时%s分' % ((time.time()-start)/60))
                faild_times = 0
                api_is_updated(1)
                conn, cur = connect_db.connect_mysql()
            else:
                faild_times += 1
        time.sleep(1)

    cur.close()
    conn.close()


if __name__ == '__main__':
    monitor()
    # api_is_updated(1)

