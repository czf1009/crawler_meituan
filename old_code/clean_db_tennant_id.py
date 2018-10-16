import pandas as pd
import pymysql
from sqlalchemy import create_engine

import config
import coord_transform
import geohash
import get_tenant_info
from connect_db import connect_mysql

pymysql.install_as_MySQLdb()


def update_lnglat():
    conn, cur = connect_mysql()

    sql = """SELECT `business_id`, `name`, `address`,
                `telephone`, `month_saled`, `shop_announcement`, `latitude`, `longitude`, `geohash`,
                `avg_rating`, `business_url`, `photo_url`, `float_minimum_order_amount`, `float_delivery_fee`,
                `delivery_consume_time`, `work_time`, `md5`, `mt_poi_id`, `minus` FROM new_hudong_db.meituan_tenantinfo_1
                """
    items = pd.read_sql(sql, conn, index_col='business_id')
    cur.close()
    conn.close()

    count = len(items)
    no = 0
    for i in items.index:
        print('正在整合第%s/%s条数据' % (no, count))
        items.loc[i, 'longitude'], items.loc[i, 'latitude']\
            = coord_transform.gcj02_to_bd09(float(items.loc[i, 'longitude']), float(items.loc[i, 'latitude']))
        items.loc[i, 'geohash'] = geohash.encode(float(items.loc[i, 'latitude']), float(items.loc[i, 'longitude']))
        no += 1

    # for i, j in minus_dic.items():
    #     items.loc[i, 'minus'] = j
    #     logger.info('正在整合第%s/%s条数据' % (minus_num, minus_count))
    #     minus_num += 1

    # items = items.drop_duplicates(['mt_poi_id'])
    # get_tenant_info.create_table()
    # logger.info('开始存入数据库。')

    get_tenant_info.create_table()
    engine = create_engine(
        "mysql+mysqldb://%s:%s@%s/%s?charset=utf8mb4" % (config.user, config.passwd, config.ip, config.db))
    items.to_sql('meituan_tenantinfo', con=engine, if_exists='append', index=True, index_label=None)
    return


# gd_lng = results['longitude']  # 高德经纬度转化为百度经纬度
# gd_lat = results['latitude']
# bd_latlng = coord_transform.gcj02_to_bd09(gd_lng, gd_lat)
# items['latitude'] = bd_latlng[1]
# items['longitude'] = bd_latlng[0]
# items['geohash'] = geohash.encode(items['latitude'], items['longitude'])

update_lnglat()