import pandas as pd
from connect_db import connect_mysql
from sqlalchemy import create_engine
import config
from log import Logger
import pymysql
pymysql.install_as_MySQLdb()

logger = Logger(logname='monitor.log', loglevel=1, logger="monitor").getlog()


def create_table():
    try:
        conn = pymysql.connect(
            '127.0.0.1',
            user='root',
            password='hudongdata',
            db='hd',
            charset='utf8mb4'
        )
    except pymysql.Error as e:
        raise SystemExit('Mysql Error %d: %s' % (e.args[0], e.args[1]))
    cur = conn.cursor()
    conn.ping(True)
    conn.autocommit(0)

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


def sync_db():
    if config.user != 'pywrite':
        logger.error('所操作的数据库不是主数据库，不应同步至测试数据库！')
        return

    conn, cur = connect_mysql()
    sql = """SELECT * FROM new_hudong_db.meituan_tenantinfo"""
    logger.info('读取主数据库数据')
    items = pd.read_sql(sql, conn, index_col='business_id')
    logger.info('读取完成')

    create_table()
    engine = create_engine(
        "mysql+mysqldb://%s:%s@%s/%s?charset=utf8mb4" % ('root', 'hudongdata', '120.77.8.6', 'hd'))
    logger.info('将数据存入测试数据库')
    items.to_sql('meituan_tenantinfo', con=engine, if_exists='append', index=True, index_label=None)
    logger.info('存入完成')

    cur.close()
    conn.close()

if __name__ == '__main__':
    sync_db()
