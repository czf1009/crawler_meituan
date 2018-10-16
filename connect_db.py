import config
import pymysql
import aiomysql


def connect_mysql():
    try:
        conn = pymysql.connect(
            config.ip,
            user=config.user,
            password=config.passwd,
            db=config.db,
            charset='utf8mb4'
        )
    except pymysql.Error as e:
        raise SystemExit('Mysql Error %d: %s' % (e.args[0], e.args[1]))
    cur = conn.cursor()
    conn.ping(True)
    conn.autocommit(0)
    return conn, cur


async def connect_aiomysql(loop):
    conn = await aiomysql.connect(host=config.ip, port=3306,
                                  user=config.user, password=config.passwd,
                                  db=config.db, loop=loop,
                                  charset='utf8mb4'
                                  )
    cur = await conn.cursor()
    return conn, cur
