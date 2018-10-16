import numpy as np
import pandas as pd
import asyncio
import aiomysql
# import matplotlib.pyplot as plt
import pymysql
import connect_db

pymysql.install_as_MySQLdb()

loop = asyncio.get_event_loop()

global conn
global cur
async def get():
    global conn
    global cur
    conn, cur = await connect_db.connect_aiomysql(loop)


async def go():
    await cur.execute('UPDATE `new_hudong_db`.`meituan_tenantinfo` SET `business_url`="ccc" WHERE `id`="31159";')
    await conn.commit()

    conn.close()

loop.run_until_complete(get())
loop.run_until_complete(go())
