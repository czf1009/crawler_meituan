import random

import config
import requests
import asyncio
import aiohttp
import json
import logging
from user_agents import agents

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('downloader')


class Downloader(object):
    def __init__(self, queue):
        self.queue = queue
        # self.session = requests.Session()  # 创建一个会话接口
        # requests.adapters.DEFAULT_RETRIES = 5
        # self.session.keep_alive = False  # 访问完后关闭会话
        self.dir_name = 'json'
        self.session = aiohttp.ClientSession(conn_timeout=5)

    async def fetch_page(self, url_item):
        cookie_str = ''
        for i, j in url_item['cookies'].items():
            cookie_str += '%s=%s;' % (i, j)
        url_item['headers']['cookie'] = cookie_str
        url_item['headers']['user-agent'] = random.choice(agents)
        if config.use_proxy:
            async with self.session.post(url_item['url'], headers=url_item['headers'],
                                         data=json.dumps(url_item['post_data']),
                                         proxy=config.proxies['http']) as response:
                return response, await response.text(encoding='utf8')
        else:
            async with self.session.post(url_item['url'], headers=url_item['headers'], data=url_item['post_data'],
                                         ) as response:
                return response, await response.text()

    async def download(self, url_item):
        r, text = await self.fetch_page(url_item)
        text = text.encode('utf8')
        logger.info("正在爬取页面: %s\npost_data: %s\ncookies:%s\n状态码：%s" % (url_item['url'], url_item['post_data'],
                                                                       url_item['cookies'], r.status))

        if r.status != requests.codes.ok:
            self.retry(url_item)
        else:
            return r, text

    def retry(self, url_item):
        if 'retry_times' in url_item:
            if url_item['retry_times'] <= config.max_retries:
                url_item['retry_times'] += 1
            else:
                logger.debug('重试超过%d次,页面: %s\npost_data: %s\ncookies:%s' % config.max_retries, url_item['url'],
                             url_item['post_data'], url_item['cookies'])
                return False
        else:
            url_item['retry_times'] = 1
        self.queue.put(url_item)
        return True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.loop.close()
