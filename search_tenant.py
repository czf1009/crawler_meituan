import datetime
import requests
import json
import time
import config
import coord_transform
import asyncio
from downloader import Downloader
from queue import Queue
import copy

queue_food = Queue()
downloader = Downloader(queue_food)

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'referer': 'http://i.waimai.meituan.com/home?lat=22.544102&lng=113.947104',
}
url = 'http://i.waimai.meituan.com/search/ajax/v7/poi'


async def get_url(lng, lat, name):
    lng, lat = coord_transform.bd09_to_gcj02(lng, lat)
    lng = str(lng).replace('.', '')[:9]
    lat = str(lat).replace('.', '')[:8]
    post_data = {
        "keyword": name,
        "page_index": "0",
        "page_size": "100"
    }
    cookies = {
        'w_latlng': lat + ',' + lng
    }
    url_item = copy.deepcopy({'url': url, 'cookies': cookies, 'headers': headers, 'post_data': post_data})
    # r = requests.post(url, headers=headers, data=post_data, cookies=cookies, allow_redirects=False)

    r, text = await downloader.download(copy.deepcopy(url_item))
    if r.status == 200:
        jsn = json.loads(text)
        search_poi_list = jsn['data']['search_poi_list']
        for tenant in search_poi_list:
            if tenant['name'] == name:
                print('http://i.waimai.meituan.com/restaurant/' + str(tenant['id']))
                return 'http://i.waimai.meituan.com/restaurant/' + str(tenant['id'])
        print('搜索不到该店铺')
        print(text.decode('utf8'))
        # downloader.retry(url_item)
        return False


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    tasks = [get_url(114.146738715051, 22.6172638578298, '花溪牛肉粉')]
    loop.run_until_complete(asyncio.wait(tasks))
