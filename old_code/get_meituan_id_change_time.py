import datetime
import requests
import json
import time
import config



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


def download_home(lat, lng):
    home_url = 'http://i.waimai.meituan.com/search/ajax/v7/poi'
    latlng = lat + ',' + lng
    cookies['w_latlng'] = latlng
    r = requests.post(home_url, headers=headers, data=post_data, cookies=cookies, allow_redirects=False)

    print("正在爬取经纬度：%s, %s  状态码：%s" % (lat, lng, r.status_code))
    file_name = lat + "_" + lng + ".json"
    with open('json\\' + file_name, 'wb') as f:
        f.write(r.content)
    return None


if __name__ == '__main__':
    with open('id_monitor.log', 'a') as f:
        while True:
            try:
                r = requests.post(url, headers=headers, data=post_data, cookies=cookies, proxies=config.proxies, allow_redirects=False)
                if r.status_code == 200:
                    jsn = json.loads(r.text)
                    f.write('%s  %s\n' %
                            (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), jsn['data']['search_poi_list'][0]['id']))
                else:
                    f.write(str(r.status_code)+'\n')
            except Exception as e:
                print('发生错误：%s\n' % e)
                f.write('发生错误：%s\n' % e)
            f.flush()
            time.sleep(60)
