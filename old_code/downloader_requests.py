import config
import requests


class Downloader(object):
    def __init__(self, queue):
        self.queue = queue
        self.session = requests.Session()  # 创建一个会话接口
        requests.adapters.DEFAULT_RETRIES = 5
        self.session.keep_alive = False  # 访问完后关闭会话
        self.dir_name = 'json'

    def download(self, url_item):
        try:
            if config.use_proxy:
                r = self.session.post(url_item['url'], headers=url_item['headers'], data=url_item['post_data'],
                                      cookies=url_item['cookies'], allow_redirects=False, proxies=config.proxies)
            else:
                r = self.session.post(url_item['url'], headers=url_item['headers'], data=url_item['post_data'],
                                      cookies=url_item['cookies'], allow_redirects=False)
        except Exception as e:
            print("请求连接时发生错误：%s\n爬取页面: %s\npost_data: %s\ncookies:%s" % (e, url_item['url'], url_item['post_data'],
                                                                         url_item['cookies']))
            self.retry(url_item)
            return None

        print("正在爬取页面: %s\npost_data: %s\ncookies:%s\n状态码：%s" % (url_item['url'], url_item['post_data'],
                                                                 url_item['cookies'], r.status_code))
        if r.status_code != requests.codes.ok:
            self.retry(url_item)
        else:
            return r

    def retry(self, url_item):
        if 'retry_times' in url_item:
            if url_item['retry_times'] <= config.max_retries:
                url_item['retry_times'] += 1
            else:
                print('重试超过%d次,页面: %s\npost_data: %s\ncookies:%s' % config.max_retries, url_item['url'],
                      url_item['post_data'], url_item['cookies'])
                return False
        else:
            url_item['retry_times'] = 1
        self.queue.put(url_item)
        return True
