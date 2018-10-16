from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import config
import requests
import re
import time


driver = webdriver.PhantomJS('C:\\phantomjs.exe', service_args=config.service_args)
r = requests.get("https://awp-assets.meituan.net/hfe/rohr/1.0.2/rohr.min.js")
js_code = '''
    var rohrdata = "";
    var Rohr_Opt = new Object;
    Rohr_Opt.Flag = %s;
    Rohr_Opt.LogVal = "rohrdata";
    %s
    var result = Rohr_Opt.reload()
    document.write(result);
    ''' % (100009, r.text)


def get_token():
    """
    Flag_id: 100009
    Flag_info: 100010
    :return:
    """
    # driver = webdriver.Chrome('C:\\chromedriver.exe')
    # driver = webdriver.Firefox('C:\\geckodriver.exe', service_args=config.service_args)
    # driver = webdriver.Edge('C:\\MicrosoftWebDriver.exe')
    # driver = webdriver.Chrome('C:\\chromedriver.exe')
    # driver.set_window_size(1000,30000)
    # driver.get("http://ip.360.cn/IPShare/info")
    # driver.get("http://i.waimai.meituan.com/home?lat=22.590126&lng=113.884476")

    driver.execute_script(js_code)
    token = re.findall('<body>(.*?)</body>', driver.page_source)[0]
    return token


start = time.time()
for i in range(100):
    print(i)
    get_token()

print('程序耗时%s' % (time.time()-start))
