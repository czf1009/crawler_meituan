import aiohttp

province = '广东省'
city = '深圳市'
region = ''

max_retries = 20

# 本地数据库
# ip = "127.0.0.1"
# user = "root"
# passwd = "1234"
# db = "new_hudong_db"

# docker数据库
ip = "127.0.0.1"
user = "root"
passwd = "root"
db = "new_hudong_db"

# MongoDB
mongo_host = '127.0.0.1'
mongo_port = 27017

# 代理配置
use_proxy = False
proxyHost = ""
proxyPort = ""
proxyUser = ''
proxyPass = ""

# # requests
proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    "user": proxyUser,
    "pass": proxyPass,
    }
proxies = {
    "http": proxyMeta,
    "https": proxyMeta,
    }

# # aiohttp
proxy = "http://%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    }
proxy_auth = aiohttp.BasicAuth(proxyUser, proxyPass)


# selenium
service_args = [
    "--proxy-type=http",
    "--proxy=%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
    },
    "--proxy-auth=%(user)s:%(pass)s" % {
        "user": proxyUser,
        "pass": proxyPass,
    },
]