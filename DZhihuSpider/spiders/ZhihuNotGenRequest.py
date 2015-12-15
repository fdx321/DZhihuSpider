# -*- coding: utf-8 -*-
# coding: utf-8
from scrapy import signals, Request, FormRequest
from scrapy.selector import Selector
from scrapy.spiders import Spider, CrawlSpider
from scrapy.xlib.pydispatch import dispatcher
from scrapy_redis import connection
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisMixin

from DZhihuSpider.items import ZhihuspiderItem

try:
    import cPickle as pickle
except ImportError:
    import pickle

QUEUE_KEY = 'Zhihu:requests'


class ZhihuNotGenRequestSpider(RedisMixin, CrawlSpider):
    name = "ZhihuNotGenRequest"
    allowed_domains = ['zhihu.com']
    start_urls = ['http://www.zhihu.com/people/xiao-liu-95-62/about']
    host = 'http://www.zhihu.com'

    def __init__(self):
        settings = get_project_settings()
        self.queue_key = settings.get('SCHEDULER_QUEUE_KEY', QUEUE_KEY)
        self.server = connection.from_settings(settings)
        self.headers = {
            "Host": "www.zhihu.com",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
            "Referer": "http://www.zhihu.com/people/raymond-wang",
            "Accept-Encoding": "gzip,deflate,sdch",
            "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-TW;q=0.2",
        }
        self.cookies = {
            '_za': r'bda5810c-88f0-40a8-8d2b-d9be0e0c58a9',
            'q_c1': r'28f9a453b53a482486644378553c3a10|1447162001000|1447162001000',
            '_xsrf': r'4307a4b2977f25efbdacbd89edf2e789',
            'cap_id': r'"OThkOGIwMDVkMDllNGZmMzkzN2JkY2MzNzhhMmZjZWQ=|1448186640|774a87a7e0bd5ecec150a0d4bed38b570859c822"',
            'z_c0': r'"QUFBQUF1VWdBQUFYQUFBQVlRSlZUUjBnZVZZM0ptcEVROU9YSzZ3bXpUUEJXQm0zSUkxSFl3PT0=|1448186653|1eb9dfd0eff895cab5c818fd97d103a17d557dfe"',
            'unlock_ticket': r'"QUFBQUF1VWdBQUFYQUFBQVlRSlZUU1dhVVZhcmFDck02VUROeVV3c1oyRHQ1aWduQmVLYWdRPT0=|1448186653|c734f11184740390f0b34536e218952aabdcff46"',
            '__utmt': r'1',
            '__utma': r'51854390.16347795.1448186642.1448186642.1448186642.1',
            '__utmb': r'51854390.18.10.1448186642',
            '__utmc': r'51854390',
            '__utmz': r'51854390.1448186642.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
            '__utmv': r'51854390.100-1|2=registration_date=20131118=1^3=entry_date=20131118=1'
        }
        super(ZhihuNotGenRequestSpider, self).__init__()

    def start_requests(self):
        for i, url in enumerate(self.start_urls):
            yield FormRequest(url, meta={'cookiejar': i}, headers=self.headers, cookies=self.cookies,
                              callback=self.parse_about)

    def request_from_dict(self, d):
        cb = getattr(self, 'parse_about')
        return Request(
            url=d['url'].encode('ascii'),
            callback=cb,
            errback=None,
            method=d['method'],
            headers=d['headers'],
            body=d['body'],
            cookies=d['cookies'],
            meta=d['meta'],
            encoding=d['_encoding'],
            priority=d['priority'],
            dont_filter=d['dont_filter'])

    def parse_about(self, response):
        selector = Selector(response)
        item = ZhihuspiderItem()
        # http://www.zhihu.com/people/jiang-xiao-fan-92-14/about
        # 取jiang-xiao-fan-92-14作为userID
        item['userid'] = response.url.split('/')[-2]
        item['name'] = selector.css('div>.name').xpath('text()').extract()
        item['location'] = selector.css('.location>a').xpath('text()').extract()
        item['business'] = selector.css('.business>a').xpath('text()').extract()
        item['gender'] = selector.css('.gender>i').xpath('@class').extract()
        item['employment'] = selector.css('.employment>a').xpath('text()').extract()
        item['position'] = selector.css('.position>a').xpath('text()').extract()
        item['education'] = selector.css('.education>a').xpath('text()').extract()
        item['major'] = selector.css('.education-extra>a').xpath('text()').extract()

        for (key, val) in item.items():
            if isinstance(val, list) and len(val) > 0:
                item[key] = val[0]
            elif len(val) == 0:
                item[key] = ''

        if item['gender'].find('female') != -1:
            item['gender'] = u'female'
        elif item['gender'].find('male') != -1:
            item['gender'] = u'male'
        else:
            item['gender'] = u'unknown'

        yield item

        while self.server.llen(self.queue_key) > 2000:
            encoded_request = self.server.rpop(self.queue_key)
            if encoded_request:
                request = self.request_from_dict(pickle.loads(encoded_request))
                if request._get_url().find('about') == -1:
                    self.server.lpush(self.queue_key, encoded_request)
                    continue
            yield request
            break
