# -*- coding: utf-8 -*-
# coding: utf-8
from scrapy import signals, Request
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.xlib.pydispatch import dispatcher
from scrapy_redis import connection
from scrapy.utils.project import get_project_settings
from DZhihuSpider.items import ZhihuspiderItem

try:
    import cPickle as pickle
except ImportError:
    import pickle

QUEUE_KEY = 'Zhihu:requests'


class ZhihuNotGenRequestSpider(Spider):
    name = "ZhihuNotGenRequest"
    allowed_domains = ['zhihu.com']
    start_urls = ['http://www.zhihu.com/people/yu-kuang-76/about']
    host = 'http://www.zhihu.com'

    def __init__(self):
        settings = get_project_settings()
        self.queue_key = settings.get('SCHEDULER_QUEUE_KEY', QUEUE_KEY)
        self.server = connection.from_settings(settings)

        dispatcher.connect(self.get_requests, signals.spider_opened)
        super(ZhihuNotGenRequestSpider, self).__init__()

    def get_requests(self, spider):
        if self.server.llen(self.queue_key) > 2000:
            encoded_request = self.server.rpop(self.queue_key)
            if encoded_request:
                request = self.request_from_dict(pickle.loads(encoded_request))
            self.crawler.engine.crawl(request, spider)

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
