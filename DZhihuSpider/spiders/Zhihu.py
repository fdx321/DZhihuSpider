# -*- coding: utf-8 -*-
# coding: utf-8
from scrapy.http import FormRequest
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider
from DZhihuSpider.items import ZhihuspiderItem
from scrapy_redis.spiders import RedisMixin
import json
import logging


class ZhihuSpider(RedisMixin, CrawlSpider):
    name = "Zhihu"
    allowed_domains = ['zhihu.com']
    start_urls = ['http://www.zhihu.com/people/daniuge/about']
    host = 'http://www.zhihu.com'

    def __init__(self):
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
        super(ZhihuSpider, self).__init__()

    def start_requests(self):
        for i, url in enumerate(self.start_urls):
            yield FormRequest(url, meta={'cookiejar': i}, headers=self.headers, cookies=self.cookies,
                              callback=self.parse_about)

    # 解析每个用户的基本信息
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

        # 解析完后,获得关注列表和被关注列表的链接,生成新的请求
        followeesHref = selector.xpath('/html/body/div[3]/div[2]/div[1]/a[1]/@href').extract()[0]
        followersHref = selector.xpath('/html/body/div[3]/div[2]/div[1]/a[2]/@href').extract()[0]
        yield FormRequest(self.host + followeesHref, meta={'cookiejar': 'followeesCookie'}, headers=self.headers,
                          cookies=self.cookies, callback=self.parse_followees)
        yield FormRequest(self.host + followersHref, meta={'cookiejar': 'followersCookie'}, headers=self.headers,
                          cookies=self.cookies, callback=self.parse_followers)

        yield item

    def parse_followees(self, response):
        selector = Selector(response)

        # 知乎上的关注了列表一次显示20条,后面的是通过ajax动态获取的,每次获取20条
        followees_or_followers = selector.xpath(
            '//*[@id="zh-profile-follows-list"]/div/div/div[2]/h2/a/@href').extract()
        for i in range(0, len(followees_or_followers)):
            yield FormRequest(followees_or_followers[i] + '/about', meta={'cookiejar': 'followersCookie'},
                              headers=self.headers, cookies=self.cookies, callback=self.parse_about)

        for request in self.ajax_request(selector, 'http://www.zhihu.com/node/ProfileFolloweesListV2'):
            yield request

    def parse_followers(self, response):
        selector = Selector(response)
        # 知乎上的关注者列表一次显示20条,后面的是通过ajax动态获取的,每次获取20条
        followees_or_followers = selector.xpath(
            '//*[@id="zh-profile-follows-list"]/div/div/div[2]/h2/a/@href').extract()
        for i in range(0, len(followees_or_followers)):
            yield FormRequest(followees_or_followers[i] + '/about', meta={'cookiejar': 'followersCookie'},
                              headers=self.headers, cookies=self.cookies, callback=self.parse_about)

        for request in self.ajax_request(selector, 'http://www.zhihu.com/node/ProfileFollowersListV2'):
            yield request

    def ajax_request(self, selector, url):
        # 获得被多少人关注或关注了多少人
        followingNum = selector.css('.zm-profile-side-following').xpath('a[2]/strong/text()').extract()[0]
        # 获得hashId
        hashId = selector.xpath('//*[@id="zh-profile-follows-list"]/div/@data-init').extract()
        # 没有hashId, 关注数量或被关注数量为0
        if len(hashId) == 0:
            return
        hashId = json.loads(hashId[0])['params']['hash_id']
        # 发起Ajax请求获取剩余的名单,注意dont_filter=True, 不然scrapy会认为该链接已经请求过就不会在请求了
        pageNum = int(followingNum) / 20
        for i in range(1, pageNum):
            yield FormRequest(url, meta={'cookiejar': 'followeesCookie'},
                              headers=self.headers, cookies=self.cookies,
                              formdata={'method': 'next',
                                        'params': '{"offset":' + str(
                                            i * 20) + ',"order_by":"created","hash_id":"' + hashId + '"}',
                                        '_xsrf': self.cookies['_xsrf']},
                              dont_filter=True,
                              callback=self.parse_following_list)

    def parse_following_list(self, response):
        logging.log(logging.DEBUG, "This is a list from ajax")
        # 获取服务器返回的json数据, 关注者或被关注者列表
        followings = json.loads(response.body)['msg']
        for following in followings:
            selector = Selector(text=following)
            # 提取关注者或被关注者的链接
            followingUrl = selector.css('.zm-list-content-title').xpath('a/@href').extract()[0]
            yield FormRequest(followingUrl + '/about', meta={'cookiejar': 'followingCookie'},
                              headers=self.headers, cookies=self.cookies, callback=self.parse_about)
