# -*- coding: utf-8 -*-
import pymongo
import logging

MONGODB_ITEM_ID_FIELD = "_id"
MONGODB_UNIQ_KEY = "userid"

class MongoDBPipeline(object):
    def __init__(self, mongodb_server, mongodb_port, mongodb_db,mongodb_collection, mongodb_uniq_key,
                 mongodb_item_id_field):
        client = pymongo.MongoClient(mongodb_server, mongodb_port)
        self.mongodb_db = mongodb_db
        self.db = client[mongodb_db]
        self.collection = self.db[mongodb_collection]
        self.uniq_key = mongodb_uniq_key
        self.itemid = mongodb_item_id_field

        if isinstance(self.uniq_key, basestring) and self.uniq_key == "":
            self.uniq_key = None

        if self.uniq_key:
            self.collection.ensure_index(self.uniq_key, unique=True)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings.get('MONGODB_SERVER', 'localhost'), settings.get('MONGODB_PORT', 27017),
                   settings.get('MONGODB_DB', 'scrapy'), settings.get('MONGODB_COLLECTION', 'zhihupeople'),
                   settings.get('MONGODB_UNIQ_KEY', MONGODB_UNIQ_KEY), settings.get('MONGODB_ITEM_ID_FIELD', MONGODB_ITEM_ID_FIELD))

    def process_item(self, item, spider):
        if self.uniq_key is None:
            result = self.collection.insert(dict(item))
        else:
            result = self.collection.update({self.uniq_key: item[self.uniq_key]}, {'$set': dict(item)},
                                            upsert=True)

        # If item has _id field and is None
        if self.itemid in item.fields and not item.get(self.itemid, None):
            item[self.itemid] = result

        logging.log(logging.DEBUG, "Item %s wrote to MongoDB database %s" % (result, self.mongodb_db))

        # print repr(item).decode('unicode-escape')
        return item
