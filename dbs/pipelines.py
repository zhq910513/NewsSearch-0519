from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from multiprocessing.pool import ThreadPool
from common.log_out import log_err
from common.config import MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USR, MONGO_PWD


class MongoPipeline:
    def __init__(self, COLLECTION):
        if MONGO_USR and MONGO_PWD:
            client = MongoClient(f'mongodb://{MONGO_USR}:{MONGO_PWD}@{MONGO_HOST}:{MONGO_PORT}')
        else:
            client = MongoClient(f'mongodb://{MONGO_HOST}:{MONGO_PORT}')
        self.coll = client[MONGO_DB][COLLECTION]

    def insert_item(self, item):
        status = False
        if not item:return False
        elif isinstance(item, list):
            for _i in item:
                try:
                    self.coll.insert_one(_i)
                    print(_i["hash_key"])
                except DuplicateKeyError:
                    status = True
                except Exception as error:
                    log_err(error)
                    status = True
        elif isinstance(item, dict):
            try:
                self.coll.insert_one(item)
            except DuplicateKeyError:
                status = True
                pass
            except Exception as error:
                log_err(error)
                status = True
        else:
            status = False
        return status

    @staticmethod
    def field_query(model, data):
        new_data = {}
        for key in model.keys():
            new_data.update({
                key: data.get(key)
            })
        return new_data

    def update_item(self, query, item):
        if not item: return
        if isinstance(item, list):
            thread_list = []
            # 设置进程数
            pool = ThreadPool(processes=5)
            for _i in item:
                out = pool.apply_async(func=self.item_up, args=(query, _i,))
                thread_list.append(out)
                # break

            pool.close()
            pool.join()
        elif isinstance(item, dict):
            try:
                mongo_data = self.coll.find_one({"hash_key": item["hash_key"]})
                if mongo_data:
                    # print("--- 已存在 ---")
                    if item.get("search_key"):
                        new_search_key = list(set(mongo_data["search_key"] + item["search_key"]))
                        item["search_key"] = new_search_key
                self.coll.update_one(self.field_query(query, item), {'$set': item}, upsert=True)
                # print(item)
            except Exception as error:
                log_err(error)

    def item_up(self, query, _item):
        try:
            _data = self.coll.find_one({"hash_key": _item["hash_key"]})
            if _data:
                # print("--- 已存在 ---")
                if _item.get("search_key"):
                    _search_key = list(set(_data["search_key"] + _item["search_key"]))
                    _item["search_key"] = _search_key
            self.coll.update_one(self.field_query(query, _item), {'$set': _item}, upsert=True)
            # print(_item)
        except Exception as error:
            log_err(error)

    def find(self, query):
        return self.coll.find(query)

    def delete(self, query):
        return self.coll.delete_one(query)

    def find_one(self, query):
        return self.coll.find_one(query)

    def count(self, query):
        return self.coll.count_documents(query)
