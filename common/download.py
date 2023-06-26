#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
@author: the king
@project: zyl_company
@file: download.py
@time: 2022/4/21 14:17
"""
import hashlib
import json
import os
import shutil
from multiprocessing.pool import ThreadPool
from os import path

import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from common.log_out import log_err, log
from .config import ARTICLEUPLOAD, MONGO_URI, MONGO_DB

client = MongoClient(MONGO_URI)
url_coll = client[MONGO_DB]["urls"]
article_coll = client[MONGO_DB]["articles"]
requests.packages.urllib3.disable_warnings()

picHeaders = {
    'accept': '*/*',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Host': '27.150.182.135:8855',
    'Origin': 'http://8.129.215.170:8855',
    'Pragma': 'no-cache',
    'Referer': 'http://8.129.215.170:8855/swagger-ui.html',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
}
videoPageHeaders = {
    'authority': 'v.jin10.com',
    'method': 'GET',
    'path': '/details.html?id=12574',
    'scheme': 'https',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
}
videoUploadHeaders = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
}
articleHeaders = {
    'accept': '*/*',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '532',
    'Content-Type': 'application/json',
    'Host': '8.129.215.170:8855',
    'Origin': 'http://8.129.215.170:8855',
    'Pragma': 'no-cache',
    # 'Referer': 'http://8.129.215.170:8855/swagger-ui.html',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
}
serverUrl = 'https://zuiyouliao-prod.oss-cn-beijing.aliyuncs.com/zx/image/'
videoServerUrl = 'http://qiniu.zuiyouliao.com/video/upload/'
pic_info = {'id': 0, 'pic_type': 3}
image_base_path = path.dirname(os.path.abspath(path.dirname(__file__)))

# 下载/上传 图片/视频 函数
def DownloadPicture_Video(img_path, img_info, retry=0):
    img_url = img_info[1]
    try:
        if not str(img_url).startswith("http"):return
        res = requests.get(img_url, timeout=60)
        if res.status_code == 200:
            basename = hashlib.md5(img_url.encode("utf8")).hexdigest() + '.jpg'
            filename = os.path.abspath(os.path.join(img_path + '/' + basename))
            with open(filename, "wb") as f:
                content = res.content
                f.write(content)

            # upload picture
            uploadUrl = 'http://27.150.182.135:8855/api/common/upload?composeId={0}&type={1}&isNameReal=0'.format(
                pic_info['id'], pic_info['pic_type'])

            files = {
                'file': (basename, open(filename, 'rb'), 'image/jpg')
            }
            picHeaders.update({
                'Content-Length': str(os.path.getsize(filename))
            })

            try:
                resp = requests.post(url=uploadUrl, headers=picHeaders, files=files, timeout=60)
                if resp.json().get('message') == '携带数据成功':
                    return_url = 'https://zuiyouliao-prod.oss-cn-beijing.aliyuncs.com' + resp.json().get('entity')['filePath']
                    print(f"id {pic_info['id']} *** type {pic_info['pic_type']} *** download image successfully:{img_url} *** upload {return_url}")
                    img_info.append(return_url)

                    return img_info
                else:
                    log_err(resp.json())
            except requests.exceptions.ConnectionError:
                log(f'服务器上传图片网络问题，重试中...{img_url}')
                if retry < 3:
                    return DownloadPicture_Video(img_path, img_url, retry + 1)
                else:
                    log_err(f'超过三次 服务器上传图片网络问题  {img_url}')
            except Exception as error:
                log_err(error)
                log_err(uploadUrl)
    except requests.exceptions.ConnectionError:
        print(f'下载图片网络问题，重试中...  {img_url}')
        if retry < 3:
            return DownloadPicture_Video(img_path, img_url, retry + 1)
    except Exception as error:
        log_err(error)
        return None
    return None


# 多线程处理数据
def command_thread(company_name, image_list, Async=True):
    file_path = os.path.abspath(image_base_path + f'/download_data/{company_name}')
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    thread_list = []
    # 设置进程数
    pool = ThreadPool(processes=8)

    for img_info in image_list:
        print(f'------------------ {img_info}')
        if Async:
            out = pool.apply_async(func=DownloadPicture_Video, args=(file_path, img_info,))  # 异步
        else:
            out = pool.apply(func=DownloadPicture_Video, args=(file_path, img_info,))  # 同步
        thread_list.append(out)
        # break
    pool.close()
    pool.join()

    # 获取输出结果
    com_list = []
    if Async:
        for p in thread_list:
            com = p.get()  # get会阻塞
            com_list.append(com)
    else:
        com_list = thread_list
    com_list = [i for i in com_list if i is not None]

    # 删除文件夹
    shutil.rmtree(file_path, True)

    return com_list


# 上传文章
def UploadArticle(dataJson: dict):
    try:
        if '验证码' not in str(dataJson.get('content')) and '验证码' not in str(dataJson.get('cssHtml')):
            resp = requests.post(url=ARTICLEUPLOAD, headers=articleHeaders, data=json.dumps(dataJson), timeout=60)
            if resp.json().get('ok'):
                print("文章id {0} *** upload Article successfully *** upload status {1}".format(dataJson.get('id'), resp.json().get('code')))

                # 数据备份与标记
                url_coll.update_one({'hash_key': dataJson.get('id')}, {"$set":{'hash_key': dataJson.get('id'), 'status': 1}}, upsert=True)
                try:
                    article_coll.insert_one(dataJson)
                except DuplicateKeyError:
                    pass
            elif resp.json().get('status') == 500 and 'DuplicateKey' in resp.json().get('exception'):
                pass
            elif resp.json().get('status') == 500 and resp.json().get('error') == 'Internal Server Error':
                try:
                    print(resp.json())
                    url_coll.update_item({'hash_key': dataJson.get('id')}, {"$set":{'hash_key': dataJson.get('id'), 'status': 500}}, upsert=True)
                except:
                    pass
            else:
                print(resp.json(), json.dumps(dataJson))
        else:
            print('--- 检测正文有验证码 ---')
            return
    except requests.exceptions.ConnectionError:
        print('上传文章网络问题，重试中...')
        return UploadArticle(dataJson)
    except Exception as error:
        log_err(error)
