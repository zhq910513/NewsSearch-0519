#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
@author: the king
@project: xxx
@file: resp_tools.py
@time: 2023/4/6 19:15
"""

from multiprocessing.pool import ThreadPool


class ThreadScheduler:
    def __init__(self, _list, func=None, cookie=None, ip=None):
        self.list = _list
        self.func = func
        self.cookie = cookie
        self.ip = ip

    def thread_handle(self):
        pool = ThreadPool(processes=5)
        thread_list = []
        for _ls in self.list:
            if self.cookie and self.ip:
                out = pool.apply_async(func=self.func, args=(_ls,self.cookie,self.ip,))  # 异步
            else:
                out = pool.apply_async(func=self.func, args=(_ls,self.cookie,self.ip,))  # 异步
            thread_list.append(out)
            # break
        pool.close()
        pool.join()

        # 获取输出结果
        com_list = []
        for p in thread_list:
            com = p.get()  # get会阻塞
            com_list.append(com)
        com_list = [i for i in com_list if i is not None]
        return com_list