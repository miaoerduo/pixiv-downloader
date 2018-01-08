# -*- coding: utf-8 -*-

"""
本程序借助pixivpy，用于抓取pixiv的每天的热门插画
"""

import datetime
import os
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor

try:
    import cPickle as pickle
except:
    import pickle

import pixivpy3 as pp


class ImageTask(object):

    def __init__(self, img_url, title, date, rank, page_idx=None):
        self.img_url = img_url
        self.title = title
        self.page_idx = page_idx
        self.date = date
        self.rank = rank

    def get_url_md5(self):
        m = hashlib.md5()
        m.update(self.img_url.encode())
        return m.hexdigest()
    
    def __str__(self):
        data = {
            'img_url': self.img_url,
            'title': self.title,
            'page_idx': self.page_idx,
            'date': self.date,
            'rank': self.rank
        }
        return json.dumps(data)


class PixivImageUrlExtractor(object):

    def __init__(self, api):
        self.api = api

    def extract_urls(self, start_date, end_date, max_rank=20):
        curr_date = start_date
        while curr_date < end_date:
            rank = 0
            param = {'mode': 'day', 'date': curr_date.strftime('%Y-%m-%d'), 'req_auth': True}

            while True:
                rank_data = self.api.illust_ranking(**param)
                illusts = rank_data['illusts']
                next_url = rank_data['next_url']
                for illust in illusts[:max_rank - rank]:
                    title = illust['title']
                    if illust['page_count'] == 1:
                        yield ImageTask(illust['meta_single_page']['original_image_url'], title, curr_date, rank, None)
                    else:
                        img_urls = [d['image_urls']['original'] for d in illust['meta_pages']]
                        for page_idx, img_url in enumerate(img_urls):
                            yield ImageTask(img_url, title, curr_date, rank, page_idx)
                    rank += 1
                if rank >= max_rank or next_url is None:
                    break
                param = self.api.parse_qs(next_url)
            curr_date += datetime.timedelta(1)


class Downloader(object):
    def __init__(self, save_root, api, visited_urls=None, try_time=5, try_interval=1):
        self.save_root = save_root
        self.api = api
        self.visited_urls = visited_urls if visited_urls is not None else set()
        self.try_time = try_time
        self.try_interval = try_interval

    def download(self, image_task):

        url_md5 = image_task.get_url_md5()
        if url_md5 in self.visited_urls:
            return

        date = image_task.date
        folder_name = os.path.join(self.save_root, date.strftime('%Y%m%d'))
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        image_name = '{:08} {}'.format(image_task.rank, image_task.title)
        if image_task.page_idx is not None:
            image_name += " " + str(image_task.page_idx)
        suffix = image_task.img_url[image_task.img_url.rfind('.'):]
        image_name += suffix

        self.visited_urls.add(url_md5)
        try_count = 0
        while try_count < self.try_time:
            try:
                self.api.download(
                    url = image_task.img_url,
                    path=folder_name,
                    name=image_name
                )
                return
            except:
                try_count += 1
                time.sleep(self.try_interval)
        print('cannot download image task: {}'.format(image_task))
                

    def __call__(self, image_task):
        return self.download(image_task)


if __name__ == '__main__':

    start_date = datetime.datetime(2017, 1, 1)  # 开始时间
    end_date = datetime.datetime(2018, 1, 1)    # 结束时间（不含）
    day_rank_limit = 1                          # 每天的抓取数
    thread_num = 4                              # 线程数
    username = 'YOUR USER NAME'                 # pixiv用户名
    password = 'YOUR USER PASSWORD'             # pixiv密码
    save_root = './download'                    # 保存路径
    timeout = 10                                # 超时时间
    try_time = 5                                # 超时后重试次数
    try_interval = 1                            # 超时后重试的时间间隔

    api = pp.AppPixivAPI(timeout=timeout)
    resp = api.login(username, password)
    pixiv_image_url_extractor = PixivImageUrlExtractor(api)
    downloader = Downloader(save_root=save_root, api=api, try_time=try_time, try_interval=try_interval)

    try:
        with ThreadPoolExecutor(max_workers=thread_num) as e:
            e.map(downloader, pixiv_image_url_extractor.extract_urls(start_date, end_date, day_rank_limit))
    except Exception as et:
        print(et)
        with open('./snapshot', 'wb') as f:
            pickle.dump(downloader.visited_urls, f)
