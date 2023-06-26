import copy
import json
import pprint
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from common.download import command_thread, UploadArticle

pp = pprint.PrettyPrinter(indent=4)
from dbs.pipelines import MongoPipeline
from common.log_out import log_err

format_data = {
    'id': None,  # string素材文章id
    'channelName': None,  # 频道
    'tagGroupName': None,  # 标签组
    'tagName': None,  # 子标签
    'articleAddress': None,  # 文章位置
    'type': None,  # 2 资讯/文章  3 快讯
    'source': None,  # string 来源网站

    'titleName': None,  # string 素材标题
    'content': None,  # string 正文
    'readCount': None,  # integer($int32) 阅读量
    'cssHtml': None,  # 文章源码
    'sourceLink': None,  # string 来源链接

    'titleImageUrls': None,  # [...] 标题图片
    'contentImageUrls': None,  # [...] 内容图片

    'createTime': None,  # string($date-time) 创建时间
    'releaseTime': None,  # string($date-time) 发布时间
    'updateTime': None,  # string($date-time) 更新时间
}

url_coll = MongoPipeline("urls")
article_coll = MongoPipeline("articles")


def get_article(info, retry=0):
    try:
        url = info["url"]
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)
        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            parse_article(info, res.text)
    except Exception as error:
        if retry < 3:
            return get_article(info, retry + 1)
        else:
            log_err(error)


def parse_article(info, _html):
    platform = info["platform"]
    func = {
        "plasticstoday": plasticstoday,
        "interplasinsights": interplasinsights,
        "sustainableplastics": sustainableplastics,
        "kunststoffe": kunststoffe,
        "plasticportal": plasticportal,
        "medicalplasticsnews": medicalplasticsnews,
        "packagingnews": packagingnews,
        "packworld": packworld,
        "packagingdigest": packagingdigest,
        "packagingeurope": packagingeurope,
        "ptonline": ptonline,
        "phys": phys
    }
    func[platform](info, _html)


def plasticstoday(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("meta", {"itemprop": "datePublished"}).get("content")
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("span", {"class": "bread-currentpage"}).get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = []
        for p in soup.find("div", {"itemprop": "articleBody"}).find_all("p"):
            _text = p.get_text()
            if _text:
                content.append(_text.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("\xa0", " "))
        if content:
            insert_data["content"] = "\n\n".join(content)
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        try:
            for img in soup.find("div", {"class": "big-article__image"}).find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://www.plasticstoday.com" + img_url
                contentImageUrls.append([img_url, img_url_new])
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        tagName = []
        for a in soup.find("div", {"class": "secondary-tags"}).find_all("a"):
            tagName.append(a.get_text().strip())
        if tagName:
            insert_data["tagName"] = " | ".join(tagName)
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            img_cssHtml = soup.find("div", {"class": "big-article__top"})
        except:
            img_cssHtml = None

        cssHtml = soup.find("div", {"itemprop": "articleBody"})
        if img_cssHtml:
            cssHtml = str(img_cssHtml) + str(cssHtml)

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def interplasinsights(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("meta", {"property": "article:published_time"}).get("content")
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("h1", {"itemprop": "headline"}).get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = soup.find("article", {"id": "article_content"}).get_text()
        if content:
            insert_data["content"] = content
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        set_urls = []
        try:
            for img in soup.find("article", {"id": "article_content"}).find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://interplasinsights.com" + img_url
                if img_url not in set_urls:
                    contentImageUrls.append([img_url, img_url_new])
                    set_urls.append(img_url)
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        tagName = []
        for a in soup.find("div", {"class": "tags"}).find_all("a"):
            tagName.append(a.get_text().strip())
        if tagName:
            insert_data["tagName"] = " | ".join(tagName)
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("article", {"id": "article_content"}))
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def sustainableplastics(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("meta", {"property": "article:published_time"}).get("content")
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("h1", {"itemprop": "headline"}).get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = soup.find("div", {"itemprop": "articleBody"}).get_text()
        if content:
            insert_data["content"] = content
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        set_urls = []
        try:
            for img in soup.find("div", {"class": "field__item article-main-image"}).find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://interplasinsights.com" + img_url
                if img_url not in set_urls:
                    contentImageUrls.append([img_url, img_url_new])
                    set_urls.append(img_url)
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        pass
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            img_cssHtml = soup.find("div", {"class": "field__item article-main-image"})
        except:
            img_cssHtml = None

        cssHtml = soup.find("div", {"itemprop": "articleBody"})
        if img_cssHtml:
            cssHtml = str(img_cssHtml) + str(cssHtml)

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = str(cssHtml)

    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def kunststoffe(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    html_json = re.findall('<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', _html, re.S)

    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    data_json = json.loads(html_json[0])
    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        # pp.pprint(data_json)
        releaseTime = data_json.get("props").get("pageProps").get("article").get("meta").get("General_Attributes").get(
            "publicationDate")
        if not releaseTime:
            releaseTime = data_json.get("props").get("pageProps").get("article").get("name").split(" ")[0]
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("title").get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = []
        for item in data_json.get("props").get("pageProps").get("article").get("body"):
            content.append(item.get("text"))
        if content:
            insert_data["content"] = "\n".join(content)
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        set_urls = []
        try:
            for img in soup.find("article", {"class": "bg-white pt-8 mb-5 clearfix relative"}).find_all("img"):
                img_url = img.get("data-src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://interplasinsights.com" + img_url
                if img_url not in set_urls:
                    contentImageUrls.append([img_url, img_url_new])
                    set_urls.append(img_url)
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        tagName = data_json.get("props").get("pageProps").get("article").get("taxonomy")[0].get("item").get("addendum")[
            0].get("value")
        if tagName:
            insert_data["tagName"] = tagName
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = soup.find("article", {"class": "bg-white pt-8 mb-5 clearfix relative"})
        except:
            cssHtml = None

        if cssHtml:
            cssHtml = str(cssHtml)
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def plasticportal(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("section", {"class": "clanok_toolbar"}).find_all("li")[0].get_text().strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("h1").get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = soup.find("section", {"class": "clanok_body"}).get_text()
        if content:
            insert_data["content"] = content
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        set_urls = []
        try:
            for img in soup.find("section", {"class": "clanok_body"}).find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                elif str(img_url).startswith("/"):
                    img_url_new = "https://www.plasticportal.eu" + img_url
                else:
                    img_url_new = "https://www.plasticportal.eu/" + img_url
                if img_url not in set_urls:
                    contentImageUrls.append([img_url, img_url_new])
                    set_urls.append(img_url)
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        pass
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("div", {"class": "detail_panel clanok_detail"}))
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def medicalplasticsnews(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("time", {"itemprop": "datePublished"}).get_text().strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("title").get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = soup.find("div", {"id": "content"}).get_text()
        if content:
            insert_data["content"] = content
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        set_urls = []
        try:
            for img in soup.find("div", {"id": "content"}).find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://interplasinsights.com" + img_url
                if img_url not in set_urls:
                    contentImageUrls.append([img_url, img_url_new])
                    set_urls.append(img_url)
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        tagName = []
        for a in soup.find("aside", {"class": "tags"}).find_all("a"):
            tagName.append(a.get_text().strip())
        if tagName:
            insert_data["tagName"] = " | ".join(tagName)
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("div", {"id": "content"}))
            cssHtml = cssHtml.replace("'", "")
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def packagingnews(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("span", {"class": "tie-date"}).get_text().strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("span", {"itemprop": "name"}).get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = soup.find("div", {"class": "content"}).find("article").get_text()
        if content:
            insert_data["content"] = content
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        set_urls = []
        try:
            for img in soup.find("div", {"class": "content"}).find("article").find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://www.packagingnews.co.uk" + img_url
                if img_url not in set_urls:
                    contentImageUrls.append([img_url, img_url_new])
                    set_urls.append(img_url)
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        pass
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("div", {"class": "content"}).find("article"))
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def packworld(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("div", {"class": "page-dates"}).get_text().strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime.replace("\n", " "))
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("h1").get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = soup.find("div", {"class": "page-contents"}).get_text()
        if content:
            insert_data["content"] = content
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        set_urls = []
        try:
            for img in soup.find("div", {"class": "page-contents"}).find_all("img"):
                img_url = img.get("src")
                if "image/gif" in img_url: pass
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://www.packworld.com" + img_url
                if img_url not in set_urls:
                    contentImageUrls.append([img_url, img_url_new])
                    set_urls.append(img_url)
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        pass
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("div", {"class": "page-contents"}))
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def packagingdigest(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("span", {"class": "date"}).get_text().strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime)
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("span", {"class": "bread-currentpage"}).get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = []
        for p in soup.find("div", {"itemprop": "articleBody"}).find_all("p"):
            _text = p.get_text()
            if _text:
                content.append(_text.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("\xa0", " "))
        if content:
            insert_data["content"] = "\n\n".join(content)
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        try:
            for img in soup.find("div", {"class": "big-article__image"}).find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://www.plasticstoday.com" + img_url
                contentImageUrls.append([img_url, img_url_new])
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        tagName = []
        for a in soup.find("div", {"class": "secondary-tags"}).find_all("a"):
            tagName.append(a.get_text().strip())
        if tagName:
            insert_data["tagName"] = " | ".join(tagName)
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            img_cssHtml = soup.find("div", {"class": "big-article__top"})
        except:
            img_cssHtml = None

        cssHtml = soup.find("div", {"itemprop": "articleBody"})
        if img_cssHtml:
            cssHtml = str(img_cssHtml) + str(cssHtml)

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def packagingeurope(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("meta", {"name": "pubdate"}).get("content").strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime)
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("h1").get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = []
        for p in soup.find("div", {"class": "articleContent"}).find_all("p"):
            _text = p.get_text()
            if _text:
                content.append(_text.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("\xa0", " "))
        if content:
            insert_data["content"] = "\n\n".join(content)
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        try:
            for img in soup.find("div", {"class": "articleContent"}).find_all("img"):
                img_url = img.get("data-src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://d2wrwj382xgrci.cloudfront.net" + img_url
                contentImageUrls.append([img_url, img_url_new])
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        pass
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("div", {"class": "articleContent"}))
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def ptonline(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("div", {"class": "sm"}).find("time").get_text().strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime)
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("title").get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = []
        for p in soup.find("div", {"class": "article-body col-xs-12 col-md-12 col-lg-12 col-xl-9 xl:p-l-1"}).find_all(
                "p"):
            _text = p.get_text()
            if _text:
                content.append(_text.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("\xa0", " "))
        if content:
            insert_data["content"] = "\n\n".join(content)
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        try:
            for img in soup.find("div",
                                 {"class": "article-body col-xs-12 col-md-12 col-lg-12 col-xl-9 xl:p-l-1"}).find_all(
                    "img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://d2n4wb9orp1vta.cloudfront.net" + img_url
                contentImageUrls.append([img_url, img_url_new])
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        pass
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("div", {"class": "article-body col-xs-12 col-md-12 col-lg-12 col-xl-9 xl:p-l-1"}))
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def phys(info, _html):
    soup = BeautifulSoup(_html, "lxml")
    insert_data = copy.deepcopy(format_data)
    insert_data["id"] = info["hash_key"]
    insert_data["type"] = 2
    insert_data["channelName"] = "国外资讯"
    insert_data["source"] = info["platform"]
    insert_data["sourceLink"] = info["url"]
    insert_data["createTime"] = info["spider_time"]
    insert_data["updateTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    replace_list = []

    # releaseTime    time.strftime("%Y-%m-%d %H:%M", time.localtime(upload_time))
    try:
        releaseTime = soup.find("p", {"class": "text-uppercase text-low"}).get_text().strip()
        if releaseTime:
            new_releaseTime = format_time(releaseTime.split("\n")[0])
            insert_data["releaseTime"] = new_releaseTime
    except Exception as error:
        log_err(error)

    # titleName
    try:
        titleName = soup.find("h1").get_text().strip()
        if titleName:
            insert_data["titleName"] = titleName
    except Exception as error:
        log_err(error)

    # content
    try:
        content = []
        for p in soup.find("div", {"class": "mt-4 article-main"}).find_all("p"):
            _text = p.get_text()
            if _text:
                content.append(_text.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("\xa0", " "))
        if content:
            insert_data["content"] = "\n\n".join(content)
    except Exception as error:
        log_err(error)

    # contentImageUrls
    try:
        contentImageUrls = []
        try:
            for img in soup.find("div", {"class": "mt-4 article-main"}).find_all("img"):
                img_url = img.get("src")
                if str(img_url).startswith("http"):
                    img_url_new = img_url
                else:
                    img_url_new = "https://d2n4wb9orp1vta.cloudfront.net" + img_url
                contentImageUrls.append([img_url, img_url_new])
        except:
            pass
        if contentImageUrls:
            replace_list = command_thread(info["platform"], contentImageUrls)
    except Exception as error:
        log_err(error)

    # tagName
    try:
        pass
    except Exception as error:
        log_err(error)

    # cssHtml
    try:
        try:
            cssHtml = str(soup.find("article", {"class": "news-article"}))
        except:
            cssHtml = None

        if cssHtml:
            if replace_list:
                images_back = []
                for replace_info in replace_list:
                    font_url, back_url = replace_info[0], replace_info[2]
                    cssHtml = cssHtml.replace(font_url, back_url)
                    images_back.append(back_url)
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
                insert_data["contentImageUrls"] = images_back
            else:
                cssHtml = str(cssHtml)
                cssHtml = cssHtml.replace("'", "")
                insert_data["cssHtml"] = cssHtml
    except Exception as error:
        log_err(error)

    UploadArticle(insert_data)
    # pp.pprint(insert_data)


def format_time(_time):
    _time = _time.strip()
    formatTime = "*****"

    # 2023-03-31
    try:
        if re.match("\d+-\d+-\d+", _time):
            formatTime = _time + " 00:00:00"
    except:
        pass
    # Jun 05, 2023
    try:
        if re.match("\w+ \d+, \w+", _time):
            _m, _d, _y = _time.replace(",", "").split(" ")[0], _time.replace(",", "").split(" ")[1], \
                         _time.replace(",", "").split(" ")[2]
            _m = str(_m[:3]).title()
            timeArray = time.strptime("{} {}, {}".format(_m, _d, _y), "%b %d, %Y")
            _timestamp = time.mktime(timeArray)
            formatTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(_timestamp))
    except:
        pass

    # 1 June 2023 09:20
    try:
        if re.match("\d+ \w+ \d+ \d+:\d+", _time):
            _d, _m, _y = _time.replace(",", "").split(" ")[0], _time.replace(",", "").split(" ")[1], \
                         _time.replace(",", "").split(" ")[2]
            _m = str(_m[:3]).title()
            ymd_text = "{} {}, {}".format(_m, _d, _y)
            timeArray = time.strptime(ymd_text, "%b %d, %Y")
            _timestamp = time.mktime(timeArray)

            hms_text = _time.split(" ")[-1]
            if len(hms_text) == 5:
                hms_text += ":00"
            formatTime = str(time.strftime("%Y-%m-%d", time.localtime(_timestamp))) + " " + hms_text
    except:
        pass

    # 1 June 2023
    try:
        if re.match("\d+ \w+ \d+", _time):
            _d, _m, _y = _time.replace(",", "").split(" ")[0], _time.replace(",", "").split(" ")[1], \
                         _time.replace(",", "").split(" ")[2]
            _m = str(_m[:3]).title()
            ymd_text = "{} {}, {}".format(_m, _d, _y)
            timeArray = time.strptime(ymd_text, "%b %d, %Y")
            _timestamp = time.mktime(timeArray)

            hms_text = "00:00:00"
            formatTime = str(time.strftime("%Y-%m-%d", time.localtime(_timestamp))) + " " + hms_text
    except:
        pass

    # June 05, 2023 11:50 PM
    try:
        if re.match("\w+ \d+, \w+ \d+:\d+ AM", _time) or re.match("\w+ \d+, \w+ \d+:\d+ PM", _time):
            _m, _d, _y = _time.replace(",", "").split(" ")[0], _time.replace(",", "").split(" ")[1], \
                         _time.replace(",", "").split(" ")[2]
            _m = str(_m[:3]).title()
            ymd_text = "{} {}, {}".format(_m, _d, _y)
            timeArray = time.strptime(ymd_text, "%b %d, %Y")
            _timestamp = time.mktime(timeArray)
            ymd = str(time.strftime("%Y-%m-%d", time.localtime(_timestamp)))
            hms_text = " ".join(_time.split(" ")[-2:])
            hms = str(datetime.strptime(hms_text, '%I:%M %p')).split(" ")[-1]
            formatTime = "{} {}".format(ymd, hms)
    except:
        pass

    # 2023-05-30T16:49:45
    try:
        if re.match("\d+-\d+-\d+T\d+:\d+:\d+", _time):
            formatTime = _time.replace("T", " ")
    except:
        pass

    # 23.05.2023
    try:
        if re.match("\d+.\d+.\d+", _time):
            _d, _m, _y = _time.split(".")[0], _time.split(".")[1], _time.split(".")[2]
            if int(_d) < 10 and len(_d) == 1:
                _d = "0" + _d
            if int(_m) < 10 and len(_m) == 1:
                _m = "0" + _m
            formatTime = "{}-{}-{} 00:00:00".format(_y, _m, _d)
    except:
        pass

    # 6/5/2023
    try:
        if re.match("\d+/\d+/\d+", _time):
            _d, _m, _y = _time.split("/")[0], _time.split("/")[1], _time.split("/")[2]
            if int(_d) < 10 and len(_d) == 1:
                _d = "0" + _d
            if int(_m) < 10 and len(_m) == 1:
                _m = "0" + _m
            formatTime = "{}-{}-{} 00:00:00".format(_y, _m, _d)
    except:
        pass

    # Thu, 05 Jan 2023 09:27 GMT
    try:
        if re.match("\w+, \d+ \w+ \d+ \d+:\d+ GMT", _time):
            _time = _time.split(", ")[1]
            _d, _m, _y = _time.replace(",", "").split(" ")[0], _time.replace(",", "").split(" ")[1], \
                         _time.replace(",", "").split(" ")[2]
            _m = str(_m[:3]).title()
            ymd_text = "{} {}, {}".format(_m, _d, _y)
            timeArray = time.strptime(ymd_text, "%b %d, %Y")
            _timestamp = time.mktime(timeArray)

            hms_text = _time.split(" ")[-2]
            if len(hms_text) == 5:
                hms_text += ":00"
            formatTime = str(time.strftime("%Y-%m-%d", time.localtime(_timestamp))) + " " + hms_text
    except:
        pass

    # 2023-06-05T17:03:00+00:00
    try:
        if re.match("\d+-\d+-\d+T\d+:\d+:\d+\+\d+:\d+", _time):
            formatTime = _time.split("+")[0].replace("T", " ")
    except:
        pass

    # 2023-06-05T09:31:25 00:00:00
    try:
        if re.match("\d+-\d+-\d+T\d+:\d+:\d+ \d+:\d+:\d+", _time):
            formatTime = _time.split(" ")[0].replace("T", " ")
    except:
        pass

    # 2022-07-04T12:28:40.821312
    try:
        if re.match("\d+-\d+-\d+T\d+:\d+:\d+\.\d+", _time):
            formatTime = _time.split(".")[0].replace("T", " ")
    except:
        pass

    if formatTime == "*****":
        return _time
    return formatTime

def run_articles():
    for _info in url_coll.find({"status": None}):
        get_article(_info)
        # break

if __name__ == '__main__':
    run_articles()

