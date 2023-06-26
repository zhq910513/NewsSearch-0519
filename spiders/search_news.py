import hashlib
import pprint
import time

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from common.config import MONGO_URI, MONGO_DB

pp = pprint.PrettyPrinter(indent=4)
import requests
from bs4 import BeautifulSoup

from common.log_out import log_err

client = MongoClient(MONGO_URI)
url_coll = client[MONGO_DB]["urls"]


def insert_list(coll, _list):
    for item in _list:
        try:
            coll.insert_one(item)
        except DuplicateKeyError:
            pass


# plasticstoday   ajax
def get_plasticstoday(retry=0):
    try:
        url = "https://www.plasticstoday.com/recent"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            plasticstoday_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_plasticstoday(retry + 1)
        else:
            log_err(error)


def plasticstoday_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("div", {"class": "article-teaser__content"}):
            try:
                url = div.find("a").get("href")
                if str(url).startswith("/"):
                    url = "https://www.plasticstoday.com" + url
            except:
                url = ""

            try:
                title = div.find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "plasticstoday",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll,url_list)


# interplasinsights
def get_interplasinsights(retry=0):
    try:
        url = "https://interplasinsights.com/plastics-industry-news/latest-plastics-industry-news"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            interplasinsights_first_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_interplasinsights(retry + 1)
        else:
            log_err(error)

    try:
        url = "https://interplasinsights.com/plastic-industry-insights"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            interplasinsights_second_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_interplasinsights(retry + 1)
        else:
            log_err(error)


def interplasinsights_first_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("li", {"class": "mp-list-item"}):
            try:
                url = div.find("h3").find("a").get("href")
            except:
                url = ""

            try:
                title = div.find("h3").find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "interplasinsights",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


def interplasinsights_second_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        divs = soup.find_all("div", {"class": "mp-layout-sprocket mp-grid-12"})
        num = 0
        for num_str, div_str in enumerate(divs):
            if "LATEST INSIGHT, ANALYSIS AND OPINION" in str(div_str):
                num = num_str + 1
                break
        for div in divs[num].find_all("li", {"class": "mp-list-item"}):
            try:
                url = div.find("h3").find("a").get("href")
            except:
                url = ""

            try:
                title = div.find("h3").find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "interplasinsights",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# sustainableplastics
def get_sustainableplastics(retry=0):
    try:
        url = "https://www.sustainableplastics.com/news"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            sustainableplastics_first_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_sustainableplastics(retry + 1)
        else:
            log_err(error)

    try:
        url = "https://www.sustainableplastics.com/opinions"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            sustainableplastics_second_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_sustainableplastics(retry + 1)
        else:
            log_err(error)


def sustainableplastics_first_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("div", {"class": "feature-article-headline"}):
            try:
                url = div.find("a").get("href")
                if str(url).startswith("/news/"):
                    url = "https://www.sustainableplastics.com" + url
            except:
                url = ""

            try:
                title = div.find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "sustainableplastics",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


def sustainableplastics_second_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("div", {"data-content-type": "article"}):
            try:
                url = div.find("a").get("href")
                if str(url).startswith("/news/") or str(url).startswith("/article/"):
                    url = "https://www.sustainableplastics.com" + url
            except:
                url = ""

            try:
                title = div.find("div", {"class": "top-stories-title"}).get_text().strip()
            except:
                try:
                    title = div.find("div", {"class": "feature-article-headline"}).get_text().strip()
                except:
                    title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "sustainableplastics",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# kunststoffe
def get_kunststoffe(retry=0):
    try:
        url = "https://en.kunststoffe.de/news"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            kunststoffe_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_kunststoffe(retry + 1)
        else:
            log_err(error)


def kunststoffe_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("section", {"class": "article_teaser_content flex flex-col"}):
            try:
                url = div.find("a", {"class": "mt10"}).get("href")
                if str(url).startswith("/a/news"):
                    url = "https://en.kunststoffe.de" + url
            except:
                url = ""

            try:
                title = " ".join(url.split("/a/news/")[1].split("-")[:-1])
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "kunststoffe",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# plasticportal
def get_plasticportal(retry=0):
    try:
        url = "https://www.plasticportal.eu/en/clanky/"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            plasticportal_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_plasticportal(retry + 1)
        else:
            log_err(error)


def plasticportal_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for ul in soup.find_all("ul", {"class": "panel-list"}):
            for div in ul.find_all_next("li"):
                try:
                    url = div.find("h3").find("a").get("href")
                    if str(url).startswith("/en/"):
                        url = "https://www.plasticportal.eu" + url
                except:
                    url = ""

                try:
                    title = div.find("h3").find("a").get_text().strip()
                except:
                    title = ""

                if title and url:
                    hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                    url_list.append({
                        "hash_key": hash_key,
                        "platform": "plasticportal",
                        "source": source,
                        "title": title,
                        "url": url,
                        "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# medicalplasticsnews
def get_medicalplasticsnews(retry=0):
    try:
        url = "https://www.medicalplasticsnews.com/news"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            medicalplasticsnews_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_medicalplasticsnews(retry + 1)
        else:
            log_err(error)


def medicalplasticsnews_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("div", {"class": "mp-layout-sprocket mp-grid-12"}):
            if "Top Medical Plastics Stories" in str(div) \
                    or "Latest Medical Plastics News" in str(div) \
                    or "latest technology news" in str(div) \
                    or "latest Device news" in str(div) \
                    or "latest merger &amp; acquisitions news" in str(div) \
                    or "Sustainability News" in str(div):
                for li in div.find_all("li", {"class": "mp-list-item"}):
                    try:
                        url = li.find("h3").find("a").get("href")
                    except:
                        url = ""

                    try:
                        title = li.find("h3").find("a").get_text().strip()
                    except:
                        title = ""

                    if title and url:
                        hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                        url_list.append({
                            "hash_key": hash_key,
                            "platform": "medicalplasticsnews",
                            "source": source,
                            "title": title,
                            "url": url,
                            "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


## packagingnews
def get_packagingnews(retry=0):
    for url in ["https://www.packagingnews.co.uk/news/materials/flexible-plastics",
                "https://www.packagingnews.co.uk/news/materials/rigid-plastics"]:
        try:
            _headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
            }
            _session = requests.session()
            _session.headers.update(_headers)

            res = _session.get(url=url, timeout=60)
            if res.status_code == 200:
                packagingnews_parse(url, res.text)
        except Exception as error:
            if retry < 3:
                return get_packagingnews(retry + 1)
            else:
                log_err(error)


def packagingnews_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("article"):
            try:
                url = div.find("h2").find("a").get("href")
            except:
                url = ""

            try:
                title = div.find("h2").find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "packagingnews",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# packworld
def get_packworld(retry=0):
    try:
        url = "https://www.packworld.com/news"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            packworld_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_packworld(retry + 1)
        else:
            log_err(error)


def packworld_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("div", {"class": "section-feed-content-node__contents"}):
            try:
                url = div.find("h5").find("a").get("href")
                if str(url).startswith("/"):
                    url = "https://www.packworld.com" + url
            except:
                url = ""

            try:
                title = div.find("h5").find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "packworld",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# packagingdigest
def get_packagingdigest(retry=0):
    try:
        url = "https://www.packagingdigest.com/recent"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            packagingdigest_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_packagingdigest(retry + 1)
        else:
            log_err(error)


def packagingdigest_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("div", {"class": "article-teaser__content"}):
            try:
                url = div.find("div", {"class": "title"}).find("a").get("href")
                if str(url).startswith("/"):
                    url = "https://www.packagingdigest.com" + url
            except:
                url = ""

            try:
                title = div.find("div", {"class": "title"}).find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "packagingdigest",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# packagingeurope
def get_packagingeurope(retry=0):
    try:
        url = "https://packagingeurope.com/sections/news"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            packagingeurope_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_packagingeurope(retry + 1)
        else:
            log_err(error)


def packagingeurope_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("div", {"class": "subSleeve"}):
            try:
                url = div.find("h2").find("a").get("href")
            except:
                url = ""

            try:
                title = div.find("h2").find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "packagingeurope",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# ptonline
def get_ptonline(retry=0):
    for url in ["https://www.ptonline.com/zones/browse/materials", "https://www.ptonline.com/zones/browse/end-markets"]:
        try:
            _headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
            }
            _session = requests.session()
            _session.headers.update(_headers)

            res = _session.get(url=url, timeout=60)
            if res.status_code == 200:
                ptonline_parse(url, res.text)
        except Exception as error:
            if retry < 3:
                return get_ptonline(retry + 1)
            else:
                log_err(error)


def ptonline_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find("div", {"class": "content-list"}).find_all("h3"):
            try:
                url = div.find("a").get("href")
                if str(url).startswith("/"):
                    url = "https://www.ptonline.com" + url
            except:
                url = ""

            try:
                title = div.find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "ptonline",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


# phys
def get_phys(retry=0):
    try:
        url = "https://phys.org/tags/plastic+materials/"
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        _session = requests.session()
        _session.headers.update(_headers)

        res = _session.get(url=url, timeout=60)
        if res.status_code == 200:
            phys_parse(url, res.text)
    except Exception as error:
        if retry < 3:
            return get_phys(retry + 1)
        else:
            log_err(error)


def phys_parse(source, _html):
    url_list = []
    try:
        soup = BeautifulSoup(_html, "lxml")
        for div in soup.find_all("article", {"class": "sorted-article"}):
            try:
                url = div.find("h3").find("a").get("href")
            except:
                url = ""

            try:
                title = div.find("h3").find("a").get_text().strip()
            except:
                title = ""

            if title and url:
                hash_key = hashlib.md5(str(url).encode("utf8")).hexdigest()
                url_list.append({
                    "hash_key": hash_key,
                    "platform": "phys",
                    "source": source,
                    "title": title,
                    "url": url,
                    "spider_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                })
    except Exception as error:
        log_err(error)
    finally:
        if url_list:
            insert_list(url_coll, url_list)


def run_urls():
    get_plasticstoday()
    get_interplasinsights()
    get_sustainableplastics()
    get_kunststoffe()
    get_plasticportal()
    get_medicalplasticsnews()
    get_packagingnews()
    get_packworld()
    get_packagingdigest()
    get_packagingeurope()
    get_ptonline()
    get_phys()


if __name__ == '__main__':
    run_urls()
