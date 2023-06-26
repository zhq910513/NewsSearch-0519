from spiders.search_news import run_urls
from spiders.search_articles import run_articles


def run():
    run_urls()
    run_articles()


if __name__ == '__main__':
    run()