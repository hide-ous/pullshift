import datetime
import os.path
from urllib.parse import urljoin, urlsplit

import requests
import tqdm
import urllib3
from lxml import html
import re

from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import IncompleteRead, ProtocolError

BASE_URL_PATTERN = 'https://files.pushshift.io/reddit/{contribution_type}/'
ARCHIVE_URL_PATTERN = BASE_URL_PATTERN + '{contribution_prefix}_{' \
                                         'year}_{' \
                                         'month}.zst '

CONTRIBUTION_TYPES = ('comments', 'submissions')
CONTRIBUTION_PREFIXES = ('RC', 'RS')


# see https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
# patch from https://github.com/getsentry/responses/issues/394
def patch_urllib3():
    """Set urllib3's enforce_content_length to True by default."""
    previous_init = urllib3.HTTPResponse.__init__

    def new_init(self, *args, **kwargs):
        previous_init(self, *args, enforce_content_length=True, **kwargs)

    urllib3.HTTPResponse.__init__ = new_init


patch_urllib3()


def extract_archive_links(contribution_type):
    contribution_prefix = CONTRIBUTION_PREFIXES[CONTRIBUTION_TYPES.index(
        contribution_type)]
    url = BASE_URL_PATTERN.format(contribution_type=contribution_type)
    page = requests.get(url)
    tree = html.fromstring(page.content)
    links = tree.xpath('//*[@id="container"]/table/tbody/tr/td[1]/a/@href')
    links = filter(
        lambda link: re.match(f'.*{contribution_prefix}_\d\d\d\d-\d\d.zst',
                              link), links)
    links = map(lambda link: urljoin(url, link), links)
    return list(links)


def format_url(contribution_type='comments',
               year=2014,
               month=12):
    contribution_prefix = CONTRIBUTION_PREFIXES[CONTRIBUTION_TYPES.index(
        contribution_type)]
    return ARCHIVE_URL_PATTERN.format(contribution_type=contribution_type,
                                      contribution_prefix=contribution_prefix,
                                      year=year,
                                      month=month)


def date(url):
    m = re.match(".*(?P<year>\d{4})-(?P<month>\d{2})\.zst", url)
    month, year = int(m.group('month')), int(m.group('year'))
    return datetime.date(month=month, year=year, day=1)


def stream(url):
    response = requests.get(url, stream=True)
    return response.raw


def download(url, store_path, chunk_size=1024 ** 2, overwrite=False, retry_times=3):
    if (not overwrite) and os.path.exists(store_path):
        print(f'skipping {url} as it already exists in {store_path}. Set `overwrite=True to download anyway.`')
        return
    response = requests.get(url, stream=True)

    total_size = int(response.headers['Content-Length'])
    chunk_iterator = tqdm.tqdm(response.iter_content(chunk_size=chunk_size),
                               "downloading " + url + " to " + store_path,
                               int(total_size / chunk_size))
    try:
        with open(store_path, "wb+") as out_file:
            for chunk in chunk_iterator:
                out_file.write(chunk)
    except (ChunkedEncodingError, IncompleteRead, ProtocolError) as e:
        print(e)
        if retry_times - 1 > 0:
            download(url, store_path, chunk_size=chunk_size, overwrite=True, retry_times=retry_times - 1)


def to_fname(url):
    url_path = urlsplit(url).path
    return os.path.split(url_path)[-1]


if __name__ == '__main__':
    min_date = datetime.date(2005, 12, 1)
    max_date = datetime.date(2016, 12, 1)
    base_store_path = 'E:\\pushshift'
    for contribution_type in CONTRIBUTION_TYPES:
        for url in filter(lambda url: min_date <= date(url) <= max_date,
                          extract_archive_links(contribution_type)):
            download(url, os.path.join(base_store_path, to_fname(url)))
