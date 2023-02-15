import io
import json

from pushshift_pages import download, extract_archive_links

import zstandard as zstd


def decompress(fh):
    reader = zstd.ZstdDecompressor(max_window_size=2147483648).stream_reader(fh)
    yield from io.TextIOWrapper(reader, encoding='utf-8')


if __name__ == '__main__':

    link = 'https://files.pushshift.io/reddit/submissions/RS_2005-06.zst'
    download(link, 'test.zst')
    with open('test.zst', 'rb') as fh:
        for line in map(json.loads, decompress(fh)):
            print(line)
