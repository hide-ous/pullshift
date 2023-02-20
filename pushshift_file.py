import io
import json
import queue
from abc import ABC, abstractmethod
from time import sleep
from copy import deepcopy
from multiprocessing import Queue, RLock, Process, Barrier

from pushshift_pages import download, extract_archive_links

import zstandard as zstd


def decompress(fh):
    reader = zstd.ZstdDecompressor(max_window_size=2147483648).stream_reader(fh)
    yield from io.TextIOWrapper(reader, encoding='utf-8')


class Reader(ABC, Process):
    def __init__(self, out_queue: Queue, barrier: Barrier, fpath: str):
        super(Reader, self).__init__()
        self.barrier = barrier
        self.out_queue = out_queue
        self.fpath = fpath

    @abstractmethod
    def read(self, **args):
        pass

    def forward_item(self, item):
        self.out_queue.put(item)

    def run(self):
        self.read()
        self.close_reader()
        self.close()

    def close_reader(self):
        print('closing reader')
        try:
            self.barrier.wait()
            self.out_queue.close()
            print('closed reader')
        except Exception as e:
            print('error in closing the barrier')
            print(e)

            pass

class JsonlFileReader(Reader):
    def read(self, **args):
        with open(self.fpath) as f:
            for l in f:
                self.forward_item(json.loads(l))


class ZstdFileReader(JsonlFileReader):
    def read(self, **args):
        with open(self.fpath, 'rb') as fh:
            for line in map(json.loads, decompress(fh)):
            # for line in decompress(fh):
                # print(line)
                # if line.strip():
                self.forward_item(line)
        print('done reading')


class Writer(Process, ABC):
    def __init__(self, in_queue: Queue, fpath: str):
        super(Writer, self).__init__()
        self.fpath = fpath
        self.in_queue = in_queue

    @abstractmethod
    def write(self, item, **args):
        pass

    @abstractmethod
    def close_writer(self):
        pass

    # def collect_items(self):
    #     try:
    #         for item in self.in_queue.get():
    #             self.write(item)
    #     except ValueError as e:  # queue closed
    #         pass

    def collect_items(self):
        done=False
        try:
            while not done:
                item = self.in_queue.get(timeout=1)
                self.write(item)
        except queue.Empty as e:  # queue closed
            print('val error', e)

    def run(self):
        self.collect_items()
        self.close_writer()
        self.close()

class JsonlFileWriter(Writer):

    def __init__(self, in_queue: Queue, fpath: str):
        super(JsonlFileWriter, self).__init__(in_queue, fpath)

    def write(self, item, **args):
        self.fhandle.write(json.dumps(item) + '\n')

    def close_writer(self):
        print('closing writer')
        self.fhandle.close()
        print('closed writer')

    def collect_items(self):
        self.fhandle = open(self.fpath, 'w+', encoding='utf8')
        super(JsonlFileWriter, self).collect_items()
        print('items collected')


class MultiJsonlFileWriter(JsonlFileWriter):
    def __init__(self, in_queue: Queue, fpaths_and_filters: dict, multiple_writes_per_item: bool = False):
        self.multiple_writes_per_item = multiple_writes_per_item
        self.fpaths_and_filters = deepcopy(fpaths_and_filters)
        self.fhandles = {k: open(k, 'a+', encoding="utf8") for k in fpaths_and_filters}
        self.in_queue = in_queue

    def write(self, item, **args):
        for k, f in self.fpaths_and_filters.items():
            if f(item):
                self.fhandles[k].write(json.dumps + '\n')
                if not self.multiple_writes_per_item:
                    break

    def close_writer(self):
        for fh in self.fhandles.values():
            fh.close()


def main():
    fin = 'E:\\pushshift\\RS_2005-12.zst'
    fin2 = 'E:\\pushshift\\RS_2006-01.zst'
    fout = 'E:\\pushshift\\RS_2005-12.jsonl'
    q= Queue()
    b=Barrier(2)
    rp=ZstdFileReader(out_queue=q, barrier=b, fpath=fin)
    rp2=ZstdFileReader(out_queue=q, barrier=b, fpath=fin2)
    wp=JsonlFileWriter(in_queue=q, fpath=fout)
    # rp=Process(target=r.read)
    # wp = Process(target=w.collect_items)
    rp.start()
    rp2.start()
    wp.start()

    rp.join()
    rp2.join()
    wp.join()

    print('finished!')


if __name__ == '__main__':
    # link = 'https://files.pushshift.io/reddit/submissions/RS_2005-06.zst'
    # download(link, 'test.zst')
    # with open('test.zst', 'rb') as fh:
    #     for line in map(json.loads, decompress(fh)):
    #         print(line)
    main()