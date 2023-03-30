import gzip
import io
import json
import os
import queue
from abc import ABC, abstractmethod
from gzip import GzipFile
from json import JSONDecodeError
from copy import deepcopy
from multiprocessing import Queue, Process, Event, Manager, Pool
from typing import Callable, Optional
# from multiprocessing.dummy import Pool
from tqdm import tqdm

import zstandard as zstd


def decompress(fh):
    reader = zstd.ZstdDecompressor(max_window_size=2147483648).stream_reader(fh)
    yield from io.TextIOWrapper(reader, encoding='utf-8')


class Reader(ABC, Process):
    def __init__(self, out_queue: Queue, fpath: str):
        super(Reader, self).__init__()
        self.stop_event = Event()
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
            self.stop_event.set()
            # self.out_queue.close()
            if self.fpath:
                print(f'closed {self.fpath}')
        except Exception as e:
            print('error in closing the barrier')
            print(e)
    def join(self, timeout: Optional[float] = None) -> None:
        self.stop_event.wait(timeout=timeout)




class JsonlFileReader(Reader):
    def read(self, **args):
        with open(self.fpath) as f:
            for line in f:
                if (line is not None) and len(line.strip()):
                    self.forward_item(json.loads(line))


class ZstdFileReader(JsonlFileReader):
    def read(self, **args):
        print(f"reading {self.fpath}")
        with open(self.fpath, 'rb') as fh:
            for line in decompress(fh):
                try:
                    self.forward_item(json.loads(line))
                except JSONDecodeError as e:
                    print(f"error in {self.fpath}")
                    print(e)

def zstd_read(args):
    fpath, q = args
    print(f'processing {fpath}')
    with open(fpath, 'rb') as fh:
        for line in decompress(fh):
            try:
                q.put(json.loads(line))
            except JSONDecodeError as e:
                print(f"error in {fpath}")
                print(e)

class ZstdFileParallelReader(ZstdFileReader):
    def __init__(self, out_queue: Queue, fpaths: list[str], nthreads: int = 10, fpath: str = None):
        super(ZstdFileParallelReader, self).__init__(out_queue, fpath)
        self.fpaths = fpaths
        self.nthreads=nthreads

    def read(self, **args):
        with Pool(self.nthreads) as pool:
            args = [(fpath, self.out_queue) for fpath in self.fpaths]
            _ = pool.map(zstd_read, args)


class Writer(Process, ABC):
    def __init__(self, in_queue: Queue, fpath: str):
        super(Writer, self).__init__()
        self.stop_event = Event()
        self.fpath = fpath
        self.in_queue = in_queue

    @abstractmethod
    def write(self, item, **args):
        pass

    @abstractmethod
    def close_writer(self):
        pass

    def collect_items(self):
        done = False
        with tqdm() as pbar:
            while not done:
                try:
                    item = self.in_queue.get(timeout=1)
                    self.write(item)
                    pbar.update(1)
                except queue.Empty as e:  # queue closed or timeout in get
                    if self.stop_event.is_set():
                        done = True
                    else:
                        pass

    def run(self):
        self.collect_items()
        self.close_writer()
        self.close()

    def stop(self): # signal the writer that upstream processing finished
        self.stop_event.set()

class DummyWriter(Writer):
    def close_writer(self):
        pass
    def write(self, item, **args):
        pass

class JsonlFileWriter(Writer):

    def __init__(self, in_queue: Queue, fpath: str):
        super(JsonlFileWriter, self).__init__(in_queue, fpath)

    def write(self, item, **args):
        if self.fpath is not None:
            self.fhandle = open(self.fpath, 'w+', encoding='utf8')
        self.fhandle.write(json.dumps(item, sort_keys=True) + '\n')

    def close_writer(self):
        print('closing writer')
        self.fhandle.close()
        print('closed writer')

    # def collect_items(self):
    #     super(JsonlFileWriter, self).collect_items()
    #     print('items collected')


class MultiJsonlFileWriter(JsonlFileWriter):
    def __init__(self, in_queue: Queue, fpaths_func: Callable[[dict], str], fpath: str = None):
        super(MultiJsonlFileWriter, self).__init__(in_queue, fpath)
        # self.stop_event = Event()
        self.fpaths_func = fpaths_func
        # self.multiple_writes_per_item = multiple_writes_per_item
        # self.fhandles = {k: open(k, 'a+', encoding="utf8") for k in self.fpaths_and_filters}
        self.fhandles = dict()
        # self.in_queue = in_queue

    def open(self, fpath):
        if fpath not in self.fhandles:
            print(fpath)
            self.fhandles[fpath] = open(fpath, 'a+', encoding="utf8")
        self.fhandle = self.fhandles[fpath]

    def write(self, item, **args):
        fpaths = self.fpaths_func(item)
        for fpath in fpaths:
            self.open(fpath)
            super(MultiJsonlFileWriter, self).write(item)

    def close_writer(self):
        for fh in self.fhandles.values():
            fh.close()

class MultiGzipJsonFileWriter(MultiJsonlFileWriter):
    def open(self, fpath):
        if fpath not in self.fhandles:
            self.fhandles[fpath] = gzip.open(fpath, 'wt+', encoding="utf8")
        self.fhandle = self.fhandles[fpath]


def fout_func(item):
    return [f"RC_subreddit\\RC_{item['subreddit']}.jsonl.gz"]


def main():
    fin = 'E:\\pushshift\\RS_2006-12.zst'
    fin2 = 'E:\\pushshift\\RS_2006-01.zst'
    fout = 'E:\\pushshift\\RS_test.jsonl'
    # q = Queue()
    #
    # rp = ZstdFileReader(out_queue=q, fpath=fin)
    # rp2 = ZstdFileReader(out_queue=q, fpath=fin2)
    # # wp = JsonlFileWriter(in_queue=q, fpath=fout)
    # wp=DummyWriter(in_queue=q, fpath=fout)
    # rp.start()
    # rp2.start()
    # wp.start()
    #
    # rp.join() # wait for all readers to finish
    # rp2.join()
    # wp.stop()
    # wp.join()
    #
    # print('finished with two processes')
    #
    # fpaths = [f'E:\\pushshift\\RS_2009-{m:02d}.zst' for m in range(1, 13)]
    # q = Queue()
    # readers = [ZstdFileReader(out_queue=q, fpath=fin) for fin in fpaths]
    # wp=DummyWriter(in_queue=q, fpath=fout)
    # for reader in readers:
    #     reader.start()
    # wp.start()
    # for reader in readers:
    #     reader.join()
    # wp.stop()
    # wp.join()
    # print('finished with multiple processes')

    fin = 'C:\\Users\\matti\\Downloads\\reddit\\comments\\RC_2006-12.zst'
    os.makedirs('RC_subreddit', exist_ok=True)
    q = Queue()
    reader = ZstdFileReader(out_queue=q, fpath=fin)
    wp=MultiGzipJsonFileWriter(in_queue=q, fpaths_func=fout_func)
    reader.start()
    wp.start()
    reader.join()
    wp.stop()
    wp.join()
    print('finished with rotating writes')


    # fout = 'E:\\pushshift\\RS_test.jsonl'
    # m = Manager()
    # q = m.Queue()
    # rpp=ZstdFileParallelReader(out_queue=q, fpaths=[f'E:\\pushshift\\RS_2009-{m:02d}.zst' for m in range(1, 13)], nthreads=20)
    # # wp = JsonlFileWriter(in_queue=q, fpath=fout)
    # wp=DummyWriter(in_queue=q, fpath=fout)
    # rpp.start()
    # wp.start()
    #
    # rpp.join()
    # wp.stop()
    # wp.join()
    #
    # print('finished with pooled reader')

if __name__ == '__main__':

    main()
