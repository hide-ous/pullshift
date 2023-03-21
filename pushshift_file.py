import io
import json
import os
import queue
from abc import ABC, abstractmethod
from json import JSONDecodeError
from copy import deepcopy
from multiprocessing import Queue, RLock, Process, Barrier, Event, Pool, Manager

from tqdm import tqdm

from pushshift_pages import download, extract_archive_links

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
            print('closed reader')
        except Exception as e:
            print('error in closing the barrier')
            print(e)
    def join(self, timeout: float | None = None) -> None:
        self.stop_event.wait(timeout=timeout)




class JsonlFileReader(Reader):
    def read(self, **args):
        with open(self.fpath) as f:
            for line in f:
                if (line is not None) and len(line.strip()):
                    self.forward_item(json.loads(line))


class ZstdFileReader(JsonlFileReader):
    def read(self, **args):
        print("reading")
        with open(self.fpath, 'rb') as fh:
            for line in decompress(fh):
                try:
                    self.forward_item(json.loads(line))
                except JSONDecodeError as e:
                    print(f"error in {self.fpath}")
                    print(e)


class ZstdFileSequenceReader(ZstdFileReader):
    def __init__(self, out_queue: Queue, barrier: Barrier, fpaths: list[str], stop_event: Event, fpath: str = None):
        super(ZstdFileSequenceReader, self).__init__(out_queue, barrier, fpath, stop_event)
        self.fpaths = fpaths

    def read(self, **args):
        size_file = list(zip(map(lambda f: os.stat(f).st_size, self.fpaths), self.fpaths))
        NORM_FACTOR = 2**20
        total_size = sum(size for size, _ in size_file)
        size_file = [(size, fpath) for size, fpath in size_file]
        # size_file = [(size / (total_size * len(size_file)
        #                       ), fpath) for size, fpath in size_file]
        processed = 0
        with tqdm(size_file, total=total_size//NORM_FACTOR, unit="MB") as pbar:
            for size, fpath in pbar: # the total in terms of file size is not recognized
                pbar.set_description(f"reading {fpath}")
                self.fpath=fpath
                super(ZstdFileSequenceReader, self).read()
                old_processed = processed
                processed +=size
                pbar.update((processed-old_processed)//NORM_FACTOR)
                # with open(fpath, 'rb') as fh:
                #     for line in map(json.loads, decompress(fh)):
                #         self.forward_item(line)
        print('done reading')

# def spawn_zstd_file_reader(args):
#     fpath, q = args
#     rp = ZstdFileReader(out_queue=q, fpath=fpath)
#     rp.daemon = False
#     rp.start()
#     return rp

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
        # self.daemon=False

    def read(self, **args):
        with Pool(self.nthreads) as pool:
            # pool = Pool(processes=self.nthreads)
            args = [(fpath, self.out_queue) for fpath in self.fpaths]
            _ = pool.map(zstd_read, args)


            # pool.join()
    # def __getstate__(self):
    #     self_dict = self.__dict__.copy()
    #     del self_dict['pool']
    #     return self_dict
    #
    # def __setstate__(self, state):
    #     self.__dict__.update(state)
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
                    # print(item)
                    self.write(item)
                    pbar.update(1)
                except queue.Empty as e:  # queue closed
                    if self.stop_event.is_set():
                        done = True
                    else:
                        pass

    def run(self):
        self.collect_items()
        self.close_writer()
        self.close()

    def stop(self):
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
        self.fhandle.write(json.dumps(item, sort_keys=True) + '\n')

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
        self.stop_event = Event()
        self.multiple_writes_per_item = multiple_writes_per_item
        self.fpaths_and_filters = deepcopy(fpaths_and_filters)
        self.fhandles = {k: open(k, 'a+', encoding="utf8") for k in fpaths_and_filters}
        self.in_queue = in_queue

    def write(self, item, **args):
        for k, f in self.fpaths_and_filters.items():
            if f(item):
                self.fhandles[k].write(json.dumps(item, sort_keys=True) + '\n')
                if not self.multiple_writes_per_item:
                    break

    def close_writer(self):
        for fh in self.fhandles.values():
            fh.close()


def main():
    fin = 'E:\\pushshift\\RS_2006-12.zst'
    fin2 = 'E:\\pushshift\\RS_2006-01.zst'
    fout = 'E:\\pushshift\\RS_test.jsonl'
    q = Queue()

    rp = ZstdFileReader(out_queue=q, fpath=fin)
    rp2 = ZstdFileReader(out_queue=q, fpath=fin2)
    # wp = JsonlFileWriter(in_queue=q, fpath=fout)
    wp=DummyWriter(in_queue=q, fpath=fout)
    rp.start()
    rp2.start()
    wp.start()

    rp.join() # wait for all readers to finish
    rp2.join()
    wp.stop()
    wp.join()

    print('finished with two processes')

    fout = 'E:\\pushshift\\RS_test.jsonl'
    m = Manager()
    q = m.Queue()
    rpp=ZstdFileParallelReader(out_queue=q, fpaths=[f'E:\\pushshift\\RS_2009-{m:02d}.zst' for m in range(1, 13)], nthreads=20)
    # wp = JsonlFileWriter(in_queue=q, fpath=fout)
    wp=DummyWriter(in_queue=q, fpath=fout)
    rpp.start()
    wp.start()

    rpp.join()
    wp.stop()
    wp.join()

    print('finished in parallel')

if __name__ == '__main__':

    main()
