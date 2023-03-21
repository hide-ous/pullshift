import os
import queue
import sys
from abc import ABC, abstractmethod
from multiprocessing import Process, Queue, Barrier, Event

from preprocess_text import doc2token
from pushshift_file import JsonlFileWriter, ZstdFileSequenceReader

from dotenv import load_dotenv

class Processor(ABC, Process):
    def __init__(self, qin: Queue, qout: Queue, barrier: Barrier, stop_event_out: Event, stop_event_in: Event):
        super(Processor, self).__init__()
        self.stop_event_in = stop_event_in
        self.stop_event_out = stop_event_out
        self.barrier = barrier
        self.qin = qin
        self.qout = qout

    @abstractmethod
    def process_item(self, item):
        pass

    def process_items(self):
        done = False
        while not done:
            try:
                item = self.qin.get(timeout=1)
                processed = self.process_item(item)
                if processed is not None:
                    self.qout.put(processed)
            except queue.Empty as e:  # queue closed
                if self.stop_event_in.is_set():
                    done = True
                else:
                    # print('val error in process', e)
                    pass
        self.close_processor()

    def close_processor(self):
        try:
            self.barrier.wait()
            self.stop_event_out.set()
            self.qout.close()
        except Exception as e:
            print('error in closing the barrier in processor')
            print(e)

        self.close()

    def run(self) -> None:
        self.process_items()


def normalize_text(item: dict):
    contribution_type = "comment"
    if 'is_self' in item:
        if item['is_self']:
            contribution_type = "selftext_submission"
        else:
            contribution_type = "link_submission"
    item['contribution_type'] = contribution_type

    if contribution_type == 'comment':
        item['text'] = item.pop['body']
    elif contribution_type == 'selftext_submission':
        item['text'] = "\n".join([item.pop('title'), item.pop('selftext')])
    elif contribution_type == 'link_submission':
        item['text'] = item.pop('title')
    # title if link sub; title+self if self sub; text if comment

    if contribution_type in set(['selftext_submission', 'link_submission']):
        item['fullname'] = "t3_" + item.pop('id')
    elif contribution_type == 'comment':
        item['fullname'] = "t1_" + item.pop('id')

    if contribution_type in set(['selftext_submission', 'link_submission']):
        item['parent_fullname'] = None
        item['link_fullname'] = item['fullname']
    elif contribution_type == 'comment':
        item['parent_fullname'] = item.pop('parent_id')
        item['link_fullname'] = item.pop('link_id')
    return item


def keep_fields(item: dict, fields: set[str]):
    return {k: v for k, v in item.items() if k in fields}


def keep_contribution(item: dict, fields_and_values: dict[str:set[str]]):
    for field, values in fields_and_values.items():
        if (field not in item) or (item[field] not in values):
            return None
    return item


def clean_text(item, text_field='text', remove_punct=True, remove_digit=True, remove_stops=True, remove_pron=True,
               lemmatize=True, lowercase=True):
    item[text_field] = " ".join(doc2token(item[text_field], remove_punct=remove_punct,
                                          remove_digit=remove_digit,
                                          remove_stops=remove_stops,
                                          remove_pron=remove_pron,
                                          lemmatize=lemmatize,
                                          lowercase=lowercase, ))
    return item


class Pipeline(Processor):
    def __init__(self, qin: Queue, qout: Queue, barrier: Barrier, stop_event_out: Event, stop_event_in: Event,
                 funcs: list):
        super(Pipeline, self).__init__(qin=qin, qout=qout, barrier=barrier, stop_event_out=stop_event_out,
                                       stop_event_in=stop_event_in)
        self.funcs = funcs

    def process_item(self, item):
        for (func, args) in self.funcs:
            if item is None:
                return None
            else:
                item = func(item, **args)
        # print(item)
        return item


def main():
    load_dotenv()
    base_path = os.environ['base_path']

    fname = os.environ['fname']
    subs = list()
    with open(fname, encoding='utf8') as f:
        for l in f:
            subs.append(l.split('\t')[0])
    subs=set(subs)
    # fins = [os.path.join(base_path, fname) for fname in os.listdir(base_path) if fname.endswith('.zst')
    #         and ('RS_' in fname) and ('2018' not in fname)]
    fins = [os.path.join(base_path, f"RS_{year}-{month:02}.zst") for year in range(2005, 2019) for month in range(1, 13)
            if not ((year==2005) and (month != 12))]
    # for fin in fins:
    #     print(fin)
    print(fins)
    fout = os.environ['fout']

    # print(base_path, fname, fout)
    # sys.exit()
    q_to_process = Queue(maxsize=10000)
    q_from_process = Queue()
    stop_writer = Event()
    stop_process = Event()
    # reader_barrier = Barrier(len(fins))
    reader = ZstdFileSequenceReader(out_queue=q_to_process, barrier=Barrier(1), fpaths=fins, stop_event=stop_process)
    # readers = [ZstdFileReader(out_queue=q_to_process, barrier=reader_barrier, fpath=fin) for fin in fins]
    # rp = ZstdFileReader(out_queue=q_to_process, barrier=Barrier(1), fpath=fin)

    n_processors = 10
    processor_barrier = Barrier(n_processors)
    # funcs = [
    #     (normalize_text, dict()),
    #     (keep_fields, {'fields': set(['subreddit', 'text'])}),
    #     (clean_text, dict(text_field='text', remove_pron=False, lemmatize=False))
    # ]
    funcs = [ # extract and normalize titles of submissions only
        (keep_contribution, dict(fields_and_values={"subreddit":subs})),
        (keep_fields, {'fields': set(['id', 'subreddit', 'title'])}),
        (clean_text, dict(text_field='title',
                          remove_punct=True, remove_digit=True, remove_stops=False, remove_pron=False,
                          lemmatize=False, lowercase=True
                          ))
    ]
    processors = [Pipeline(q_to_process, q_from_process, processor_barrier, stop_writer, stop_process, funcs=funcs)
                  for _ in range(n_processors)]
    # pp = Pipeline(q_to_process, q_from_process, Barrier(1), funcs=[
    #     (normalize_text, dict()),
    #     (keep_fields, {'fields': set(['subreddit', 'text'])}),
    #     (clean_text, dict(text_field='text'))
    #                                                                ])
    wp = JsonlFileWriter(in_queue=q_from_process, fpath=fout, stop_event=stop_writer)

    # for reader in readers:
    reader.start()
    for processor in processors:
        processor.start()

    # rp.start()
    # pp.start()
    wp.start()

    # for reader in readers:
    reader.join()
    for processor in processors:
        processor.join()
    # rp.join()
    # pp.join()
    wp.join()

    print('finished!')


if __name__ == '__main__':
    main()
