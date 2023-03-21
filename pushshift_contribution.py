import os
import queue
import sys
from abc import ABC, abstractmethod
from multiprocessing import Process, Queue, Barrier, Event, Manager

from preprocess_text import doc2token
from pushshift_file import JsonlFileWriter, ZstdFileParallelReader

from dotenv import load_dotenv

class Processor(ABC, Process):
    def __init__(self, qin: Queue, qout: Queue):
        super(Processor, self).__init__()
        self.stop_event_in = Event()
        self.stop_event_out = Event()
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
                    pass

    def close_processor(self):
        self.stop_event_out.set()
        # try:
        #     self.barrier.wait()
        #     self.stop_event_out.set()
        #     self.qout.close()
        # except Exception as e:
        #     print('error in closing the barrier in processor')
        #     print(e)


    def run(self) -> None:
        self.process_items()
        self.close_processor()
        self.close()

    def stop(self):
        self.stop_event_in.set()

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
    def __init__(self, qin: Queue, qout: Queue, funcs: list):
        super(Pipeline, self).__init__(qin=qin, qout=qout)
        self.funcs = funcs

    def process_item(self, item):
        for (func, args) in self.funcs:
            if item is None:
                return None
            else:
                item = func(item, **args)
        return item


def main():
    load_dotenv()
    base_path = os.environ['base_path']

    # read allowed subreddits
    subreddit_fname = os.environ['subreddit_fname']
    subs = list()
    with open(subreddit_fname, encoding='utf8') as f:
        for l in f:
            subs.append(l.split('\t')[0])
    subs=set(subs)

    # set up readers
    fins = [os.path.join(base_path, f"RS_{year}-{month:02}.zst") for year in range(2005, 2007) for month in range(1, 13)
            if not ((year==2005) and (month != 12))]
    m = Manager()
    q_to_process = m.Queue()
    reader = ZstdFileParallelReader(out_queue=q_to_process, fpaths=fins, nthreads=20)

    # set up processors
    q_from_process = m.Queue()
    n_processors = 10
    funcs = [ # extract and normalize titles of submissions only
        (keep_contribution, dict(fields_and_values={"subreddit":subs})),
        (keep_fields, {'fields': set(['id', 'subreddit', 'title'])}),
        (clean_text, dict(text_field='title',
                          remove_punct=True, remove_digit=True, remove_stops=False, remove_pron=False,
                          lemmatize=False, lowercase=True
                          ))
    ]
    processors = [Pipeline(q_to_process, q_from_process, funcs=funcs)
                  for _ in range(n_processors)]

    # set up writer
    fout = os.environ['fout']
    wp = JsonlFileWriter(in_queue=q_from_process, fpath=fout)

    # start everything
    reader.start()
    for processor in processors:
        processor.start()
    wp.start()

    # wait for readers to stop
    reader.join()
    for processor in processors:
        processor.stop()

    # wait for processors to stop
    for processor in processors:
        processor.join()
    wp.stop()
    wp.join()

    print('finished!')


if __name__ == '__main__':
    main()
