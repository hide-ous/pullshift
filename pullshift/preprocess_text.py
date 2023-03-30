import re
import string
import warnings
from bs4 import BeautifulSoup
from markdown import markdown
import spacy

ESCAPE_PUNCT_RE = re.compile('[%s]' % re.escape(string.punctuation))

__parser = None
spacy_stopwords = None  # depends on the parser, should `load_spacy` before use
# try:
#     spacy.require_gpu()
# except:
#     print('no gpu support for spacy')

def load_spacy(model_name='en_core_web_lg'):
    global __parser
    global spacy_stopwords
    if __parser is None:
        __parser = spacy.load(model_name)
        spacy_stopwords = __parser.Defaults.stop_words
        spacy_stopwords.update(set(string.punctuation))
        # import en_core_web_lg
        # parser = en_core_web_lg.load()


def get_parser():
    global __parser
    load_spacy()
    return __parser


def markdown_to_text(markdown_string):
    """ Converts a markdown string to plaintext """

    # md -> html -> text since BeautifulSoup can extract text cleanly
    html = markdown(markdown_string)

    # remove code snippets
    html = re.sub(r'<pre>(.*?)</pre>', ' ', html)
    html = re.sub(r'<code>(.*?)</code >', ' ', html)

    # extract text
    soup = BeautifulSoup(html, "html.parser")
    text = ' '.join(soup.findAll(text=True))

    return text


warnings.filterwarnings("ignore", category=UserWarning, module='bs4')
def unescape_html(x):
    return BeautifulSoup(x, features="html.parser").get_text().strip()
def remove_urls(x):
    return re.sub("http(.+)?(\W|$)", ' ', x)
def normalize_spaces(x):
    return re.sub("[\n\r\t ]+", ' ', x)
def escape_punct(x):
    return ESCAPE_PUNCT_RE.sub(' ', x)
def lower(x):
    return x.lower()
def substitute_subreddits(x):
    return re.sub(r"\br/", "SubredditR", x)

def preprocess_pre_tokenizing(x):
    return substitute_subreddits(
        normalize_spaces(
            remove_urls(
                markdown_to_text(
                    x))))

def preprocess_e2e(x):
    return escape_punct(
        lower(
            preprocess_pre_tokenizing
            (x)))


def doc2tokens(parsed, remove_punct=True, remove_digit=True, remove_stops=True, remove_pron=True, lemmatize=True, lowercase=True):
    tokens = list()
    for token in parsed:
        if remove_punct and token.is_punct:
            continue
        if remove_digit and token.is_digit:  # skip digits
            continue
        if remove_stops and (token.lemma_ in spacy_stopwords):  # skip stopwords
            continue
        if remove_pron and (token.lemma_ == '-PRON-'):  # skip pronouns
            continue
        else:
            token = token.lemma_ if lemmatize else token.orth_
            if lowercase:
                token = token.lower()
            if remove_punct:
                token = escape_punct(token)

            tokens.append(token.strip())
    return tokens

def text2tokens(txt, remove_punct=True, remove_digit=True, remove_stops=True, remove_pron=True, lemmatize=True, lowercase=True):
    parser = get_parser()
    parsed = parser(preprocess_pre_tokenizing(txt))
    return doc2tokens(parsed=parsed, remove_punct=remove_punct, remove_digit=remove_digit, remove_stops=remove_stops, remove_pron=remove_pron, lemmatize=lemmatize, lowercase=lowercase)

def texts2tokens(txt_stream, remove_punct=True, remove_digit=True, remove_stops=True, remove_pron=True, lemmatize=True, lowercase=True, n_process=-1):
    parser = get_parser()
    for parsed in parser.pipe(map(preprocess_pre_tokenizing, txt_stream), n_process=n_process):
        yield doc2tokens(parsed=parsed, remove_punct=remove_punct, remove_digit=remove_digit, remove_stops=remove_stops, remove_pron=remove_pron, lemmatize=lemmatize, lowercase=lowercase)

def clean_items(item_stream, text_field, remove_punct=True, remove_digit=True, remove_stops=True, remove_pron=True, lemmatize=True, lowercase=True, n_process=-1):
    parser = get_parser()
    for parsed, item in parser.pipe(((preprocess_pre_tokenizing(i[text_field]), i) for i in item_stream), n_process=n_process, as_tuples=True):
        item[text_field] = ' '.join( doc2tokens(parsed=parsed, remove_punct=remove_punct, remove_digit=remove_digit, remove_stops=remove_stops, remove_pron=remove_pron, lemmatize=lemmatize, lowercase=lowercase))
        yield item
