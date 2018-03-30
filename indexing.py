import os
import json
import subprocess
import struct
import mmh3

from binary_encoders import encode_sequence
from doc2words import extract_words
from bs4 import BeautifulSoup
# from docreader import *

index_partition_size = 500000
SPIDER_HOME = 'spider_home'
HTML_DATA_PATH = os.path.join(SPIDER_HOME, 'raw_data')
TEXT_DATA_PATH = os.path.join(SPIDER_HOME, 'clean_texts')
INDEX_PATH = 'temp_idx'


def clear_index(index):
    for term in index.keys():
        index[term] = [index[term][-1]]


def write_index_partition(filename_pattern, index, encoding):
    terms_dict = {}
    with open(filename_pattern + '.idx', 'wb') as index_file:
        term_offset_in_index = 0
        for term, related_docs_ids in index.items():
            if len(related_docs_ids) < 2:
                continue
            encoded_seq = encode_sequence(related_docs_ids, encoding)
            index_file.write(encoded_seq)
            terms_dict[term] = (term_offset_in_index, len(encoded_seq))
            term_offset_in_index += len(encoded_seq)

    with open(filename_pattern + '.dct', 'wb') as terms_dict_file:
        terms_dict_file.write(struct.pack("Q", len(terms_dict)))
        for term, (offset, seq_len) in terms_dict.items():
            terms_dict_file.write(struct.pack("qII", term, offset, seq_len))
    clear_index(index)


def parse_index_header(path):
    with open(path, 'r', encoding='utf-8') as index_hdr:
        index_object = json.load(index_hdr)
    return index_object


def get_snippet(doc_id, term, edge=100):
    title = "Untitled"
    text = ""
    try:
        with open(os.path.join(HTML_DATA_PATH, '{0}.html'.format(doc_id)), 'r', encoding='utf-8') as html:
            soup = BeautifulSoup(html.read(), 'lxml')
            title = soup.head.title.string
            text = get_doctext(doc_id)
            pos = text.index(term)
            begin = pos - edge if pos - edge >= 0 else 0
            end = pos + edge if pos + edge <= len(text) else len(text)
            snippet = text[begin:end]
    except ValueError:
        snippet = text[:edge * 2]
    return title, snippet.replace('\n', ' ').replace('\r', '')


def get_doctext(doc_id):
    with open(os.path.join(TEXT_DATA_PATH, '{0}.txt'.format(doc_id)), 'r', encoding='cp1251') as textfile:
        text = textfile.read()
        return text


if __name__ == '__main__':
    if not os.path.exists(INDEX_PATH):
        os.makedirs(INDEX_PATH)
    index, url_list = {}, []
    current_partition_id = 0
    encoding_method = 'simple9'

    index_header = parse_index_header(os.path.join(SPIDER_HOME, 'index.json'))

    index_is_empty = True
    for doc_idx, url in index_header.items():
        doc_idx = int(doc_idx)
        text = get_doctext(doc_idx)
        index_is_empty = False
        url_list.append(url + '\n')
        terms = set(extract_words(text))
        if len(terms) == 0:
            # print('Document {0} is empty'.format(doc_idx))
            continue
        for term in terms:
            key = mmh3.hash64(term)[0]
            if key in index:
                index[key].append(doc_idx)
            else:
                # Zero index is used for delta computation for first document in sequence
                index[key] = [0, doc_idx]
        if (doc_idx + 1) % index_partition_size == 0:
            filename = os.path.join(INDEX_PATH, 'part{0:03d}'.format(current_partition_id))
            write_index_partition(filename, index, encoding_method)
            current_partition_id += 1
            index_is_empty = True
    if not index_is_empty:
        filename = os.path.join(INDEX_PATH, 'part{0:03d}'.format(current_partition_id))
        write_index_partition(filename, index, encoding_method)

    with open(os.path.join(INDEX_PATH, 'encoding.ini'), 'w') as config_file:
        config_file.write(encoding_method)
    with open(os.path.join(INDEX_PATH, 'url_list'), 'w') as f:
        f.writelines(url_list)