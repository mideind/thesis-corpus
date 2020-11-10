#!/usr/bin/env python


from sickle import Sickle
from pprint import pprint
import json
from bs4 import BeautifulSoup as bs
from pathlib import Path
import os
import time
import subprocess

from reynir import bintokenizer
from tokenizer import paragraphs, mark_paragraphs, correct_spaces
from collections import namedtuple

import fetcher


"""
Get data from Hirslan.
The base website for searching is here: https://www.hirsla.lsh.is/discover
The CSV export button is broken and only ever returns 50 entries. Use the OAI API instead.
"""

_pdfbox_location = "/home/petur/Downloads/pdfbox-app-2.0.21.jar"

#_data_dir = Path('data/')
_data_dir = Path('samples/')
_pdf_dir = _data_dir / 'pdf'
_db_dir = _data_dir / 'db'

SimpleSegment = namedtuple("Segment", "index text")

def get_metadata():
    sickle = Sickle('https://www.hirsla.lsh.is/oai/request')
    #sickle = Sickle('https://skemman.is/oai/request')
    records = sickle.ListRecords(metadataPrefix='oai_dc')


    with open(_db_dir / "hirslan-metadata.dump", 'w') as f:
    #with open("skemman-metadata.dump", 'w') as f:
        count = 0
        for r in records:
            try:
                m = r.metadata
            except Exception as e:
                print("wargh?", r)
                continue
            json.dump(r.metadata, f)
            f.write('\n')
            #for i in filter(lambda a : a.startswith('http'),r.metadata['identifier']):
            #    print(i)

            count += 1
            if count % 100 == 0:
                print(count)


def get_pdf(url):
    """Get the pdf(s) for a single article and save it to a folder according to the breadcrumbs."""

    print("fetching", url)
    html = fetcher.Fetcher.fetch(url).content
    soup = bs(html, "html.parser")

    buttons = soup.findAll('a', class_='file-download-button')
    if buttons is None:
        print("didn't find download button at link", url, button)
        return

    pdfs = []
    for b in buttons:
        pdf_url = b['href'].rsplit('?')[0]
        pdf_name = pdf_url.rsplit('/')[-1]

        if pdf_name[-3:] == 'pdf':
            pdfs.append((pdf_name, pdf_url))
            break
        print("file not pdf:", pdf_url, url)

    print("found pdf files", pdfs)

    crumbs = soup.find('ul', class_='breadcrumb')
    crumblinks = crumbs.findAll('a')
    
    savedir = _pdf_dir
    for link in crumblinks[1:]:
        savedir = savedir / link.text
    #print(savedir)
    os.makedirs(savedir, exist_ok=True)
    os.makedirs(_db_dir, exist_ok=True)

    with open(_db_dir / 'url_to_pdf', 'a') as db_file:
        for pdf, pdf_url in pdfs:
            pdf_url = "https://www.hirsla.lsh.is/" + pdf_url
            fetcher.download_file(pdf_url, savedir/pdf, "/tmp/hirslan.pdf.tmp")

            data_map = {'url':url, 'file':str(savedir/pdf)}
            db_file.write(json.dumps(data_map) + "\n")
            print("fetched", pdf, "from", url)


def find_url_in_identifier_block(metadata):
    if 'identifier' in metadata and len(metadata['identifier']) > 0:
        for s in metadata['identifier']:
            if type(s) == str and s.startswith('http://hdl.handle.net'):
                return s
    return None


def get_pdfs():
    with open(_db_dir / "hirslan-metadata.dump") as f:
        metadata = [json.loads(l) for l in f.read().splitlines()]

    STARTAT = 0 # Useful since the server has rate limits and sometimes errors which cause exceptions
    count = 0
    for m in metadata:
        count += 1
        if count < STARTAT:
            continue
        url = find_url_in_identifier_block(m)
        if url is not None:
            get_pdf(url)
        # This got abstracted to find_url_in_identifier_block(), but has not been tested in this place
        #if 'identifier' in m and len(m['identifier']) > 0:
        #    for s in m['identifier']:
        #        if type(s) == str and s.startswith('http://hdl.handle.net'):
        #            get_pdf(s)

        if count % 10 == 0:
            print(count)
        time.sleep(1)


def merge_data():
    with open(_db_dir / "hirslan-metadata.dump") as f:
        metadata = [json.loads(l) for l in f.read().splitlines()]

    with open(_db_dir / "url_to_pdf.sorted") as f:
        url_to_pdf = [json.loads(l) for l in f.read().splitlines()]

    urls_to_local = {}
    for u2p in url_to_pdf:
        if u2p['url'] in urls_to_local:
            print(u2p['url'], "occurs more than once")
            print(u2p)
            print(urls_to_local[u2p['url']])
        else:
            urls_to_local[u2p['url']] = u2p['file']

    for m in metadata:
        url = find_url_in_identifier_block(m)
        if url is None:
            continue

        if url in urls_to_local:
            m['local_filename'] = urls_to_local[url]
        # else: couldn't get the pdf; probably closed source

    with open(_db_dir / "hirslan-metadata.with-pdf.json", 'w') as f:
        json.dump(metadata, f)


def extract_text_from_pdf():
    pdf_files = []
    for (dirpath, dirnames, filenames) in os.walk(_pdf_dir):
        pdf_files.extend([str(Path(dirpath) / fn) for fn in filenames if fn.endswith(".pdf")])

    count = 0
    for f in pdf_files:
        print(f)
        subprocess.run([
                'java',
                '-jar',
                _pdfbox_location,
                'ExtractText',
                f,
                f + '.txt'
        ])

        count += 1
        if count % 10 == 0:
            print(count)


def fancify_text():
    # copied from the skemman segmenter
    def toks_to_text(tokstream):
        return correct_spaces(" ".join([tok.txt for tok in tokstream if tok.txt]))
    def segment_text(text):
        sent_offset = 1
        par_offset = 1
        # text = mark_paragraphs(text)
        # output from skemman+pdfbox seems better without marking paragraphs
        toks = bintokenizer.tokenize(text)
        paragraph_stream = paragraphs(toks)
        for (par_idx, paragraph) in enumerate(paragraph_stream):
            par_idx += par_offset
            for (rel_sent_idx, (offset, sentence)) in enumerate(paragraph):
                sent_idx = sent_offset + rel_sent_idx
                # yield SimpleSegment(index=f"{par_idx}.{sent_idx}", text=toks_to_text(sentence))
                #yield SimpleSegment(index=str(sent_idx), text=toks_to_text(sentence))
                yield toks_to_text(sentence)

    txt_files = []
    for (dirpath, dirnames, filenames) in os.walk(_pdf_dir):
        txt_files.extend([str(Path(dirpath) / fn) for fn in filenames if fn.endswith(".pdf.txt")])

    count = 0
    for fn in txt_files:
        print(fn)
        with open(fn) as f:
            with open(fn + '.fancified', 'w') as fplus:
                text = f.read()
                for t in segment_text(text):
                    fplus.write(t)
                    fplus.write('\n')

        count += 1
        if count % 10 == 0:
            print(count)


if __name__ == "__main__":
    #get_metadata()
    #get_pdfs()
    #merge_data()
    extract_text_from_pdf()
    fancify_text()

