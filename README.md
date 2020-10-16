This is a set of scripts to crawl skemman.is for all open access articles and extracting their text.

Runnable files:

thesis_scraper.py : Collects the index of available files
sync.py : Uses the index to download pdfs
utils.py : (broken) collect some stats about pdfs?
to_text.py : Updates language info in the db. Extracts text with pdftotext. (pdftotext is not the best tool, unfortunately)
segment_skemman : extracts segments from pdfs with pdfbox and stores them in segments.db
extract_abstracts.py: lots of heuristics to try to extract good abstracts

Database files:

skemman.db : Index of pdfs from Skemman
segment.db : Segment database (?)
