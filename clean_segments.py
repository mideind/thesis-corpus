
from segment_cleaner import *
import segment_skemman


def clean_current_db():
    seg_db = segment_skemman.SegmentDb()
    max_docid = seg_db.get_max_docid()

    cleaner = SegmentCleaner.default_cleaner()
    
    #for i in range(1, max_docid+1):

    import random
    i = random.randint(1, max_docid) 

    segs = seg_db.get_segments_for_document(i)
    cleaner = SegmentCleaner.default_cleaner()

    cleaned = cleaner.clean_segments(segs)
    #for s in cleaned:
        #print(s)

    #break


