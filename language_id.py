#!/usr/bin/env python3

import fasttext

import segment_skemman

_model = None

# Only actually load the model file if we're going to use it.
def load_model():
    # Downloaded from https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz
    # More info:
    #   https://fasttext.cc/docs/en/language-identification.html
    #   https://medium.com/@c.chaitanya/language-identification-in-python-using-fasttext-60359dc30ed0
    global _model
    if not _model:
        _model_file = "fasttext-language-identification.small.176.ftz"
        _model = fasttext.load_model(_model_file)


def predict_lang(text):
    load_model()
    return _model.predict(text, k=2)  # returns top 2 matching languages


def predict_all():
    seg_db = segment_skemman.SegmentDb()
    max_docid = seg_db.get_max_docid()

    for i in range(1, max_docid + 1):
        pred = predict_lang("".join(seg_db.get_segments_for_document(i)))
        if pred[0][0] != "__label__is" and pred[0][0] != "__label__en":
            print("predicted non is/en", i, pred)
        if pred[1][0] < 0.9:
            print("low certainty", i, pred)
        # TODO: save results.
        # Some documents contain passages in both Icelandic and English.
        # Need to mark the language separately for these passages.


if __name__ == "__main__":
    # print(predict_lang("i should buy a boat"))
    # print(predict_lang("ég ætti að kaupa bát"))

    # seg_db = segment_skemman.SegmentDb()
    # print(predict_lang(''.join(seg_db.get_segments_for_document(3))))

    predict_all()
