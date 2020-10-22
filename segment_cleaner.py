#!/usr/bin/env python

from __future__ import annotations
from typing import Callable, Iterable, List
from segment_skemman import Segment
import language_id


SegmentCollection = List[Segment]
SegmentCleaningRule = Callable[[SegmentCollection], SegmentCollection]


class SegmentCleaner:
    def __init__(self):
        self.rules: List[SegmentCleaningRule] = []

    def add_rule(self, rule: SegmentCleaningRule):
        self.rules.append(rule)

    def clean_segments(self, segments: SegmentCollection) -> SegmentCollection:
        out = segments
        for rule in self.rules:
            print(" applying rule:", rule)
            out = rule(out)
        return out

    @staticmethod
    def default_cleaner() -> SegmentCleaner:
        # TODO: some sensible default rules
        c = SegmentCleaner()
        # c.add_rule(kill_short_lines)
        # c.add_rule(print_rule)
        c.add_rule(fasttext_confidence_filter)
        return c


def nop_rule(segments: SegmentCollection) -> SegmentCollection:
    print("Applying the NOP rule. This does nothing.")
    return segments


def drop_rule(segments):
    return [x for i, x in enumerate(segments) if i % 2 == 0]


def print_rule(segments):
    for s in segments:
        print(s)
    return segments


class save_to_file_rule:
    def __init__(self, filename: str):
        self.filename = filename

    def __call__(self, segments: SegmentCollection) -> SegmentCollection:
        with open(self.filename, "w") as logfile:
            for s in segments:
                logfile.write(s.text)
                logfile.write("\n")

        return segments


def strip_whitespace(segments: SegmentCollection) -> SegmentCollection:
    return [Segment(s.text.strip(), s.metadata) for s in segments]


def identify_language(segments):
    for s in segments:
        s.metadata["language"] = language_id.predict_lang(s.text)[0][0][9:]
        print("id", s, s.text, s.metadata)

    return segments


def kill_short_lines(segments):
    return [s for s in segments if len(s) > 10]


def fasttext_confidence_filter(segments: SegmentCollection) -> SegmentCollection:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"
    for s in segments:
        lang_id = language_id.predict_lang(s.text)
        confidence = lang_id[1][0]
        color = RED
        if confidence > 0.8:
            color = GREEN
        elif confidence > 0.4:
            color = YELLOW

        print(color, confidence, RESET, s.text)

    return segments


if __name__ == "__main__":
    print("spring cleaning")

    sc = SegmentCleaner()
    # sc.add_rule(nop_rule)
    # sc.add_rule(strip_whitespace)
    # sc.add_rule(save_to_file_rule("log_file.txt"))
    # sc.add_rule(print_rule)
    # sc.add_rule(drop_rule)
    # sc.add_rule(print_rule)
    # sc.add_rule(identify_language)
    sc.add_rule(fasttext_confidence_filter)

    segs = [Segment("Ég ætti að kaupa bát."), Segment("I should buy a boat.")]
    clean_segs = sc.clean_segments(segs)
    print(clean_segs)

    # sc2 = SegmentCleaner.default_cleaner()
    # clean2 = sc2.clean_segments(segs)
    # print(clean2)
