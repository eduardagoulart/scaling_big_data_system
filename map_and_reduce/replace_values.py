import re


def replace_7t(s):
    return s.replace("7", "t")


def replace_3e(s):
    return s.replace("3", "e")


def replace_6g(s):
    return s.replace("6", "g")


def replace_4a(s):
    return s.replace("4", "a")


class ChineseMatcher:
    def __init__(self):
        self.r = re.compile(r"[\u4e00-\u9fff]+")

    def sub_chinese(self, s):
        return re.sub(self.r, " ", s)


C = ChineseMatcher()
sample_messages = ["7his一is万h4ck3r", "I丈4m业mor3"]
translated_message = map(
    C.sub_chinese,
    map(
        replace_4a,
        map(replace_3e, map(replace_6g, map(replace_7t, sample_messages))),
    ),
)
print(list(translated_message))

from toolz.functoolz import compose

apply_compose = compose(
    C.sub_chinese, replace_4a, replace_3e, replace_6g, replace_7t
)
translated_compose = map(apply_compose, sample_messages)
print(list(translated_compose))
