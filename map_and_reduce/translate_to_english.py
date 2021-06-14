import re


def replace_7t(text):
    return text.replace('7', 't')

def replace_9i(text):
    return text.replace('9', 'i')

def replace_4a(text):
    return text.replace('4', 'a')

def replace_3e(text):
    return text.replace('3', 'e')

def replace_8m(text):
    return text.replace('8', 'm')

def map_spaces(text):
    return re.sub('[!@#$%^&*()[]{};:,./<>?\|`~-=_+]', " ", text)

def hacker_translation(texts):
    texts = list(map(replace_7t, texts))
    texts = list(map(replace_9i, texts))
    texts = list(map(replace_4a, texts))
    texts = list(map(replace_3e, texts))
    texts = list(map(replace_8m, texts))
    texts = list(map(map_spaces, texts))
    return texts

sentences = ["7h9s!9s?h4ck3r;73x7-", "9;48;8or3=h4ck3r/73xt?"]
print(hacker_translation(sentences))