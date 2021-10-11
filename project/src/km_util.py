import sys
import os
import re
import nltk

tokenizer = nltk.RegexpTokenizer(r"\w+")

def report_progress(completed: float, total: float) -> None:
    """Shows a progress bar. Adapted from: 
    https://stackoverflow.com/questions/3160699/python-progress-bar"""
    
    progress = completed / total
    bar_length = 20
    block = int(round(bar_length * progress))
    text = "\rProgress: [{0}] {1}% ({2}/{3})".format(
        "█" * block + "-" * (bar_length - block), 
        round(progress * 100),
        int(completed),
        int(total))
    sys.stdout.write(text)
    sys.stdout.flush()

    if completed == total:
        print("\n")

def read_all_lines(path: str) -> 'list[str]':
    """Reads a text file into a list of strings"""

    with open(path, 'r') as f:
        lines = f.readlines()
        lines = [line.strip("\n\r") for line in lines]

    return lines

def write_all_lines(path: str, items: 'list[str]') -> None:
    """Writes a list of strings to a file"""

    dir = os.path.dirname(path)

    if not os.path.exists(dir):
        os.mkdir(dir)

    with open(path, 'w') as f:
        for item in items:
            f.write(str(item))
            f.write('\n')

def get_sanitized_text(text: str, regex: str):
    sanitized_text = re.sub(regex, '', text)
    return sanitized_text

def get_n_grams(text: str, n: int, n_gram_mem_buffer: list) -> 'list[str]':
    sanitized_text = get_sanitized_text(text, r'[^\w\s]')
    tokens = tokenizer.tokenize(sanitized_text)
    n_gram_mem_buffer.clear()

    for i, token in enumerate(tokens):
        if n > 1:
            for j in range(i + 1, i + n + 1):
                n_gram_mem_buffer.append(' '.join(tokens[i:j]))
        else:
            n_gram_mem_buffer.append(tokens[i])

    return n_gram_mem_buffer