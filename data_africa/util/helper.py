import re

def splitter(x):
    return re.split(",(?! )", x)
