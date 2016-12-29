'''Helper module for miscellanous utlities for use throughout the API'''
import re


def splitter(user_input):
    '''Logic for splitting input variables based on commas'''
    return re.split(",(?! )", user_input)
