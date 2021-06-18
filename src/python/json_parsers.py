import pymongo
import pandas as pd
import numpy as np
import pprint
import os
import math
import json
import datetime
import argparse
import logging
from re import search
from datetime import datetime
from pymongo import MongoClient


def json_extract(obj, key):
    """
    Recursively fetch values from nested JSON.
    
    Parameters
    ----------
    obj : obj
        JSON str to look in
    key : str
        key or substring within key within the obj you want to find
        
    Returns
    -------
    float if original value is float, str otherwise
        1 value is found
    
    array
        more than one value is found
        
    """
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif search(key, k):
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


def json_extract_exact(obj, key):
    """
    Recursively fetch values from nested JSON.
    
    Parameters
    ----------
    obj : obj
        JSON str to look in
    key : str
        key or substring within key within the obj you want to find
        
    Returns
    -------
    float if original value is float, str otherwise
        1 value is found
    array
        more than one value is found
        
    """
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


def json_extract_with_key(obj, key):
    """
    Recursively fetch values from nested JSON.
    
    Parameters
    ----------
    obj : str
        JSON str
    key : str
        key or substring within key within the obj you want to find
        
    Returns
    -------
    array
        key value pairs of key input
        
    """
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif search(key, k):
                    arr.append([k, v])
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values

        
def json_extract_value_with_key(obj, value):
    """
    Recursively fetch values from nested JSON.
    
    Parameters
    ----------
    obj : str
        JSON str
    key : str
        key or substring within key within the obj you want to find
        
    Returns
    -------
    str
        value of key value pair
        
    """
    arr = []

    def extract(obj, arr, value):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, value)
                elif search(value, v):
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, value)
        return arr

    values = extract(obj, arr, value)
    return values
      

def json_extract_unique_exact_array(jstring, key):
    """
    Returns arrays of length 1 as a string, array otherwise
    
    Parameters
    ----------
    jstring : str
        JSON str
    key : str
        key within key within the obj you want to find
        
    Returns
    -------
        array
        
    """
    values = np.unique(json_extract_exact(jstring, key))
    return values


def json_extract_unique_exact(obj, key):
    """
    Returns arrays of length 1 as a string, array otherwise
    
    Parameters
    ----------
    obj : str
        JSON str
    key : str
        key within key within the obj you want to find
        
    Returns
    -------
        array
           
    """
    values = np.unique(json_extract_exact(obj, key))

    if len(values) == 1:
        return values[0]
    else:
        return values

    return values

    
def json_extract_unique(obj, key):
    """
    Returns arrays of length 1 as a string, array otherwise
    
    Parameters
    ----------
    obj : str
        JSON str
    key : str
        key within key within the obj you want to find
        
    Returns
    -------
    array
        if input length of array > 1
    str
        otherwise
        
    """
    values = np.unique(json_extract(obj, key))

    if len(values) == 1:
        return values[0]
    else:
        return values

    return values


def json_extract_unique_array(obj, key):
    """
    Returns arrays of length 1 as a string, array otherwise
    
    Parameters
    ----------
    obj : str
        JSON str
    key : str
        key within key within the obj you want to find
        
    Returns
    -------
    array
        if input length of array > 1
    str
        otherwise
        
    """
    values = np.unique(json_extract(obj, key))
    return values


def json_extract_unique_with_key(obj, key):
    '''
    Returns arrays of length 1 as a string, array otherwise; only unique values
    
    Parameters
    ----------
    obj : str
        JSON str
    key : str
        key within key within the obj you want to find
    values = np.unique(json_extract_with_key(jstring, key))

    Returns
    -------
    array
        if input length of array > 1
    str
        otherwise
        
    '''
    values = np.unique(json_extract_with_key(obj, key)) 
    return values
    

def parse_json_exact(df, column, key):
    '''
    Parses nested json inside a dataframe column, first nesting only, partial substring key match
    
    Parameters
    ----------
    df : pandas dataframe
        dataframe of input
    column : str
        dataframe column
    key : str
        json key 


    Returns
    -------
    float
        if length of record is 1
    array
        if length of record > 1
        
    '''
    parsed_json = json_extract_unique_exact(df['{}'.format(column)], key)
    return parsed_json


def parse_json_exact_array(df, column, key):
    '''
    Parses nested json inside a dataframe column, first nesting only, partial substring key match
    
    Parameters
    ----------
    df : pandas dataframe
        dataframe of input
    column : str
        dataframe column
    key : str
        json key 


    Returns
    -------
    float
        if length of record is 1
    array
        if length of record > 1
        
    '''
    parsed_json = json_extract_unique_exact_array(df['{}'.format(column)], key)
    return parsed_json


def parse_json(df, column, key):
    '''
    Parses nested json inside a dataframe column, first nesting only, exact key match
    
    Parameters
    ----------
    df : pandas dataframe
        dataframe of input
    column : str
        dataframe column
    key : str
        json key 


    Returns
    -------
    float
        if length of record is 1
    array
        if length of record > 1
        
    '''
    parsed_json = json_extract_unique(df['{}'.format(column)], key)
    return parsed_json


def nested_equipment_ids(df, column):
    """
    Returns all equipment IDs in JETS record except level 0 nests
    
    Parameter:
    df : pandas dataframe
        JETS dataframe
    column : str
        string/column name containing nested equipment_ids
        
    """
    return [x for x in json_extract(df[column], 'EQUIPMENT_ID') if x not in [df['source_id']]]

            
def jf12(df):
    """
    Parses JF-12 values inside bibliography on a JETS record
    
    Parameters
    ----------
    df : pandas dataframe
        JETS specific dataframe
    
    Returns
    -------
    df
        JETS specific dataframe
    
    """
    try:
        res = json_extract_with_key(df['jf12'],'BIBLIO_SHORT_TITLE_TEXT')
        jf12 = [res[i][1] for i in range(len(res)) if search('J/F 12', res[i][1])]
        jf12 = [i.split('J/F 12/')[1] for i in jf12]
        '''
        if len(jf12) == 1:
            jf12 = jf12[0]
        '''   
        return jf12
    except:
        return ''
    
               
def parse_json_frequency_low(df, column, key):
    """
    Takes a JETS dataframe and column containing JSON strings
    and finds the lowest 'Mode' or 'Config' frequency.
    Excludes intermediate frequencies
    
    Parameters
    ----------
    df : pandas dataframe
        JETS dataframe
    column : str
        The column where the frequencies are located
    key : str
        A substring of the key you are looking for in the json record(ex. 'FREQ' or 'MIN_FREQ')
        
    Returns
    -------
        float
    
    """
    parsed_json = json_extract_with_key(df['{}'.format(column)], key)
    arr = []
    for i in range(len(parsed_json)):
        if search("MODE", parsed_json[i][0]):
            arr.append(parsed_json[i][1])
        elif search("CONFIG", parsed_json[i][0]):
            arr.append(parsed_json[i][1])
    try:
        return min(arr)
    except:
        return ''

    
def parse_json_frequency_high(df, column, key):
    """
    Takes a JETS dataframe and column containing JSON strings
    and finds the highest 'Mode' or 'Config' frequency.
    Excludes intermediate frequencies
    
    Parameters
    ----------
    df : pandas dataframe
        JETS dataframe
    column : str
        The column where the frequencies are located
    key : str
        A substring of the key you are looking for in the json record(ex. 'FREQ' or 'MIN_FREQ')
        
    Returns
    -------
        float
    
    """
    parsed_json = json_extract_with_key(df['{}'.format(column)], key)
    arr = []
    for i in range(len(parsed_json)):
        if search("MODE", parsed_json[i][0]):
            arr.append(parsed_json[i][1])
        elif search("CONFIG", parsed_json[i][0]):
            arr.append(parsed_json[i][1])
    try:
        return max(arr)
    except:
        return ''
