#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sat Jan 11 00:00:00 2020

"""
import json
import time
import re
import nltk
import requests
tokenizer = nltk.TweetTokenizer()
import subprocess
import urllib.parse
from urllib.parse import urlparse
import os
import pandas as pd
import sys
import fasttext
tweets_list = [] 

config_folder = "./../.config/"
lang_detect_file = config_folder+"lang_detect"
current_conf = ''
with open(lang_detect_file, 'r') as fp:
    current_conf = json.load(fp)
file_name = current_conf['file']

#sys.stderr = open(config_folder+'temp', 'a+')
lang_model = fasttext.load_model(file_name)
#sys.stderr = sys.__stdout__

def get_language(tweet_text):   
    lang = ['lang', 0.0]
    try:
        tweet_text = re.sub('[@#][^ ]+','',tweet_text)
        tmp = ' '.join([x for x in tweet_text.split() if str.isalpha(x) and not x.startswith('#') and not x.startswith('@')])
        if len(tmp) > 3:
            tweet_text = tmp    #this to avoid removing Ch/Ja strings
        lang = lang_model.predict(tweet_text,1,0.2)
    except Exception as e:
        lang = ['lang', 0.0]
        pass
    return lang

def update_log(file_name, updates):
    print(updates,'\n')
    with open(file_name, 'a+',encoding='utf-8') as logging:
        logging.write("%s%s"%(updates,'\n'))

def write_running_status(status_file,status):
    with open(status_file,'w') as f_out:
        f_out.write(status)
    return True

def read_running_status(status_file):
    try:
        with open(status_file,'r') as f_in:
            kRun = f_in.readline()
        return kRun.strip() == '1'
    except Exception as exp:
        return False
    return False

def dump_dict_to_file(output_file, data_dict, mode = 'w',encoding='utf-8'):
    with open(output_file,mode,encoding='utf-8') as fo:
        json.dump(data_dict,fp=fo, ensure_ascii=False)

def load_api_keys(keys="keys", index=0):
    import pandas as pd
    df = pd.DataFrame()
    api_keys = {"apikeys":dict()}
    try:
        df = pd.read_csv(keys,sep=',')
        if not df.empty:
            for item in df.iterrows():
                if item[0] % 2 == index:
                    api_keys["apikeys"][item[0]] = dict(item[1])
    except Exception as exp:
        print('error while loading API keys... ' + str(exp))
        pass
    return api_keys

def get_search_dict(terms_file, since_id=1, geocode = None):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    terms_list = []
    with open (terms_file, 'r') as f_in:
        for line in f_in.readlines():
            terms_list.append(line)
    import math
    x = math.ceil(len(terms_list) / 15)
    data_dict = {}
    for i in range(0,x):
        data_dict['search'+str(i)] = dict()
        data_dict['search'+str(i)]["geocode"] = geocode
        data_dict['search'+str(i)]["since_id"] = since_id
        data_dict['search'+str(i)]["terms"] = []
        for j in range(0,15):
            if j+15*i < len(terms_list):
                data_dict['search'+str(i)]["terms"].append(terms_list[j+15*i])
    return data_dict


def get_screen_name_dict(screen_name_file, since_id=1, geocode = None):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    screen_names_dict = {}
    with open (screen_name_file, 'r') as f_in:
        screen_names_dict = json.load(f_in)
    return screen_names_dict
    
def get_ids_dict(ids_file, since_id=1, geocode = None):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    ids_dict = {}
    with open (ids_file, 'r') as f_in:
        ids_dict = json.load(f_in)
    return ids_dict

def update_screen_name_file(screen_names_dict, screen_name_file):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    for k in screen_names_dict.keys():
        if "since_id" not in screen_names_dict[k].keys():
            screen_names_dict[k]["since_id"] = 1
    with open (screen_name_file, 'w') as f_out:
        json.dump(screen_names_dict, f_out, ensure_ascii = False)


def get_terms_list(terms_file):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    
    '''
    terms_list = []
    with open (terms_file, 'r', encoding='utf-8') as f_in:
        for line in f_in.readlines():
            terms_list.append(line.strip())
    return terms_list


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0
    
    
def get_services(config_file):
    services = []
    if is_non_zero_file(config_file):
        df = pd.read_csv(config_file, sep='\n')
        services = list(df['core_name'].values.tolist())
    return services
    