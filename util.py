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
import urllib.parse
from urllib.parse import urlparse
import os
import pandas as pd
import sys
import subprocess

tweets_list = [] 

log_folder = './../.log/'
config_folder = './../.config/'
streamer_log=log_folder+'streamer'
crawler_log=log_folder+'crawler'
cache_folder="./../.cache/"
current_conf = ''



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
    keys = config_folder + keys
    df = pd.DataFrame()
    api_keys = {"apikeys":dict()}
    try:
        df = pd.read_csv(keys,sep=',')
        if not df.empty:
            for item in df.iterrows():
                #if item[0] % len(df) == index:
                    api_keys["apikeys"][item[0]] = dict(item[1])
    except Exception as exp:
        print('error while loading API keys... ' + str(exp))
        pass
    return df

def load_api_keys_dict(keys="keys", index=0):
    keys = config_folder + keys
    df = pd.DataFrame()
    api_keys = {"apikeys":dict()}
    try:
        df = pd.read_csv(keys,sep=',')
        if not df.empty:
            for item in df.iterrows():
                #if item[0] % len(df) == index:
                    api_keys["apikeys"][item[0]] = dict(item[1])
    except Exception as exp:
        print('error while loading API keys... ' + str(exp))
        pass
    return api_keys

def save_api_keys(df, keys="keys", index=0):
    keys = config_folder + keys
    try:
        df.to_csv(keys,sep=',',index=False)
        return True
    except Exception as exp:
        print('error while writing API keys... ' + str(exp))
        return False

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
    terms_file = config_folder + terms_file
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
    screen_name_file = config_folder + screen_name_file
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
    ids_file = config_folder + ids_file
    
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
    screen_name_file = config_folder + screen_name_file
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
    terms_file = config_folder + terms_file
    terms_list = []
    with open (terms_file, 'r', encoding='utf-8') as f_in:
        for line in f_in.readlines():
            terms_list.append(line.strip())
    return terms_list

def update_terms_list(terms_list, terms_file):
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
    terms_file = config_folder + terms_file
    with open (terms_file, 'w', encoding='utf-8') as f_out:
        for item in terms_list:
            f_out.write('{}\r\n'.format(item))

def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0
    
    
def get_services(config_file):
    services = []
    if is_non_zero_file(config_file):
        df = pd.read_csv(config_file, sep='\n')
        services = list(df['core_name'].values.tolist())
    return services

def run_crawler( keys_path=config_folder+'keys',command = "search",terms_file='terms',wait_time=10):
    '''
    if command=='search':
        pid=subprocess.Popen('python ./twitter_collector.py -c '+ str(keys_path) + ' -tf '+str(terms_file) + ' -cmd ' + str(command) + ' -wait ' + str(wait_time) + ' &', shell=True)
    elif command=='stream':
        pid=subprocess.Popen('python ./twitter_collector.py -c '+ str(keys_path) + ' -cmd ' + str(command) + ' -wait ' + str(wait_time) + ' &', shell=True)
    else:
        pid = None
    '''
    pid=subprocess.Popen('python ./twitter_collector.py -c '+ str(keys_path) + ' -tf '+str(terms_file) + ' -cmd ' + str(command) + ' -wait ' + str(wait_time) + ' &', shell=True)
    if pid:
        with open(cache_folder+'process','w') as f_out:
            f_out.write(str(pid.pid))
        return True 
    return False

def stop_crawler():
    os.system("kill `ps aux | grep twitter_collector.py | awk '{ print $2 }' | sort -n` ")
