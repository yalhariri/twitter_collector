#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sat Jan 11 00:00:00 2020

"""

import logging
import logging.handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


import sys
import os
import time
import itertools
import json
from twitter_classes import TwitterCrawler
from util import *

WAIT_TIME = 30
log_folder = '../.log/'
config_folder = '../.config/'
streamer_log=log_folder+'streamer'
crawler_log=log_folder+'crawler'
cache_folder="../.cache/"
status_file = cache_folder+"status_crawler"

if not os.path.isdir(log_folder):
    os.mkdir(log_folder)
if not os.path.isdir(config_folder):
    os.mkdir(config_folder)
if not os.path.isdir(cache_folder):
    os.mkdir(cache_folder)
        
def collect_tweets_by_search_terms(config, url, uname, password, terms_file, selected_fields = False, key_turn=0, file_name_output='output_data'):
    #api_keys = list(config['api_keys'].values()).pop()
    if terms_file == None:
        print('Please include the terms file!\nCrawler ended...')
        return
        
    api_keys = list(config['apikeys'].values())[key_turn]
    app_key = api_keys['app_key']
    app_secret = api_keys['app_secret']
    oauth_token = api_keys['oauth_token']
    oauth_token_secret = api_keys['oauth_token_secret']
    
    #print("api_keys" + str(api_keys))
    if len(api_keys) > 0:
    
        search_configs = {}
        search_configs = get_search_dict(terms_file)
        
        for search_config_id in itertools.cycle(search_configs):
           
            search_config = search_configs[search_config_id]
            
            search_terms = [term.lower() for term in search_config['terms']]
            
            querystring = '%s'%(' OR '.join('(' + term.strip() + ')' for term in search_terms))
            
            since_id = search_config['since_id'] if 'since_id' in search_config else 0
            geocode = tuple(search_config['geocode']) if ('geocode' in search_config and search_config['geocode']) else None

            
            try:
                #twitterCralwer = TwitterCrawler(api_keys=api_keys, url=url, uname=uname, password=password)
                twitterCralwer = TwitterCrawler(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields, file_name_output=file_name_output)
                since_id = twitterCralwer.search_by_query(querystring, geocode=geocode, since_id=since_id, current_max_id = 0)
            except Exception as exc:
                update_log(crawler_log, exc)
                pass

            search_config['since_id'] = since_id
            search_config['querystring'] = querystring
            search_config['geocode'] = geocode

            search_configs[search_config_id] = search_config
            time.sleep(WAIT_TIME)


def collect_timeline_by_screen_name(config, url, uname, password, screen_name_file, selected_fields = False, key_turn=0):
    #api_keys = list(config['api_keys'].values()).pop()
    if screen_name_file == None:
        print('Please include the screen names file!\nCrawler ended...')
        return
        
    api_keys = list(config['apikeys'].values())[key_turn]
    app_key = api_keys['app_key']
    app_secret = api_keys['app_secret']
    oauth_token = api_keys['oauth_token']
    oauth_token_secret = api_keys['oauth_token_secret']
    
    #print("api_keys" + str(api_keys))
    if len(api_keys) > 0:
    
        accounts_screen_names = {}
        
        accounts_screen_names = get_screen_name_dict(screen_name_file)
        
        for screen_name in itertools.cycle(accounts_screen_names):
            print(screen_name)
            account = accounts_screen_names[screen_name]
           
            since_id = account['since_id'] if 'since_id' in account else 1
            try:
                twitterCralwer = TwitterCrawler(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields)
                since_id, No_Error = twitterCralwer.fetch_user_timeline_by_screen_name(account['screen_name'], since_id=since_id, include_rts = True)
            except Exception as exc:
                update_log(crawler_log, exc)
                pass

            accounts_screen_names[screen_name]['since_id'] = since_id
            update_screen_name_file(accounts_screen_names, screen_name_file)
            if not read_running_status(status_file):
                return 0
            else:
                time.sleep(WAIT_TIME)


def collect_timeline_by_id(config, url, uname, password, ids_file, selected_fields = False, key_turn=0):
    #api_keys = list(config['api_keys'].values()).pop()
    if ids_file == None:
        print('Please include the ids file!\nCrawler ended...')
        return
        
    api_keys = list(config['apikeys'].values())[key_turn]
    app_key = api_keys['app_key']
    app_secret = api_keys['app_secret']
    oauth_token = api_keys['oauth_token']
    oauth_token_secret = api_keys['oauth_token_secret']
    
    #print("api_keys" + str(api_keys))
    if len(api_keys) > 0:
    
        accounts_ids = {}
        
        accounts_ids = get_ids_dict(ids_file)
        
        for account_id in itertools.cycle(accounts_ids):
            print(account_id)
            account = accounts_ids[account_id]
           
            since_id = account['since_id'] if 'since_id' in account else 1
            try:
                twitterCralwer = TwitterCrawler(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields)
                since_id, No_Error = twitterCralwer.fetch_user_timeline_by_id(account['id'], since_id=since_id, include_rts = True)
            except Exception as exc:
                update_log(crawler_log, exc)
                pass

            accounts_ids[account_id]['since_id'] = since_id
            update_screen_name_file(accounts_ids, ids_file)
            if not read_running_status(status_file):
                return 0
            else:
                time.sleep(WAIT_TIME)



def get_friends(config, url, uname, password, screen_name_config_filepath=None, selected_fields = False, key_turn=0):
    """Function to start crawler to collect accounts' freidns by using terms list.

    Args:
        config (dict): Python dictionary that holds configuration details. Mainly for Twitter api keys.
        url (str): solr url (combined with core)
        uname (str): solr admin username
        password (str): solr admin password
        screen_name_config_filepath ([type], optional): path to accounts screen names list. Defaults to None.
        key_turn (int, optional): a switcher to select the Twitter api. It is useful to pass Twitter API limits. Defaults to 0.

    Returns:
        int: 0 if finished successfully.
    """
    #api_keys = list(config['apikeys'].values()).pop()
    api_keys = list(config['apikeys'].values())[key_turn]
    call = '/friends/list'
    screen_name_config = {}
    print('screen_name_config_filepath:',os.path.abspath(config_folder+screen_name_config_filepath))
    with open(os.path.abspath(config_folder+screen_name_config_filepath), 'r') as screen_name_config_rf:
        screen_name_config = json.load(screen_name_config_rf)
    
    all_items = len(screen_name_config['users'])
    current_ix = screen_name_config['current_ix'] if ('current_ix' in screen_name_config) else 0
    screen_names = screen_name_config['users'][current_ix:]
    
    
    total = len(screen_names)
    
    for screen_name in screen_names:
        try:
            twitterCralwer = TwitterCrawler(api_keys=api_keys, url = url, uname=uname, password=password, selected_fields=selected_fields)
            twitterCralwer.get_relationships_by_screen_name(call=call, screen_name=screen_name)
            current_ix += 1 # one at a time... no choice
        except Exception as exc:
            update_log(crawler_log,str(exc))
            #update_log(crawler_log,str(util.full_stack()))
            pass    

        screen_name_config['current_ix'] = current_ix
        
        with open(os.path.abspath(config_folder+screen_name_config_filepath), 'w') as screen_name_config_rf:
            json.dump(screen_name_config, screen_name_config_rf,ensure_ascii=False)

        update_log(crawler_log,str('COMPLETED -> (current_ix: [%s/%d])'%(current_ix, total)))
        update_log(crawler_log,str('PAUSE %ds to CONTINUE...'%WAIT_TIME))
        time.sleep(WAIT_TIME)
        if current_ix >= all_items:
            update_log(crawler_log,str('[%s] ALL COMPLETED'%(call)))
            return 0

def collect_tweets_by_ids(config, url, uname, password, ids_file, selected_fields = False, key_turn=0):
    print('here')
    if ids_file == None:
        print('Please include the ids file!\nCrawler ended...')
        return
        
    api_keys = list(config['apikeys'].values())[key_turn]
    app_key = api_keys['app_key']
    app_secret = api_keys['app_secret']
    oauth_token = api_keys['oauth_token']
    oauth_token_secret = api_keys['oauth_token_secret']
    
    #print("api_keys" + str(api_keys))
    if len(api_keys) > 0:
    
        tweet_ids_config = {}
        with open(os.path.abspath(ids_file), 'r') as tweet_ids_config_rf:
            tweet_ids_config = json.load(tweet_ids_config_rf)

        max_range = 100
        
        current_ix = tweet_ids_config['current_ix'] if ('current_ix' in tweet_ids_config) else 0
        if 'tweet_ids' not in tweet_ids_config.keys():
            temp_tweet_ids = list(tweet_ids_config.keys())
            tweet_ids_config = {'current_ix': 0, 'tweet_ids': temp_tweet_ids}
                
        total = len(tweet_ids_config['tweet_ids'][current_ix:])
        current_limit = min(current_ix+max_range, total-1)
        ix = int(current_ix)
        tweet_id_chuncks = []
        while ix < total:
            tweet_id_chuncks.append(tweet_ids_config['tweet_ids'][ix:current_limit])
            ix += max_range
            current_limit = min(ix+max_range, total-1)
        
        print('current_ix: {}'.format(current_ix))
        print('tweet_id_chuncks: {}'.format(len(tweet_id_chuncks)))
        
        
        
        
        for tweet_ids in tweet_id_chuncks:
            try:
                twitterCralwer = TwitterCrawler(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields)
                twitterCralwer.lookup_tweets_by_ids(tweet_ids)
                current_ix += len(tweet_ids)

            except Exception as exc:
                logger.error(exc)
                #logger.error(full_stack())
                pass

            tweet_ids_config['current_ix'] = current_ix
            
            with open(os.path.abspath(ids_file), 'w') as tweet_ids_config_rf:
                json.dump(tweet_ids_config, tweet_ids_config_rf,ensure_ascii=False)

            logger.info('COMPLETED -> (current_ix: [%d/%d])'%(current_ix, total))
            logger.info('PAUSE %ds to CONTINUE...'%WAIT_TIME)
            time.sleep(WAIT_TIME)
        else:
            logger.info('[tweets_by_ids] ALL COMPLETED')
        print('Finished')
    
        
def update_log_excption(command, exc):
    if (command == 'search'):
        update_log(crawler_log, exc)
        

if __name__== "__main__":
    print('Live')
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="config.json that contains twitter api keys;")
    parser.add_argument('-cmd','--command', help="commands: search, stream_terms, stream_accounts, get_timeline")
    parser.add_argument('-tf','--terms_file', help="terms list to seach for")
    parser.add_argument('-sf','--selected_fields', help="Write all tweet object or selected fields only (True or False)", default=False)
    parser.add_argument('-lf','--lang_file', help="language list split with newline", default=None)
    parser.add_argument('-url','--url', help="solr core to store the retreived tweets", default=None)
    parser.add_argument('-uname','--uname', default=None)
    parser.add_argument('-pswrd','--password', default=None)
    parser.add_argument('-wait','--wait_time', help="wait time to check available api keys", type=int, default=10)
    
    import time
    time.sleep(2)
    
    args = parser.parse_args()
    
    if args.selected_fields == 1 or args.selected_fields == 'true' or args.selected_fields == 'True':
        args.selected_fields = True
    else:
        args.selected_fields = False
        
    if args.wait_time > WAIT_TIME:
        WAIT_TIME = args.wait_time
    
    if not args.config:
        sys.exit('ERROR: API keys and config are required!')
    if not args.command:
        sys.exit('ERROR: command is required!')
    if not args.terms_file:
        sys.exit('ERROR: terms are required!')
        
    config = dict()
    
    key_turn=0
    
    import random
    config = load_api_keys(keys=args.config, index = random.randint(0,9)%2)
    file_name_output = args.command
    print(args.url, '\t', config)
    if len(config)>0:
        kRun = read_running_status(status_file)
        if not kRun:
            write_running_status(status_file, '1')
            kRun = read_running_status(status_file)
        else:
            print('Another process is running... exitting')
            kRun = 0
        try:
            print('Starting' , str(kRun))
            while(kRun):
                try:
                    key_turn=(key_turn+1)%len(config['apikeys'])
                    kRun = read_running_status(status_file)
                    if (args.command == 'terms'):
                        collect_tweets_by_search_terms(config, args.url, args.uname, args.password, args.terms_file, args.selected_fields, key_turn=key_turn, file_name_output=file_name_output)
                    elif (args.command == 'get_friends'):
                        get_friends(config, args.url, args.uname, args.password, screen_name_config_filepath=args.terms_file, selected_fields = args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'get_timelines'):
                        collect_timeline_by_screen_name(config, args.url, args.uname, args.password, screen_name_file=args.terms_file, selected_fields = args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'get_timelines_ids'):
                        collect_timeline_by_id(config, args.url, args.uname, args.password, ids_file=args.terms_file, selected_fields = args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'tweets_by_ids'):
                        collect_tweets_by_ids(config, args.url, args.uname, args.password, ids_file=args.terms_file, selected_fields = args.selected_fields, key_turn=key_turn)
                    else:
                        raise Exception("command not found!")
                except Exception as exc:
                    print('Exception' , exc)
                    update_log_excption(args.command, exc)
                finally:
                    kRun = read_running_status(status_file)
                    if kRun:
                        print("restarting...")
                        time.sleep(3)
                    else:
                        print('Exitting as requested.')
        except KeyboardInterrupt:
            print('Exitting as requested!')
            write_running_status(status_file, '0')
            pass
        except Exception as exc:
            update_log_excption(args.command, exc)
