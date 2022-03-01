#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sat Jan 11 00:00:00 2020

"""


import sys
import os
import time
import itertools
import json
from twitter_classes import TwitterCrawler, TwitterStreamer
from util import *

WAIT_TIME = 30
log_folder = './../.log/'
log_config = './../.config/'
streamer_log=log_folder+'streamer'
crawler_log=log_folder+'crawler'
cache_folder="./../.cache/"
status_file = cache_folder+"status"

if not os.path.isdir(log_folder):
    os.mkdir(log_folder)
if not os.path.isdir(log_config):
    os.mkdir(log_config)
        
def collect_tweets_by_search_terms(config, url, uname, password, terms_file, selected_fields = False, key_turn=0):
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
                twitterCralwer = TwitterCrawler(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields)
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

def collect_public_tweets(config, url, uname, password, selected_fields = False, key_turn=0):
    #api_keys = list(config['api_keys'].values()).pop()
    api_keys = list(config['apikeys'].values())[key_turn]
    
    if len(api_keys) > 0:
        app_key = api_keys['app_key']
        app_secret = api_keys['app_secret']
        oauth_token = api_keys['oauth_token']
        oauth_token_secret = api_keys['oauth_token_secret']
        
        streamer = TwitterStreamer(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields)
        print("start collecting.....")
        streamer.statuses.sample(tweet_mode='extended')

def filter_by_terms(config, url, uname, password, terms_file=None, selected_fields = False, key_turn=0):
    """Function to start the streamer with terms list.

    Args:
        config (dict): Python dictionary that holds configuration details. Mainly for Twitter api keys.
        url (str): solr url (combined with core)
        uname (str): solr admin username
        password (str): solr admin password
        terms_file (str, optional): path to terms list. Defaults to None.
        key_turn (int, optional): a switcher to select the Twitter api. It is useful to pass Twitter API limits. Defaults to 0.
    """
        
    if terms_file == None:
        print('Please include the terms file!\nStreamer ended...')
        return
    api_keys = list(config['apikeys'].values())[key_turn]
    if len(api_keys) > 0:
        app_key = api_keys['app_key']
        app_secret = api_keys['app_secret']
        oauth_token = api_keys['oauth_token']
        oauth_token_secret = api_keys['oauth_token_secret']
        terms_list = get_terms_list(terms_file)
        #print('\nterms_list: ', terms_list)
        #print('\nstatus: ', status_file)
        streamer = TwitterStreamer(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields)
        streamer.statuses.filter(track=terms_list,tweet_mode='extended')



def filter_by_language(config, url, uname, password, terms_file=None, lang_file = None , selected_fields = False, key_turn=0):
    """Function to start the streamer with terms list and specific language.

    Args:
        config (dict): Python dictionary that holds configuration details. Mainly for Twitter api keys.
        url (str): solr url (combined with core)
        uname (str): solr admin username
        password (str): solr admin password
        terms_file (str, required): path to terms list file (line splitted). Defaults to None.
        lang_file (str, required): path to languages list file (line splitted). Defaults to None.
        key_turn (int, optional): a switcher to select the Twitter api keys. It is useful to pass Twitter API limits. Defaults to 0.
    """
        
    if terms_file == None:
        print('Please include the terms file!\nStreamer ended...')
        return
    if lang_file == None:
        print('Please include the language file!\nStreamer ended...')
    api_keys = list(config['apikeys'].values())[key_turn]
    if len(api_keys) > 0:
        app_key = api_keys['app_key']
        app_secret = api_keys['app_secret']
        oauth_token = api_keys['oauth_token']
        oauth_token_secret = api_keys['oauth_token_secret']
        terms_list = get_terms_list(terms_file)
        lang_list = get_terms_list(lang_file)
        print('\nterms_list: ', terms_list)
        print('\nlang_list: ', lang_list)
        streamer = TwitterStreamer(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields=selected_fields)
        streamer.statuses.filter(languages = lang_list, track=terms_list, tweet_mode='extended')

def filter_by_screen_names(config, url, uname, password, terms_file=None, selected_fields = False, key_turn=0):
    """Function to start crawler by using terms list.

    Args:
        config (dict): Python dictionary that holds configuration details. Mainly for Twitter api keys.
        url (str): solr url (combined with core)
        uname (str): solr admin username
        password (str): solr admin password
        terms_file (str, optional): path to terms list. Defaults to None.
        key_turn (int, optional): a switcher to select the Twitter api. It is useful to pass Twitter API limits. Defaults to 0.
    """
    if terms_file == None:
        print('Please include the terms file!\nStreamer ended...')
        return
    #api_keys = list(config['apikeys'].values()).pop()
    api_keys = list(config['apikeys'].values())[key_turn]
    
    if len(api_keys) > 0:
        app_key = api_keys['app_key']
        app_secret = api_keys['app_secret']
        oauth_token = api_keys['oauth_token']
        oauth_token_secret = api_keys['oauth_token_secret']
        terms_list = get_terms_list(terms_file)
        #print('\nterms_list: ', terms_list)
        streamer = TwitterStreamer(app_key, app_secret, oauth_token, oauth_token_secret, url, uname, password, selected_fields)
        streamer.statuses.filter(follow=terms_list,tweet_mode='extended')



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
            update_log(crawler_log,str(util.full_stack()))
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


def update_log_excption(command, exc):
    if (command == 'terms'):
        update_log(streamer_log, exc)
    if (command == 'search'):
        update_log(crawler_log, exc)
        

if __name__== "__main__":
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="config.json that contains twitter api keys;")
    parser.add_argument('-cmd','--command', help="commands: search, terms, stream_accounts, get_timeline")
    parser.add_argument('-tf','--terms_file', help="terms list to seach for")
    parser.add_argument('-sf','--selected_fields', help="Write all tweet object or selected fields only", default=False)
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
        
    print(args.url, '\t', config)
    if len(config)>0:
        kRun = write_running_status(status_file,'1')
        try:
            while(kRun):
                try:
                    key_turn=(key_turn+1)%len(config['apikeys'])
                    kRun = read_running_status(status_file)
                    if (args.command == 'terms'):
                        filter_by_terms(config, args.url, args.uname, args.password, args.terms_file, args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'lang_terms'):
                        filter_by_language(config, args.url, args.uname, args.password, args.terms_file, args.lang_file, args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'users_screen_name'):
                        filter_by_screen_names(config, args.url, args.uname, args.password, args.terms_file, args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'search'):
                        collect_tweets_by_search_terms(config, args.url, args.uname, args.password, args.terms_file, args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'get_friends'):
                        get_friends(config, args.url, args.uname, args.password, screen_name_config_filepath=args.terms_file, selected_fields = args.selected_fields, key_turn=key_turn)
                    elif (args.command == 'get_timelines'):
                        collect_timeline_by_screen_name(config, args.url, args.uname, args.password, screen_name_file=args.terms_file, selected_fields = args.selected_fields, key_turn=key_turn)
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
                        print('hello')
                        print('Exitting as requested.')
        except KeyboardInterrupt:
            print('hello2')
            print('Exitting as requested!')
            pass
        except Exception as exc:
            update_log_excption(args.command, exc)
