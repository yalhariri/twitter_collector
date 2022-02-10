#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 11 00:00:00 2020

"""

import pysolr
import time
import twython
import copy
from util import *
from thread import *
import datetime

MAX_RETRY_CNT = 3
WAIT_TIME = 40
topic_tokens = []
lock = threading.Lock()
streamer_log='./../.log/streamer.log'
crawler_log='./../.log/crawler.log'
cache_folder="./../.cache/"
status_file = cache_folder+"status"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(streamer_log)
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class TwitterCrawler(twython.Twython):
    items = []
    def __init__(self, APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, url, uname, password, selected_fields = False, file_name_output = 'output_data'):
        if url != None and uname != None and password != None:
            self.solr = pysolr.Solr(url, auth=(uname,password), timeout=20)
        else:
            self.solr = None
        
        self.threads = []
        self.selected_fields = selected_fields
        self.file_name_output = file_name_output
        super(TwitterCrawler, self).__init__(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
        '''
        api_keys = copy.copy(kwargs.pop('api_keys', None))

        if not api_keys:
            raise Exception('api keys is missing')

        self.api_keys= copy.copy(api_keys)

        oauth2 = kwargs.pop('oauth2', True)

        if oauth2:
            api_keys.pop('oauth_token')
            api_keys.pop('oauth_token_secret')
            twitter = twython.Twython(api_keys['app_key'], api_keys['app_secret'], oauth_version=2)
            access_token = twitter.obtain_access_token()
            kwargs['access_token'] = access_token
            api_keys.pop('app_secret')

        kwargs.update(api_keys)
        url = kwargs.pop('url', None)
        uname = kwargs.pop('uname', None)
        password = kwargs.pop('password', None)
        solr = pysolr.Solr(url, auth=(uname,password), timeout=20)
        super(TwitterCrawler, self).__init__(*args, **kwargs)
        '''
        
    def write_items(self, tweets):
        print('write items! ', len(tweets) , '\t' , len(self.items))
        logger.info(str.format('write items! ' + str(len(tweets)) + '\t' + str(len(self.items))))
        global lock
        thread = Tweets_Indexer(tweets, self.solr, self.selected_fields, self.file_name_output , lock)
        self.threads.append(thread)
        thread.start()
        
    def fill_items(self, tweet):
        self.items.append(tweet)
        if len(self.items) >= 100:
            while len(self.threads) >= 5:
                time.sleep(5)
                self.threads = [t for t in self.threads if t.is_alive()]
            tweets_bulk = self.items.copy()
            self.items = []
            self.write_items(tweets_bulk)

    def rate_limit_error_occured(self, resource, api):
        rate_limits = self.get_application_rate_limit_status(resources=[resource])
        
        wait_for = int(rate_limits['resources'][resource][api]['reset']) - time.time() + WAIT_TIME
        if wait_for < 0:
            wait_for = 60
        logger.warning(str.format('rate limit reached, sleep for %d'%wait_for))
        time.sleep(wait_for)

    def search_by_query(self, query, since_id = 0, current_max_id = 0,geocode=None, lang=None):
        if not query or len(query) < 1:
            raise Exception("search: query terms is empty or not entred")
        
        prev_max_id = -1
        current_max_id = 0
        cnt = 0
        current_since_id = since_id

        retry_cnt = MAX_RETRY_CNT
        while current_max_id != prev_max_id and retry_cnt > 0:
            try:
                if current_max_id > 0:
                    tweets = self.search(q=query, geocode=geocode, since_id=since_id, lang=lang, tweet_mode='extended', max_id=current_max_id-1, result_type='recent', count=100)
                else:
                    tweets = self.search(q=query, geocode=geocode, since_id=since_id, lang=lang, tweet_mode='extended', result_type='recent', count=100)
                
                prev_max_id = current_max_id
                
                for tweet in tweets['statuses']:
                    self.fill_items(tweet)
                    if current_max_id == 0 or current_max_id > int(tweet['id']):
                        current_max_id = int(tweet['id'])
                    if current_since_id == 0 or current_since_id < int(tweet['id']):
                        current_since_id = int(tweet['id'])
                
                cnt += len(tweets['statuses'])
                
                time.sleep(1)
                
                if not read_running_status(status_file):
                    print('Exitting as requested, please wait for pending process...' , end='')
                    logger.info('Exitting as requested, please wait for pending process...')

                    self.write_items(self.items)
                    for t in self.threads:
                        t.join()
                    print('Done, bye!')
                    if self.solr != None:
                        self.solr.commit()
                    logger.info('Done, now closed!')
                    os._exit(0)

            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('search', '/search/tweets')
            except Exception as exc:
                time.sleep(10)
                update_log(crawler_log, str("exception@TwitterCrawler.search_by_query(): %s"%exc))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    #print("exceed max retry... return")
                    logger.warning("exceed max retry... return")
                    return since_id

        update_log(crawler_log, str("[%s]; since_id: [%d]; total tweets: %d "%(query, since_id, cnt)))
        return current_since_id
        
    def get_relationships_by_screen_name(self, call='/friends/list', screen_name=None):
        '''
        call: /friends/list, /followers/list, /followers/ids, and /friends/ids
        '''
        if not screen_name:
            raise Exception("user_relationship: screen_name cannot be None")

        output_folder = '.'+call
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            
        filename = os.path.abspath('%s/%s'%(output_folder, screen_name))
        
        with open(filename, 'w') as f:
            pass
        cursor = -1
        cnt = 0
        retry_cnt = MAX_RETRY_CNT
        while cursor != 0 and retry_cnt > 0:
            try:
                result = None
                if (call == '/friends/list'):
                    result = self.get_friends_list(screen_name=screen_name, cursor=cursor,count=200)
                    cnt += len(result['users'])
                elif (call == '/followers/list'):
                    result = self.get_followers_list(screen_name=screen_name, cursor=cursor,count=200)
                    cnt += len(result['users'])
                if (result):
                    cursor = result['next_cursor'] 

                    with open(filename, 'a+', newline='', encoding='utf8') as f:
                        for res_item in result['users']:
                           f.write('%s\n'%json.dumps(res_item["screen_name"], ensure_ascii=False))
                           
                time.sleep(1)
                
            except twython.exceptions.TwythonRateLimitError:
                resource_family = None
                m = re.match(r'^\/(?P<resource_family>.*?)\/', call)
                if (m):
                    resource_family = m.group('resource_family')
                self.rate_limit_error_occured(resource_family, call)
            except Exception as exc:
                time.sleep(10)
                update_log(crawler_log, str("exception: %s; when fetching screen_name: %s"%(exc, screen_name)))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    update_log(crawler_log, str("exceed max retry... return"))
                    return
        
        update_log(crawler_log, str("[%s] total [%s]: %d; "%(screen_name, call, cnt)))
        return
        
    def fetch_user_timeline_by_screen_name(self, screen_name = None, since_id = 1, include_rts=True, solr_en=False):

        if not screen_name:
            raise Exception("user_timeline: screen_name cannot be None")

        now=datetime.datetime.now()
        day_output_folder = os.path.abspath('../data/timelines/%s'%(now.strftime('%Y')))

        if not os.path.exists(day_output_folder):
            os.makedirs(day_output_folder)

        filename = os.path.abspath('%s/%s'%(day_output_folder, screen_name))

        prev_max_id = -1
        current_max_id = 0
        current_since_id = since_id

        cnt = 0

        retry_cnt = MAX_RETRY_CNT
        while (current_max_id != prev_max_id and retry_cnt > 0):
            try:
                if current_max_id > 0:
                    tweets = self.get_user_timeline(screen_name=screen_name, tweet_mode='extended', since_id = since_id, include_rts = include_rts , max_id=current_max_id - 1, count=200)
                else:
                    tweets = self.get_user_timeline(screen_name=screen_name, tweet_mode='extended', since_id = since_id, include_rts = include_rts , count=200)

                prev_max_id = current_max_id # if no new tweets are found, the prev_max_id will be the same as current_max_id
                
                tweets_dict = dict()
                with open(filename, 'a+', newline='', encoding='utf8') as f:
                    for tweet in tweets:
                        #print(json.dumps(tweet, ensure_ascii=False) + '\n')
                        #f.write('%s\n'%json.dumps(tweet, ensure_ascii=False).encode('utf8'))
                        retweeted = False
                        retweeter = []
                        if 'retweeted_status' in tweet.keys():
                            tweet_obj = tweet["retweeted_status"]
                            retweeted = True
                            retweeter = [(tweet["user"]["screen_name"]).replace('@','')]
                            if 'quoted_status' in tweet_obj.keys():
                                tweet_obj = tweet_obj["quoted_status"]
                                tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj)
                        else:
                            tweet_obj = tweet
                        tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj)
            
                        if 'quoted_status' in tweet.keys():
                            tweet_obj = tweet["quoted_status"]
                            tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj)
        
                        f.write('%s\n'%json.dumps(tweet, ensure_ascii=False))
                        if current_max_id == 0 or current_max_id > int(tweet['id']):
                            current_max_id = int(tweet['id'])
                        if current_since_id == 0 or current_since_id < int(tweet['id']):
                            current_since_id = int(tweet['id'])
                
                #add new here solr
                #no new tweets found
                if (prev_max_id == current_max_id):
                    return current_since_id, False
                time.sleep(1)


            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('statuses', '/statuses/user_timeline')
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s; when fetching screen_name: %s"%(exc, screen_name))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    with open("ERROR_LOG.txt", 'a+', newline='', encoding='utf8') as f:
                        f.write("exception: %s; when fetching screen_name: %s\n"%(exc, screen_name))
                    return since_id, True # REMOVE the user from the list of track

        logger.info("[%s] total tweets: %d; since_id: [%d]"%(screen_name, cnt, since_id))
        return current_since_id, False

    def fetch_user_timeline_by_id(self, account_id = None, since_id = 1, include_rts=True, solr_en=False):
        
        if not account_id:
            raise Exception("user_timeline: account_id cannot be None")
        
        now=datetime.datetime.now()
        day_output_folder = os.path.abspath('../data/timelines/%s'%(now.strftime('%Y')))

        if not os.path.exists(day_output_folder):
            os.makedirs(day_output_folder)
        
        filename = os.path.abspath('%s/%s'%(day_output_folder, account_id))

        prev_max_id = -1
        current_max_id = 0
        current_since_id = since_id

        cnt = 0
        
        retry_cnt = MAX_RETRY_CNT
        while (current_max_id != prev_max_id and retry_cnt > 0):
            try:
                if current_max_id > 0:
                    tweets = self.get_user_timeline(user_id=account_id, tweet_mode='extended', since_id = since_id, include_rts = include_rts , max_id=current_max_id - 1, count=200)
                else:
                    tweets = self.get_user_timeline(user_id=account_id, tweet_mode='extended', since_id = since_id, include_rts = include_rts , count=200)
                prev_max_id = current_max_id # if no new tweets are found, the prev_max_id will be the same as current_max_id
                
                tweets_dict = dict()
                
                with open(filename, 'a+', newline='', encoding='utf8') as f:
                    for tweet in tweets:
                        #print(json.dumps(tweet, ensure_ascii=False) + '\n')
                        #f.write('%s\n'%json.dumps(tweet, ensure_ascii=False).encode('utf8'))
                        retweeted = False
                        retweeter = []
                        if 'retweeted_status' in tweet.keys():
                            tweet_obj = tweet["retweeted_status"]
                            retweeted = True
                            retweeter = [(tweet["user"]["screen_name"]).replace('@','')]
                            if 'quoted_status' in tweet_obj.keys():
                                tweet_obj = tweet_obj["quoted_status"]
                                tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj)
                        else:
                            tweet_obj = tweet
                        tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj)
            
                        if 'quoted_status' in tweet.keys():
                            tweet_obj = tweet["quoted_status"]
                            tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj)
        
                        f.write('%s\n'%json.dumps(tweet, ensure_ascii=False))
                        if current_max_id == 0 or current_max_id > int(tweet['id']):
                            current_max_id = int(tweet['id'])
                        if current_since_id == 0 or current_since_id < int(tweet['id']):
                            current_since_id = int(tweet['id'])
                #add new here solr
                #no new tweets found
                if (prev_max_id == current_max_id):
                    return current_since_id, False
                time.sleep(1)


            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('statuses', '/statuses/user_timeline')
            except Exception as exc:
                time.sleep(10)
                logger.error("exception: %s; when fetching account_id: %s"%(exc, account_id))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    logger.warn("exceed max retry... return")
                    with open("ERROR_LOG.txt", 'a+', newline='', encoding='utf8') as f:
                        f.write("exception: %s; when fetching account_id: %s\n"%(exc, account_id))
                    return since_id, True # REMOVE the user from the list of track

        logger.info("[%s] total tweets: %d; since_id: [%d]"%(account_id, cnt, since_id))
        return current_since_id, False
class TwitterStreamer(twython.TwythonStreamer):
    items = []
    def __init__(self, APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, url, uname, password, selected_fields=False, file_name_output = 'output_data'):
        self.counter = 0
        self.error = 0
        self.uname = uname
        self.password = password 
        self.selected_fields = selected_fields
        self.file_name_output = file_name_output
        
        if url != None and uname != None and password != None:
            self.solr = pysolr.Solr(url, auth=(uname,password), timeout=20)
        else:
            self.solr = None
        
        self.threads = []
        #print('streamer will start')
        super(TwitterStreamer, self).__init__(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
        
    
    def write_items(self, tweets):
        print('write items! ', len(tweets) , '\t' , len(self.items))
        logger.info(str.format('write items! ' + str(len(tweets)) + '\t' + str(len(self.items))))
        global lock
        thread = Tweets_Indexer(tweets, self.solr, self.selected_fields, self.file_name_output, lock)
        self.threads.append(thread)
        thread.start()
        
        
    def fill_items(self, tweet):
        self.items.append(tweet)
        if len(self.items) >= 100:
            while len(self.threads) >= 5:
                time.sleep(5)
                self.threads = [t for t in self.threads if t.is_alive()]
            tweets_bulk = self.items.copy()
            self.items = []
            self.write_items(tweets_bulk)
         
    def check_running(self):
        if not read_running_status(status_file):
            print('Exitting as requested, please wait for pending process...' , end='')
            logger.info('Exitting as requested, please wait for pending process...')

            self.write_items(self.items)
            for t in self.threads:
                t.join()
            print('Done, bye!')
            if self.solr != None:
                self.solr.commit()
            logger.info('Done, now closed!')
            os._exit(0)
            
    def on_success(self, tweet):
        #print('streamer success!')
        self.counter += 1
        if ('id' in tweet and 'created_at' in tweet and 'user' in tweet and ('text' in tweet or 'full_text' in tweet)):
            self.fill_items(tweet)
        elif not "delete" in tweet:
            self.error += 1
            logger.warning(str('%s\n'%json.dumps(tweet,ensure_ascii=False)))
        self.check_running()

            
    def on_error(self, status_code, data,**args):
        logger.warning(str('ERROR CODE: [%s]-[%s]'%(status_code, data)))
        print(str('ERROR CODE: [%s]-[%s]'%(status_code, data)))
        self.check_running()
        if str(status_code) == '420':
            time.sleep(20)
        else:
            time.sleep(5)
        pass
        self.check_running()
        
    def close(self):
        print('streamer closed!')
        self.write_items(self.items)
        self.disconnect()
