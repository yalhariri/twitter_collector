#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 15:39:05 2020

@author: yalha
"""
import threading 
import json
import time
import re
import nltk
import random
tokenizer = nltk.TweetTokenizer()
import fasttext
import requests
import os
from os import path, listdir, remove
from os.path import exists, join

import logging
from logging.handlers import TimedRotatingFileHandler as _TimedRotatingFileHandler
import gzip
import shutil
import datetime


config_folder = "./../.config/"
lang_detect_file = config_folder+"lang_detect"
current_conf = ''
with open(lang_detect_file, 'r') as fp:
    current_conf = json.load(fp)

class Tweets_Indexer(threading.Thread):
    def __init__(self, tweets, solr, selected_fields, file_name_output, lock, core=None, solr_active=True): #TODO: in Quds server make solr_active=False
        threading.Thread.__init__(self) 
        self.lock = lock
        self.tweets = tweets
        self.solr = solr
        self.core = core
        self.selected_fields = selected_fields
        self.file_name_output = file_name_output
        
        if solr != None:
            self.solr_active = solr_active
        else:
            self.solr_active = False
        
    def write_to_solr(self, tweets):
        """A function to write the tweets object to file and to solr (if solr_active is True).

        Args:
            tweets (dict): A dictionary that holds all the relevant information from the tweets.
        """
        file_name = 'other_terms.json'
        if self.solr_active:   
            file_name = 'solr_'+self.core+'.json'
            missed_data_written = 0
            Error_occured = False
            #sys.exit(-1)
            #if self.selected_fields:
            tweets_list = [tweets[k] for k in tweets.keys()]
            #else:
            #    tweets_list = tweets
            try:
                for item in tweets_list:
                    if "_version_" in item.keys():
                        item.pop('_version_')
                
                status = ''
                i = 0
                while ('"status">0<' not in status and i < 3):
                    status = self.solr.add(tweets_list, fieldUpdates={'retweeters':'add-distinct', 'sentiment':'set', 'sentiment_distribution':'set', 'language':'set', 'features':'set', 'topic':'set', 'user_location':'set', 'location_gps':'set', 'user_location_original':'set', 'location_language':'set', 'place':'set', 'hashtags':'add-distinct', 'urls':'add-distinct', 'retweet_count':'set','favorite_count':'set', 'emotion':'set', 'emotion_distribution':'set'})
                    i+=1
                if '"status">0<' not in status: #Only add the missed information when solr not accessed proberly.
                    Error_occured = True
                    self.write_data_to_file(tweets, folder='missed_data')
                    missed_data_written = 1
                else:
                    missed_data_written = 2
            except Exception:
                if missed_data_written != 1 and Error_occured == True:
                    self.write_data_to_file(tweets, folder='missed_data')
                    missed_data_written = 1
                    logger.warning('exception at write to solr 001: Process continued, and missed data recorded in missed data.')
                pass
            if missed_data_written == 2:
                logger.info('No exception occured, Data has been written in solr')
        else:
            logger.info('solr not activated.')
        try:
            self.write_data_to_file(tweets, folder='crawled_data')
            logger.info('Data has been written in crawled_data.')
        except Exception as exp:
            logger.warning('Exception at write data to file! ' + exp)
            pass
    
    def write_data_to_file(self, tweets, folder):
        """A function to write data into a file

        Args:
            tweets (dict): the dictionary of the tweets with their extracted information.
            file_name (str): the file name in which the data will be writen to.
            folder (str): the folder in which the file will be written to.
        """
        if not path.exists('../'+folder):
            os.mkdir('../'+folder)
        
        now = datetime.datetime.now()
        nows = now.strftime("%m-%d-%H")
        out_put_file = folder + '/' + self.file_name_output + str(nows)
        with open('../'+out_put_file,'a+',encoding='utf-8') as fout:
            for k in tweets.keys():
                fout.write('%s\n'%json.dumps(tweets[k], ensure_ascii=False))
        
    def run(self):
        if self.selected_fields:
            draft_tweets = extract_tweets_info(self.tweets)
            if self.solr:
                self.write_to_solr(draft_tweets)
            
        else:
            draft_tweets  = {tweet['id']: tweet for tweet in self.tweets}
            self.write_data_to_file(draft_tweets, folder='crawled_data')


class TimedRotatingFileHandler(_TimedRotatingFileHandler):
    """A class to manage the backup compression.

    Args:
        _TimedRotatingFileHandler ([type]): [description]
    """
    def __init__(self, filename="", when="midnight", interval=1, backupCount=0):
        super(TimedRotatingFileHandler, self).__init__(
            filename=filename,
            when=when,
            interval=int(interval),
            backupCount=int(backupCount))
    
    def doRollover(self):
        super(TimedRotatingFileHandler, self).doRollover()
        '''
        dirname, basename = os.path.split(self.baseFilename)
        if Tweets_Indexer.core == 'Google':
            to_compress = [
                    join(dirname, f) for f in listdir(dirname) if f.startswith(
                        basename) and not f.endswith((".gz", ".log"))]
            for f in to_compress:
                if exists(f):
                    with open(f, "rb") as _old, gzip.open(f + ".gz", "wb") as _new:
                        shutil.copyfileobj(_old, _new)
                    remove(f)
        '''
        for directory in ['../missed_data', '../crawled_data']:
            if not os.path.exists(directory):
                os.makedirs(directory)
            to_compress = [
                join(directory, f) for f in listdir(directory) if not f.endswith((".gz", ".log"))]
            for f in to_compress:
                now = datetime.datetime.now()
                nows = now.strftime("%Y-%m-%d-%H-%M-%S")
                if exists(f):
                    with open(f, "rb") as _old, gzip.open(f + nows + ".gz", "wb") as _new:
                        shutil.copyfileobj(_old, _new)
                    remove(f)

log_folder = './../.log/'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
logger = logging.getLogger(__name__)
filename = log_folder + __name__  + '.log'
file_handler = TimedRotatingFileHandler(filename=filename, when='midnight', interval=1, backupCount=0)#when midnight, s (seconds), M (minutes)... etc
#file_handler = TimedRotatingFileHandler(filename=filename, when='M', interval=2, backupCount=0)#when midnight, s (seconds), M (minutes)... etc
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)
file_name = current_conf['file']
lang_model = fasttext.load_model(file_name)
language_dict = {'af':'afrikaans','sq':'albanian','am':'amharic','ar':'arabic','arz':'arabic','an':'aragonese','hy':'armenian','as':'assamese','av':'avaric','az':'azerbaijani','ba':'bashkir','eu':'basque','be':'belarusian','bn':'bengali','bh':'bihari','bs':'bosnian','br':'breton','bg':'bulgarian','my':'burmese','ca':'catalan','ce':'chechen','zh':'chinese','cv':'chuvash','kw':'cornish','co':'corsican','hr':'croatian','cs':'czech','da':'danish','dv':'divehi','nl':'dutch','en':'english','eo':'esperanto','et':'estonian','fi':'finnish','fr':'french','gl':'galician','ka':'georgian','de':'german','el':'greek','gn':'guarani','gu':'gujarati','ht':'haitian','he':'hebrew','hi':'hindi','hu':'hungarian','ia':'interlingua','id':'indonesian','ie':'interlingue','ga':'irish','io':'ido','is':'icelandic','it':'italian','ja':'japanese','jv':'javanese','kn':'kannada','kk':'kazakh','km':'khmer','ky':'kirghiz','kv':'komi','ko':'korean','ku':'kurdish','la':'latin','lb':'luxembourgish','li':'limburgan','lo':'lao','lt':'lithuanian','lv':'latvian','gv':'manx','mk':'macedonian','mg':'malagasy','ms':'malay','ml':'malayalam','mt':'maltese','mr':'marathi','mn':'mongolian','ne':'nepali','nn':'norwegian','no':'norwegian','oc':'occitan','or':'oriya','os':'ossetian','pa':'punjabi','fa':'persian','pl':'polish','ps':'pashto','pt':'portuguese','qu':'quechua','rm':'romansh','ro':'romanian','ru':'russian','sa':'sanskrit','sc':'sardinian','sd':'sindhi','sr':'serbian','gd':'gaelic','si':'sinhala','sk':'slovak','sl':'slovenian','so':'somali','es':'spanish','su':'sundanese','sw':'swahili','sv':'swedish','ta':'tamil','te':'telugu','tg':'tajik','th':'thai','bo':'tibetan','tk':'turkmen','tl':'tagalog','tr':'turkish','tt':'tatar','ug':'uyghur','uk':'ukrainian','ur':'urdu','uz':'uzbek','vi':'vietnamese','wa':'walloon','cy':'welsh','fy':'frisian','yi':'yiddish','yo':'yoruba', 'lang':'english'}
language_dict_inv = {"afrikaans":"af","albanian":"sq","amharic":"am","arabic":"ar","aragonese":"an","armenian":"hy","assamese":"as","avaric":"av","azerbaijani":"az","bashkir":"ba","basque":"eu","belarusian":"be","bengali":"bn","bihari":"bh","bosnian":"bs","breton":"br","bulgarian":"bg","burmese":"my","catalan":"ca","chechen":"ce","chinese":"zh","chuvash":"cv","cornish":"kw","corsican":"co","croatian":"hr","czech":"cs","danish":"da","divehi":"dv","dutch":"nl","english":"en","esperanto":"eo","estonian":"et","finnish":"fi","french":"fr","galician":"gl","georgian":"ka","german":"de","greek":"el","guarani":"gn","gujarati":"gu","haitian":"ht","hebrew":"he","hindi":"hi","hungarian":"hu","interlingua":"ia","indonesian":"id","interlingue":"ie","irish":"ga","ido":"io","icelandic":"is","italian":"it","japanese":"ja","javanese":"jv","kannada":"kn","kazakh":"kk","khmer":"km","kirghiz":"ky","komi":"kv","korean":"ko","kurdish":"ku","latin":"la","luxembourgish":"lb","limburgan":"li","lao":"lo","lithuanian":"lt","latvian":"lv","manx":"gv","macedonian":"mk","malagasy":"mg","malay":"ms","malayalam":"ml","maltese":"mt","marathi":"mr","mongolian":"mn","nepali":"ne","norwegian":"nn","occitan":"oc","oriya":"or","ossetian":"os","punjabi":"pa","persian":"fa","polish":"pl","pashto":"ps","portuguese":"pt","quechua":"qu","romansh":"rm","romanian":"ro","russian":"ru","sanskrit":"sa","sardinian":"sc","sindhi":"sd","serbian":"sr","gaelic":"gd","sinhala":"si","slovak":"sk","slovenian":"sl","somali":"so","spanish":"es","sundanese":"su","swahili":"sw","swedish":"sv","tamil":"ta","telugu":"te","tajik":"tg","thai":"th","tibetan":"bo","turkmen":"tk","tagalog":"tl","turkish":"tr","tatar":"tt","uyghur":"ug","ukrainian":"uk","urdu":"ur","uzbek":"uz","vietnamese":"vi","walloon":"wa","welsh":"cy","frisian":"fy","yiddish":"yi","yoruba":"yo"}


def get_sentiments(tweets):
    """A function to access sentiment analysis service.

    'id': tweet['id'], "full_text": item["full_text"], "language"

    Args:
        tweets (dict): A dictionary of the tweets object. It should have the following keys:
        1) 'id': tweet id, 
        2) 'full_text': the full_text of the tweet,
        3) 'language': the detected language of the tweet.

    Returns:
        dict: A dictionary that hold the sentiment information as retrived from its service. The keys are the tweets ids and values are dicts that contain:
        'sentiment' : the sentiment information as being analysed from the text, (positive, nuetral or negative)
        'sentiment_distribution' : a list that has the distribution of the three sentiments (the highest would be at the index of the selected sentiment)
    """
    headers = {'content-type': 'application/json; charset=utf-8'}
    #url = 'http://127.0.0.1:10001/v1.0/sentiment' #Huawei Project
    url = 'http://127.0.0.1:7777/api/predict' #Other Project
    data = json.dumps(tweets, ensure_ascii=False)
    rs = -1
    trials = 1
    while (rs != 200 and trials <= 3):
        try:
            response = requests.post(url=url, headers = headers , data=data.encode('utf-8'))
            rs = response.status_code
        except Exception:
            rs = -1
            time.sleep(random.randrange(1,3))
        finally:
            trials += 1
    if rs != 200:
        logger.warning('Sentiment analyzer not found. Error code: ' + str(rs))
        return None
    return json.loads(response.content)
    
    
def get_topics_features(tweets):
    """A prototype function to extract the features from the tweets' text.

    Args:
        tweets ([type]): not implemented yet.

    Returns:
        dict: dictionary that hold the topics and features (currently with default values).
    """
    #TODO: get from topic detection api?
    return {x['id']: {'topic' : 'Tpc_1', 'features' : 'FTR_1'} for x in tweets}

def get_location(tweets):
    """A function to access location service.

    Args:
        tweets (dict): A dictionary of the tweets object. It should have the following keys:
        1) 'id': tweet id, 
        2) 'user': the user object as exists in the tweet object,
        3) 'geo': the geo field from the tweet,
        4) 'coordinates': the coordinates field from the tweet, 
        5) 'place': the place field from the tweet, 
        6) 'language': the detected language of the tweet.

    Returns:
        dict: A dictionary that hold the location information as retrived from location service. The keys are the tweets ids and values are dicts that contain
        'user' : the location information from user object
        'tweet' : the location information from the tweet object (location_gps)
        'language' (optional): the location as extracted from the tweets' language
    """
    url1 = "http://127.0.0.1:10000/api/get_locations"
    #url1= "http://185.32.253.54:10000/api/get_locations"
    '''
    with open('sample.json' , 'a' , encoding='utf-8') as fout:
        fout.write('%s\n'%json.dumps(tweets,ensure_ascii=False))
    '''
    data = json.dumps(tweets,ensure_ascii=False)
    headers = {'content-type': 'application/json; charset=utf-8'}
    # sending get request and saving the response as response object
    rs = -1
    trials = 1
    while (rs != 200 and trials <= 3):
        try:
            response = requests.post(url=url1, data=data.encode('utf-8'), headers=headers)
            rs = response.status_code
        except Exception:
            rs = -1
        finally:
            trials += 1
    if rs != 200:
        logger.warning('Location service not found. Error code: ' + str(rs))
        return None
    return json.loads(response.content)

def extract_tweets_info(tweets):
    """A function to extract the contents of the passed tweets. This function calls the function get_tweet_contents to extract the data from the tweet object, then it access other services to get the relevant info.

    Args:
        tweets (dict): A dictionary that is the tweet object.

    Returns:
        dict: A dictionary that holds the extracted tweets with their relevant information. 
    """
    draft_tweets = dict()
    loc_dict = dict()
    sentiment = dict()
    feature = []
    for tweet in tweets:
        item = get_tweet_contents(tweet)
        item['language'] = get_language(item['full_text'])
        #print(item['language'])
        draft_tweets[item['id']] = item
        loc_dict[tweet['id']] = {'id': tweet['id'], 'user': tweet['user'], 'geo': tweet['geo'] , 'coordinates': tweet['coordinates'] , 'place': tweet['place'] , 'language': item['language'] }
        try:
            #sentiment[tweet["id"]] = {'id': tweet['id'], "full_text": item["full_text"], "language":item['language'][0][0]}
            sentiment[tweet["id"]] = {'id': tweet['id'], "full_text": item["full_text"], "language":language_dict_inv[item['language'][0][0]]}
        except Exception:
            sentiment[tweet["id"]] = {'id': tweet['id'], "full_text": item["full_text"], "language":'en'}
            pass
        feature.append({'id': item['id'], 'full_text': item['full_text']})
    
    
    locations = get_location(loc_dict)
    if locations != None:
        for k in locations.keys():
            if int(k) in draft_tweets.keys():
                draft_tweets[int(k)]['user_location'] = locations[k]['user']
                draft_tweets[int(k)]['location_gps'] = locations[k]['tweet']
                if 'language' in locations[k].keys():
                    draft_tweets[int(k)]['location_language'] = locations[k]['language']
    try:
        #print(sentiment)
        sentiments = get_sentiments(sentiment)
    except:
        sentiments = None
    if sentiments != None:
        for k in sentiments.keys():
            item = sentiments[k]
            #print(item)
            if int(k) in draft_tweets.keys():
                draft_tweets[int(k)]['sentiment'] = item    #TODO Here we change from dict to string... think how to optimize it!
                '''
                draft_tweets[int(k)]['sentiment'] = item['sentiment']
                draft_tweets[int(k)]['sentiment_distribution'] = [item['sentiment_distribution']['negative'], item['sentiment_distribution']['neutral'], item['sentiment_distribution']['positive']]
                if 'emotion' in item.keys():
                    draft_tweets[int(k)]['emotion'] = item['emotion']
                    draft_tweets[int(k)]['emotion_distribution'] = item['emotion_distribution']
                '''

    features = get_topics_features(feature)
    for k in features.keys():
        if k in draft_tweets.keys():
            draft_tweets[k]['topic'] = features[k]['topic']
            draft_tweets[k]['features'] = features[k]['features']
    return draft_tweets

def get_language(tweet_text):
    """function to extract the language of the passed string.
    It is based on fasttext language identification and uses the libraries (fasttext, re) in python.
    Proceudre:
        1- remove hashtags, mentions and urls
        2- remove non-alpha characters
        3- predict the language
        4- in case of errors, return english with 0 confidence.
    Args:
        tweet_text (str): The string that you need to find its language.

    Returns:
        List: List of lists that contains the identified language with its considence. examples: [['english',0.9]] or [['english',0.6],['spanish',0.3]]
    """
    detect_lang = []
    try:
        tweet_text = re.sub('[@#][^ ]+','',re.sub('http[s]:[^ ]+','',tweet_text))
        tweet_text = re.sub('[ًٌٍَُِّْ]+','',tweet_text)
        tmp = ' '.join([x for x in tweet_text.split() if str.isalpha(x) and not x.startswith('#') and not x.startswith('@')])
        if len(tmp.split()) > 3:
            tweet_text = tmp    #this to avoid removing Ch/Ja/Ta strings
        lang = lang_model.predict(tweet_text,1,0.2)
        l = lang[0]
        p = lang[1]
        for i in range(len(l)):
            a = str(l[i]).replace('label','').replace('_','')
            if a in language_dict.keys():
                a = language_dict[a] 
                b = p[i]
                detect_lang.append([a, b])
    except Exception as exp:
        logger.warning('Identifying language failed. Error code: ' + str(exp))
        detect_lang = [['lang', 0.0]] #if exception occured, set langauge to lang with 0.0 confidence. (traceable for further works)
        pass
    if len(detect_lang) <= 0:
        detect_lang = [['lang', 0.0]]
    return detect_lang

def get_urls_from_object(tweet_obj):
    """Extract urls from a tweet object

    Args:
        tweet_obj (dict): A dictionary that is the tweet object, extended_entities or extended_tweet

    Returns:
        list: list of urls that are extracted from the tweet.
    """
    url_list = []
    if "entities" in tweet_obj.keys():
        if "urls" in tweet_obj["entities"].keys():
            for x in tweet_obj["entities"]["urls"]:
                try:
                    url_list.append(x["expanded_url"]  if "expanded_url" in x.keys() else x["url"])
                except Exception:
                    pass
    return url_list

def get_platform(source = '<PLT_1>'):
    """A function to extract the platform from a source string.

    Args:
        source (str, optional): source string that is usually contains the platform that is used to post the tweet. Defaults to '<PLT_1>'.

    Returns:
        str: the platform if found, otherwise the stamp PLT_1. This stamp is used for any further updates.
    """
    platform = 'PLT_1'
    try:
        platform = re.sub('[<>]', '\t', source).split('\t')[2]
        platform = platform.replace('Twitter for','').replace('Twitter','')
    except:
        platform = 'PLT_1'
    return platform.strip()

def get_tweet_contents(tweet):
    """A function to extract the contents of the passed tweet object.

    Args:
        tweet (dict): A dictionary that is the tweet object.

    Returns:
        dict: A dictionary that holds the extracted information.
    """
    tweet_obj = dict()
    retweeter = []        
    if 'retweeted_status' in tweet.keys():
        tweet_obj = tweet["retweeted_status"]
        retweeter = [(tweet["user"]["screen_name"]).replace('@','')]
    else:
        tweet_obj = tweet
    tweet_n_obj = dict()
    tweet_n_obj['id'] = tweet_obj['id']
    tweet_n_obj['created_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(tweet_obj["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
    tweet_n_obj['user_screen_name'] = tweet_obj["user"]["screen_name"]
    tweet_n_obj['user_name'] = tweet_obj["user"]["name"]
    tweet_n_obj['user_id'] = tweet_obj["user"]["id"]
    tweet_n_obj['users_followers_count'] = tweet_obj["user"]["followers_count"]
    tweet_n_obj['users_friends_count'] = tweet_obj["user"]["friends_count"]
    tweet_n_obj['user_location_original'] = tweet_obj["user"]["location"]
    if 'place' in tweet_obj.keys():
        if tweet_obj['place'] != None:
            if 'full_name' in tweet_obj['place'].keys():
                tweet_n_obj['place'] = tweet_obj["place"]["full_name"]
    tweet_n_obj['retweet_count'] = tweet_obj["retweet_count"]
    tweet_n_obj['favorite_count'] = tweet_obj["favorite_count"]

    if not tweet_obj["user"]["description"]:
        tweet_n_obj['users_description'] = ''
    else:
        tweet_n_obj['users_description'] = re.sub("[\n]+"," ",re.sub("[\r\n]+"," ",tweet_obj["user"]["description"]))
    full_text = ""
    if 'extended_tweet' in tweet_obj.keys():
        tweet_obj['full_text'] = tweet_obj['extended_tweet']['full_text']
        tweet_obj['text'] = tweet_obj['extended_tweet']['full_text']
    if 'full_text' in tweet_obj.keys():
        full_text = tweet_obj['full_text']
    elif 'text' in tweet_obj.keys():
        full_text = tweet_obj['text']
    
    tweet_n_obj['retweeters'] = retweeter
    tweet_n_obj['full_text'] = re.sub("[\n]+"," ",re.sub("[\r\n]+"," ",full_text.strip()))
    tweet_n_obj['hashtags'] = [x for x in tokenizer.tokenize(re.sub("#"," #",full_text.strip())) if x.startswith('#')]
    tweet_n_obj['mentions'] = [x for x in tokenizer.tokenize(re.sub("@"," @",full_text.strip())) if x.startswith('@')]
    tweet_n_obj['platform'] = get_platform(tweet['source']) if 'source' in tweet.keys() else get_platform()
    tweet_n_obj['urls'] = get_urls_from_object(tweet_obj)
    tweet_n_obj['urls'] += get_urls_from_object(tweet_obj["extended_entities"]) if "extended_entities" in tweet_obj.keys() else []
    tweet_n_obj['urls'] += get_urls_from_object(tweet_obj["extended_tweet"]) if  "extended_tweet" in tweet_obj.keys() else []
    tweet_n_obj['urls'] = list(set(tweet_n_obj['urls']))
    
    tweet_n_obj['domain'] = ''
    return tweet_n_obj
