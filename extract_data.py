#!/usr/bin/env python
# coding: utf-8

from os import listdir
from os.path import isfile, join
import json
import time
import re
from nltk.tokenize import TweetTokenizer
tokenizer = TweetTokenizer()
import os
import pandas as pd
import fasttext
import numpy as np

import random
import twython
from datetime import datetime

"""
Make sure to set the following configurations:

"""
data_path = "../crawled_data/"
OUTPUT_FOLDER = "../processed_data/"
file_name = OUTPUT_FOLDER+'data_processed.csv'
file_name_english = OUTPUT_FOLDER+'data_processed_english.csv'
keys = '../config/keys'
lang_model = fasttext.load_model("../lid.176.bin")

"""
End of configurations
"""

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

    
tweets_files = [join(data_path, f) for f in listdir(data_path) if isfile(join(data_path, f)) and f >= "output_data02-08-00" and f < "output_data03-01-00"]
tweets_files.sort()


language_dict = {'af':'afrikaans','sq':'albanian','am':'amharic','ar':'arabic','arz':'arabic','an':'aragonese','hy':'armenian','as':'assamese','av':'avaric','az':'azerbaijani','ba':'bashkir','eu':'basque','be':'belarusian','bn':'bengali','bh':'bihari','bs':'bosnian','br':'breton','bg':'bulgarian','my':'burmese','ca':'catalan','ce':'chechen','zh':'chinese','cv':'chuvash','kw':'cornish','co':'corsican','hr':'croatian','cs':'czech','da':'danish','dv':'divehi','nl':'dutch','en':'english','eo':'esperanto','et':'estonian','fi':'finnish','fr':'french','gl':'galician','ka':'georgian','de':'german','el':'greek','gn':'guarani','gu':'gujarati','ht':'haitian','he':'hebrew','hi':'hindi','hu':'hungarian','ia':'interlingua','id':'indonesian','ie':'interlingue','ga':'irish','io':'ido','is':'icelandic','it':'italian','ja':'japanese','jv':'javanese','kn':'kannada','kk':'kazakh','km':'khmer','ky':'kirghiz','kv':'komi','ko':'korean','ku':'kurdish','la':'latin','lb':'luxembourgish','li':'limburgan','lo':'lao','lt':'lithuanian','lv':'latvian','gv':'manx','mk':'macedonian','mg':'malagasy','ms':'malay','ml':'malayalam','mt':'maltese','mr':'marathi','mn':'mongolian','ne':'nepali','nn':'norwegian','no':'norwegian','oc':'occitan','or':'oriya','os':'ossetian','pa':'punjabi','fa':'persian','pl':'polish','ps':'pashto','pt':'portuguese','qu':'quechua','rm':'romansh','ro':'romanian','ru':'russian','sa':'sanskrit','sc':'sardinian','sd':'sindhi','sr':'serbian','gd':'gaelic','si':'sinhala','sk':'slovak','sl':'slovenian','so':'somali','es':'spanish','su':'sundanese','sw':'swahili','sv':'swedish','ta':'tamil','te':'telugu','tg':'tajik','th':'thai','bo':'tibetan','tk':'turkmen','tl':'tagalog','tr':'turkish','tt':'tatar','ug':'uyghur','uk':'ukrainian','ur':'urdu','uz':'uzbek','vi':'vietnamese','wa':'walloon','cy':'welsh','fy':'frisian','yi':'yiddish','yo':'yoruba', 'lang':'english'}
language_dict_inv = {"afrikaans":"af","albanian":"sq","amharic":"am","arabic":"ar","aragonese":"an","armenian":"hy","assamese":"as","avaric":"av","azerbaijani":"az","bashkir":"ba","basque":"eu","belarusian":"be","bengali":"bn","bihari":"bh","bosnian":"bs","breton":"br","bulgarian":"bg","burmese":"my","catalan":"ca","chechen":"ce","chinese":"zh","chuvash":"cv","cornish":"kw","corsican":"co","croatian":"hr","czech":"cs","danish":"da","divehi":"dv","dutch":"nl","english":"en","esperanto":"eo","estonian":"et","finnish":"fi","french":"fr","galician":"gl","georgian":"ka","german":"de","greek":"el","guarani":"gn","gujarati":"gu","haitian":"ht","hebrew":"he","hindi":"hi","hungarian":"hu","interlingua":"ia","indonesian":"id","interlingue":"ie","irish":"ga","ido":"io","icelandic":"is","italian":"it","japanese":"ja","javanese":"jv","kannada":"kn","kazakh":"kk","khmer":"km","kirghiz":"ky","komi":"kv","korean":"ko","kurdish":"ku","latin":"la","luxembourgish":"lb","limburgan":"li","lao":"lo","lithuanian":"lt","latvian":"lv","manx":"gv","macedonian":"mk","malagasy":"mg","malay":"ms","malayalam":"ml","maltese":"mt","marathi":"mr","mongolian":"mn","nepali":"ne","norwegian":"nn","occitan":"oc","oriya":"or","ossetian":"os","punjabi":"pa","persian":"fa","polish":"pl","pashto":"ps","portuguese":"pt","quechua":"qu","romansh":"rm","romanian":"ro","russian":"ru","sanskrit":"sa","sardinian":"sc","sindhi":"sd","serbian":"sr","gaelic":"gd","sinhala":"si","slovak":"sk","slovenian":"sl","somali":"so","spanish":"es","sundanese":"su","swahili":"sw","swedish":"sv","tamil":"ta","telugu":"te","tajik":"tg","thai":"th","tibetan":"bo","turkmen":"tk","tagalog":"tl","turkish":"tr","tatar":"tt","uyghur":"ug","ukrainian":"uk","urdu":"ur","uzbek":"uz","vietnamese":"vi","walloon":"wa","welsh":"cy","frisian":"fy","yiddish":"yi","yoruba":"yo"}

tweets_files

def load_api_keys(keys="keys", index=0):
    import pandas as pd
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
        if len(re.sub('[ ]+',' ',tweet_text).strip().split(' ')) > 1:
            lang = lang_model.predict(tweet_text,1,0.2)
            l = lang[0]
            p = lang[1]
            if len(l) > 0:
                a = str(l[0]).replace('label','').replace('_','')
                if a in language_dict.keys():
                    detect_lang = language_dict[a]
    except Exception as exp:
        print(exp)
        detect_lang = 'lang' #if exception occured, set langauge to lang with 0.0 confidence. (traceable for further works)
        pass
    if len(detect_lang) <= 0:
        detect_lang = 'lang'
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

def get_media_from_object(tweet_obj):
    """Extract media from a tweet object

    Args:
        tweet_obj (dict): A dictionary that is the tweet object, extended_entities or extended_tweet

    Returns:
        list: list of medias that are extracted from the tweet.
    """
    media_list = []
    if "extended_entities" in tweet_obj.keys():
        if "media" in tweet_obj["extended_entities"].keys():
            for x in tweet_obj["extended_entities"]["media"]:
                a = None
                b = None
                if "type" in x.keys():
                    a = x["type"]
                if "expanded_url" in x.keys():
                    b = x["expanded_url"]
                media_list.append((a, b))
    return media_list

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
    retweets_ids = []
    original = True
    if 'retweeted_status' in tweet.keys():
        tweet_obj = tweet["retweeted_status"]
        original = False
        retweeter = [(tweet["user"]["screen_name"]).replace('@','')]
        retweets_ids = [tweet["id"]]
        retweet_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(tweet["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
    else:
        tweet_obj = tweet
        
    tweet_n_obj = dict()
    
    tweet_n_obj['id'] = tweet_obj['id']
    tweet_n_obj['id_str'] = '\'' + str(tweet_obj['id'])
    tweet_n_obj['created_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(tweet_obj["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
    tweet_n_obj['user_screen_name'] = tweet_obj["user"]["screen_name"]
    tweet_n_obj['user_name'] = tweet_obj["user"]["name"]
    
    full_text = ""
    if 'extended_tweet' in tweet_obj.keys():
        tweet_obj['full_text'] = tweet_obj['extended_tweet']['full_text']
        tweet_obj['text'] = tweet_obj['extended_tweet']['full_text']
    if 'full_text' in tweet_obj.keys():
        full_text = tweet_obj['full_text']
    elif 'text' in tweet_obj.keys():
        full_text = tweet_obj['text']
    tweet_n_obj['full_text'] = re.sub("[\n]+"," ",re.sub("[\r\n]+"," ",full_text.strip()))
    tweet_n_obj['language'] = get_language(tweet_n_obj['full_text'])
    if original:
        tweet_n_obj["original"] = original
    
    tweet_n_obj['attr_quote_tweets'] = []
    tweet_n_obj['attr_quote_times'] = []
    tweet_n_obj['attr_quoters'] = []
    tweet_n_obj['quote_count_i'] = 0
    tweet_n_obj['attr_retweet_times'] = []
    
    tweet_n_obj['user_id'] = tweet_obj["user"]["id"]
    tweet_n_obj['users_followers_count'] = tweet_obj["user"]["followers_count"]
    tweet_n_obj['users_friends_count'] = tweet_obj["user"]["friends_count"]
    tweet_n_obj['user_location_original'] = tweet_obj["user"]["location"]
    if 'in_reply_to_status_id' in tweet_obj.keys():
        tweet_n_obj['reply_to_id'] = tweet_obj["in_reply_to_status_id"]
        tweet_n_obj['reply_to_id_str'] = "'" + str(tweet_n_obj['reply_to_id'])
    if 'quoted_status_id' in tweet_obj.keys():
        tweet_n_obj['quotation_id'] = tweet_obj["quoted_status_id"]
        tweet_n_obj['quotation_id_str'] = "'" + str(tweet_obj["quoted_status_id"])
        if "quoted_status" in tweet_obj.keys():
            if "text" in tweet_obj["quoted_status"]:
                tweet_n_obj['quotation_text'] = tweet_obj["quoted_status"]['text']
            elif "full_text" in tweet_obj["quoted_status"]:
                tweet_n_obj['quotation_text'] = tweet_obj["quoted_status"]['full_text']
    if tweet_n_obj['user_location_original'] == None:
        tweet_n_obj['user_location'] = 'not_available'
    elif len(tweet_n_obj['user_location_original']) == 0:
        tweet_n_obj['user_location'] = 'not_available'
        
    tweet_n_obj['place_full_name'] = ""
    tweet_n_obj['place_country'] = ""
    if 'place' in tweet_obj.keys():
        if tweet_obj['place'] != None and tweet_obj['place'] != '':
            if 'full_name' in tweet_obj['place'].keys():
                tweet_n_obj['place_full_name'] = tweet_obj["place"]["full_name"]
            if 'country' in tweet_obj['place'].keys():
                tweet_n_obj['place_country'] = tweet_obj["place"]["country"]
    if tweet_n_obj['place_full_name'] == "" and tweet_n_obj['place_country'] == "":
        tweet_n_obj['location_gps'] = 'not_available'
        
    tweet_n_obj['retweet_count'] = tweet_obj["retweet_count"]
    tweet_n_obj['favorite_count'] = tweet_obj["favorite_count"]
        
    if not tweet_obj["user"]["description"]:
        tweet_n_obj['users_description'] = ''
    else:
        tweet_n_obj['users_description'] = re.sub("[\n]+"," ",re.sub("[\r\n]+"," ",tweet_obj["user"]["description"]))
    
    tweet_n_obj['retweeters'] = retweeter
    tweet_n_obj['retweets_ids'] = retweets_ids
    
    if len(retweeter) > 0:
        tweet_n_obj['attr_retweet_times'] = [retweeter[0] + ' ' + retweet_time]
    else:
        tweet_n_obj['original_b'] = True
    tweet_n_obj['hashtags'] = [x for x in tokenizer.tokenize(re.sub("#"," #",full_text.strip())) if x.startswith('#')]
    tweet_n_obj['mentions'] = [x for x in tokenizer.tokenize(re.sub("@"," @",full_text.strip())) if x.startswith('@')]
    tweet_n_obj['platform'] = get_platform(tweet['source']) if 'source' in tweet.keys() else get_platform()
    tweet_n_obj['urls'] = get_urls_from_object(tweet_obj)
    tweet_n_obj['urls'] += get_urls_from_object(tweet_obj["extended_entities"]) if "extended_entities" in tweet_obj.keys() else []
    tweet_n_obj['urls'] += get_urls_from_object(tweet_obj["extended_tweet"]) if  "extended_tweet" in tweet_obj.keys() else []
    tweet_n_obj['urls'] = list(set(tweet_n_obj['urls']))
    
    tweet_n_obj['media'] = get_media_from_object(tweet_obj)
    return tweet_n_obj



combined_tweets_dict2 = dict()
from os.path import exists
if exists (OUTPUT_FOLDER+"combined_tweets_dict2"):
    with open (OUTPUT_FOLDER+"combined_tweets_dict2", "r", encoding="utf-8") as f_in:
        combined_tweets_dict2 = json.loads(f_in.readline())

def get_tweets_from_json(file_name):
    temp_dict = dict()
    print(file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        for item in f:
            object_temp = get_tweet_contents(json.loads(item))
            if object_temp['id'] not in temp_dict.keys():
                temp_dict[object_temp['id']] = object_temp
            else:
                if 'retweeters' not in temp_dict[object_temp['id']].keys():
                    temp_dict[object_temp['id']]['retweeters'] = object_temp['retweeters']
                else:
                    if object_temp['retweeters'] not in temp_dict[object_temp['id']]['retweeters']:
                        temp_dict[object_temp['id']]['retweeters'].extend(object_temp['retweeters'])
                        temp_dict[object_temp['id']]['retweeters'] = list(set(temp_dict[object_temp['id']]['retweeters']))

                if 'retweets_ids' not in temp_dict[object_temp['id']].keys() and 'retweets_ids' in object_temp.keys():
                    temp_dict[object_temp['id']]['retweets_ids'] = object_temp['retweets_ids']
                elif 'retweets_ids' in object_temp.keys():
                    if object_temp['retweets_ids'] not in temp_dict[object_temp['id']]['retweets_ids']:
                        temp_dict[object_temp['id']]['retweets_ids'].extend(object_temp['retweets_ids'])
                        temp_dict[object_temp['id']]['retweets_ids'] = list(set(temp_dict[object_temp['id']]['retweets_ids']))
                temp_dict[object_temp['id']]["retweet_count"] = object_temp["retweet_count"]
                temp_dict[object_temp['id']]["favorite_count"] = object_temp["favorite_count"]

    return temp_dict

def extract_tweets_contents(tweets_file=''):
    tweets_dict = get_tweets_from_json(tweets_file)
    for k in tweets_dict.keys():
        if k not in combined_tweets_dict2.keys():
            combined_tweets_dict2[k] = tweets_dict[k]
            combined_tweets_dict2[k]['original'] = (tweets_file.find('replies') >= 0) and tweets_dict[k]['original']




for tweets_file in tweets_files:
    extract_tweets_contents(tweets_file=tweets_file)

print('Extracting tweets from files done\nTotal tweets: {}!'.format(len(list(combined_tweets_dict2.keys()))))

replies_to_dict = dict()

for k in combined_tweets_dict2.keys():
    if combined_tweets_dict2[k]['reply_to_id'] is not None:
        id_ = combined_tweets_dict2[k]['reply_to_id']
        if id_ not in replies_to_dict.keys():
            if id_ not in combined_tweets_dict2.keys():
                replies_to_dict[id_] = {'id': k}
            else:
                combined_tweets_dict2[k]['reply_to_text'] = combined_tweets_dict2[id_]['full_text']


key_turn=0
config = load_api_keys(keys=keys, index = random.randint(0,9)%2)

if len(config['apikeys'].values()) < 1:
    print('Please make sure that you use the correct keys file: {}'.format(keys))

else:
    api_keys = list(config['apikeys'].values())[key_turn]
    app_key = api_keys['app_key']
    app_secret = api_keys['app_secret']
    oauth_token = api_keys['oauth_token']
    oauth_token_secret = api_keys['oauth_token_secret']

    TC = twython.Twython

    tc = TC(app_key, app_secret, oauth_token, oauth_token_secret)

    tweet_ids = list(replies_to_dict.keys())

    while len(tweet_ids) > 0:
        tweet_ids_slice = tweet_ids[0:100]
        tweet_ids = tweet_ids[100:]
        trials = 0
        failur = True
        while(failur and trials < 3 and len(tweet_ids) > 0):
            try:
                tweets = tc.lookup_status(id=list(tweet_ids_slice), tweet_mode="extended")
                if type(tweets) == list:
                    if len(tweets) > 0:
                        if "id" in tweets[0].keys():
                            now=datetime.now()
                            filename = os.path.abspath('%s/replies_%s'%(data_path, now.strftime('%Y%m%d')))
                            with open(filename, 'a+', newline='', encoding='utf-8') as f:
                                for tweet in tweets:
                                    id_ = tweet['id']
                                    if id_ in replies_to_dict.keys():
                                        combined_tweets_dict2[replies_to_dict[id_]['id']]['reply_to_text'] = tweet['full_text']
                                    f.write('%s\n'%json.dumps(tweet))
                            print('New collected tweets written to file: {}'.format(os.path.abspath('%s/replies_%s'%(data_path, now.strftime('%Y%m%d')))))
                    else:
                        print('No new tweets collected tweets')
                    failur = False
            except Exception as exp:
                print('Connection to Twitter API faild due to : {}\n{}'.format(str(exp), 'Retrying after 30 seconds' if trials < 2 else 'Moving to next set of ids.'))
                time.sleep(30)
                failur = True
            finally:
                trials += 1

    print('processing reply tweets of {} done!'.format(len(list(replies_to_dict.keys()))))

    with open (OUTPUT_FOLDER+"combined_tweets_dict2", "w", encoding="utf-8") as f_out:
        json.dump(combined_tweets_dict2, f_out, ensure_ascii=False)
    included_tweets_df = pd.DataFrame.from_dict(combined_tweets_dict2, orient='index')
    included_tweets_df.to_csv(file_name, mode='w', encoding='utf-8', header=True)
    included_tweets_df[included_tweets_df['language'] == 'english'].to_csv(file_name_english, mode='w', encoding='utf-8', header=True)

    print('Data written to file '.format(file_name)) 
    print('Data written to files done'.format(OUTPUT_FOLDER)) 
