#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 11 00:00:00 2020
Authors (alphabetical order): 
    Ibrahim Abu Farha i.abufarha@ed.ac.uk
    Youssef Al Hariri y.alhariri@outlook.com
"""


import streamlit as st
import pandas as pd
import sys
import os
import pysolr
import time
import altair as alt
import numpy as np
import matplotlib.pyplot as plt
import json
#from pandas.io.common import EmptyDataError
from pandas.errors import EmptyDataError

from wordcloud import WordCloud

# from crawler import run_me
from util import *

log_folder = '../.log/'
config_folder = '../.config/'
streamer_log=log_folder+'streamer'
crawler_log=log_folder+'crawler'
cache_folder="../.cache/"
status_file = cache_folder+"status_crawler"


def sort_dict(my_dict, top_n):
    if(top_n > len(my_dict.keys())):
        top_n = len(my_dict.keys())
    sorted_dict = sorted(my_dict.items(), key=lambda x: x[1], reverse=True)
    result = []
    for i in range(top_n):
        result.append(sorted_dict[i])
    return result


def query_handler(q, solr, top_n=100, rows=100000):
    traffic = dict()  # dict with date as key and list as element
    retweeters = dict()
    mentions = dict()
    authors = dict()
    hashtags = dict()
    urls = dict()
    retweeted = dict()
    res = solr.search(q, rows=rows)
    hits = res.hits

    for t in res.docs:
        ts = time.strftime(
            '%Y-%m-%d %H:00', time.strptime(t['created_at'][0], "%Y-%m-%dT%H:%M:%SZ"))
        if(ts in traffic.keys()):
            traffic[ts] += 1
        else:
            traffic[ts] = 1

        # retweeters
        if('retweeters' in t.keys()):
            for r in t['retweeters']:
                if(r not in retweeters.keys()):
                    retweeters[r] = 1
                else:
                    retweeters[r] = retweeters[r]+1

        # mentions
        if('mentions' in t.keys()):
            for m in t['mentions']:
                if(m not in mentions.keys()):
                    mentions[m] = 1
                else:
                    mentions[m] = mentions[m]+1

        # hashtags
        if('hashtags' in t.keys()):
            for h in t['hashtags']:
                if(h not in hashtags.keys()):
                    hashtags[h] = 1
                else:
                    hashtags[h] = hashtags[h]+1

        # authors
        if(t['user_screen_name'] in authors.keys()):
            authors[t['user_screen_name']] = authors[t['user_screen_name']]+1
        else:
            authors[t['user_screen_name']] = 1

        # url
        if('urls' in t.keys()):
            for u in t['urls']:
                if(u not in urls.keys()):
                    urls[u] = 1
                else:
                    urls[u] = urls[u]+1

        # retweeted
        retweeted[t['full_text']] = t['retweet_count']

    report = dict()
    report['traffic'] = traffic
    report['retweeters'] = sort_dict(retweeters, top_n)
    report['mentions'] = sort_dict(mentions, top_n)
    report['hashtags'] = sort_dict(hashtags, top_n)
    report['authors'] = sort_dict(authors, top_n)
    report['urls'] = sort_dict(urls, top_n)
    report['tweets'] = sort_dict(retweeted, top_n)

    data = pd.DataFrame(res.docs)

    return data, report, hits


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def manage_keys():
    st.title("Manage API Keys")
    
    df = load_api_keys()
    # data=st.write(df)
    st.write("## Adding new key?")
    k1 = st.text_input('API key')
    k2 = st.text_input('API secret key')
    k3 = st.text_input('Access token')
    k4 = st.text_input('Access token secret')
    if st.button("ADD key"):
        new = {'app_key': k1, 'app_secret': k2,
               'oauth_token': k3, 'oauth_token_secret': k4}
        df = df.append(new, ignore_index=True)
        #df.to_csv("./crawler/.config/keys", index=False)
        save_api_keys(df)

       # st.write(df)
    if df.shape[0]>0:
        dell = st.number_input('index of key to be deleted',value=0,step=1,min_value=0,max_value=df.shape[0]-1)
        if st.button('delete key'):
            df = df.drop(df.index[int(dell)])
            #df.to_csv("./crawler/.config/keys", index=False)
            save_api_keys(df)
        st.subheader("Available keys")
        df = load_api_keys()
        st.dataframe(df)
    else:
        st.subheader('Please add keys to be used for the API!')

    return


def get_tweets():
    st.title("Collector specifications configuration")
    st.write('In this page you can specifiy the configuration of the twitter data collector, these include the mode of operation: Search or Streaming.')
    st.write('Also, here you need to provide the set of terms or usernames that you want to collect data about:')

    
    term = st.text_input('New term')
    
    terms_list = get_terms_list('terms')
    if st.button('Add term'):
        if(not term == ''):
            if(term not in terms_list):
                terms_list.append(term)
                update_terms_list(terms_list, 'terms')
    
    
    terms_list = st.multiselect("Available terms", terms_list, terms_list)
    update_terms_list(terms_list, 'terms')
    
    keys = load_api_keys()

    mode = st.radio("What is the needed mode of operation?",
                    ('Searching', 'Streaming', 'Filter by terms'))
    if(keys.shape[0]>0):            
        if st.button('Save and run'): 
            if mode == 'Streaming':
                run_crawler(command='stream')

            elif(mode == 'Searching' and len(terms_list) == 0):
                st.subheader(
                    'You must provide terms for the search functionality!')
            elif(mode == 'Searching' and len(terms_list) > 0):
                update_terms_list(terms_list, 'terms')
                run_conf = {'mode': mode, 'tokens': terms_list}
                st.write(run_conf)
                run_crawler(command='search', terms_file='terms')
            elif(mode == 'Filter by terms' and len(terms_list) > 0):
                update_terms_list(terms_list, 'terms')
                run_conf = {'mode': mode, 'tokens': terms_list}
                st.write(run_conf)
                run_crawler(command='filter_by_terms', terms_file='terms')

    else:
        st.subheader('Please add keys to for the API!')

    if st.button('stop crawler'):
        #stop_crawler()
        write_running_status(status_file, '0')

    return


def get_timelines():
    st.title("Accounts Timelines Collector")
    st.write('In this page you can specifiy the configuration of the Twitter timelines collector.')
    st.write('Also, here you need to provide the set of the user\'s screen names that you want to collect timelines:')

    
    screen_name = st.text_input('New Screen Name')
    
    screen_name_dict = get_screen_name_dict('screen_names')
    screen_name_list = list(screen_name_dict.keys())
    if st.button('Add screen name'):
        if(not screen_name == ''):
            if(screen_name not in screen_name_list):
                screen_name_dict[screen_name] = {'screen_name': screen_name, 'since_id': 1}
                screen_name_list = list(screen_name_dict.keys())
                update_screen_name_file(screen_name_dict, 'screen_names')
    
    
    screen_name_list = st.multiselect("Available terms", screen_name_list, screen_name_list)
    update_screen_name_file(screen_name_dict, 'screen_names')
    
    keys = load_api_keys()

    mode = st.radio("What is the needed mode of operation?",
                    (['Timelines By Screen Name']))
    if(keys.shape[0]>0):            
        if st.button('Save and run'):
            if(mode == 'Timelines By Screen Name'):
                run_crawler(command='get_timelines', terms_file='screen_names')

    else:
        st.subheader('Please add keys to for the API!')

    if st.button('stop crawler'):
        #stop_crawler()
        write_running_status(status_file, '0')

    return

def about():
    st.title("Twitter Data Collector")
    st.write('This is a data collection tool designed to make this task easier for researchers who are interested in social media analysis.')
    st.write('The tool utilises twitter API and other libraries to provide a convinent data collection and storage for researchers.')
    st.write('The collected data would be stored in an index wich would privde the ability to easily retrive it and manage it, also this makes it easy to provide quick analysis of the data.')
    return


def search():
    st.title('Search and reports')
    st.write(
        'Here you can specify the search queries and feilds that you want to acheive')

    if is_non_zero_file('./crawler/.config/cores'):
        df = pd.read_csv('./crawler/.config/cores', sep='\n')
        cores_list = list(df['core_name'].values.tolist())
    else:
        cores_list = []

    selected_core = st.selectbox(
        'Choose the solr core you want to use', ['Select']+cores_list)
    if (not selected_core == 'Select'):
        if is_non_zero_file('./crawler/.config/solr'):
            with open('./crawler/.config/solr', 'r') as fp:
                current_conf = json.load(fp)
        core = current_conf['url']+selected_core
        solr = pysolr.Solr(core, timeout=30)

        query_terms = st.text_input('Terms to search for(comma seperated)')
        st.write(query_terms)
        search_option = st.selectbox(
            'Logical operation between terms', ('AND', 'OR'))
        if st.checkbox('Date range'):
            start_date = st.date_input('start').strftime('%Y-%m-%d')
            end_date = st.date_input('end').strftime('%Y-%m-%d')
            date_range = 'AND created_at:['+start_date + \
                'T00:00:00Z TO ' + end_date+'T23:59:59Z'+']'
        else:
            date_range = ''

        fields = st.multiselect('Fields to inlcude in your search', ['id', 'user_screen_name',
                                                                     'user_name', 'user_id', 'users_followers_count', 'users_friends_count', 'users_description', 'users_location',
                                                                     'retweet_count', 'favorite_count', 'full_text', 'mentions', 'retweeters', 'hashtags'], ['full_text'])

        query = prepare_query(fields=list(
            fields), date_range=date_range, terms=query_terms, operation=search_option)
        if(st.button('show query')):
            st.text(query)

        own_query = False

        if st.checkbox('Write own query'):
            own_query = True
            user_query = st.text_input('Please enter your solr query')

        num_of_rows = 100000
        if st.checkbox('specify number of rows to retreive(it might take more time, default 100K)'):
            num_of_rows = st.number_input(
                'Please enter the number of rows you want to retreive',value=0,step=1,min_value=0)

        if st.button('execute query and refresh data'):
            st.spinner('waiting...')
            if own_query:
                query = user_query
            else:
                query = prepare_query(fields=list(
                    fields), date_range=date_range, terms=query_terms, operation=search_option)

            data, report, hits = query_handler(
                solr=solr, q=query, rows=num_of_rows)
            data.to_csv('./crawler/.cache/data', index=None)
            with open('./crawler/.cache/report', 'w') as json_file:
                json.dump(report, json_file, ensure_ascii=False)
            st.success('Done!, we found '+str(hits)+' results')
        if is_non_zero_file('./crawler/.cache/data') and is_non_zero_file('./crawler/.cache/report'):
            try:
                data = pd.read_csv('./crawler/.cache/data')
            except EmptyDataError:
                data = pd.DataFrame()
            with open('./crawler/.cache/report', 'r') as fp:
                report = json.load(fp)
            terms = list(data.columns)
            if '_version_' in terms:
                terms.remove('_version_')
            fields = st.multiselect("Wanted fields", terms, terms)
            df = data[fields]
            st.dataframe(df)
            fout_name = st.text_input('Output file name')
            if st.button('export to csv'):
                df.to_csv(fout_name, index=None)
                st.success('Done!')
            traffic = report['traffic']
            timeline = pd.DataFrame(list(zip(traffic.keys(), traffic.values())), columns=[
                                    'date', 'tweet_count'])

            timeline['date'] = timeline['date'].astype('datetime64[ns]')

            chart = alt.Chart(timeline).mark_line().encode(
                alt.X('date:T', title="Date"), alt.Y('tweet_count:Q', title='Number of tweets'))
            st.write("", "", chart)
            del report['traffic']
            st.write('Report of the top authors, retweeters, tweets, and hashtags.')

            st.write('Hashtags')
            draw_word_cloud(dict(report['hashtags']))

            st.json(report)

    return


def draw_word_cloud(word_freq_dict):
    wordcloud = WordCloud(background_color='white', width=1600, height=800)
    wordcloud.generate_from_frequencies(word_freq_dict)
    fig = plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    st.pyplot(fig)



def prepare_query(fields, terms, date_range, operation):
    term_list = terms.split(',')
    if '' in term_list:
        term_list.remove('')
    query = ''
    # stop condition to stop creating wrong query
    if(len(term_list) == 0 or len(fields) == 0):
        return '*:* '+date_range

    for i, f in enumerate(fields):
        field_part = ''
        for j, t in enumerate(term_list):
            if (j < len(term_list)-1):
                field_part += f+':*'+t+'* '+operation+' '
            else:
                field_part += f+':'+t
        if(i < len(fields)-1):
            query += field_part+' '+operation+' '
        else:
            query += field_part

    query += ' '+date_range

    return query


st.sidebar.title("Twitter data collector")
option = st.sidebar.selectbox('What Would you like to do?',
                              ('About', 'Manage API Keys', 'Collect Tweets', 'Get Timelines'))


if(option == 'Manage API Keys'):
    manage_keys()


elif (option == 'Collect Tweets'):
    get_tweets()


elif (option == 'Get Timelines'):
    get_timelines()


else:
    about()


