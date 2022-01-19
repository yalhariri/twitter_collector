## To delete a solr core:
#python3 main_run.py -cmd delete_all_data -c Scotland -u solr_admin -p admin_2018

#python3 main_run.py -cmd get_tweets_from_files -solr true -fname files1 -c Scotland -sf true -u solr_admin -p admin_2018
#python3 main_run.py -cmd get_tweets_from_files -solr true -fname files2 -c Scotland -sf true -u solr_admin -p admin_2018
#python3 main_run.py -cmd get_tweets_from_files -solr true -fname files3 -c Scotland -sf true -u solr_admin -p admin_2018


## Run a streamer to wrtie to file
#python3 main_run.py -cmd start_streaming -solr false  -sf true -u solr_admin -p admin_2018

## Run a a streamer to wrtie to solr
#python3 main_run.py -cmd start_streaming -solr false  -sf false -u solr_admin -p admin_2018


## Run a friend crawler to wrtie to solr
#python3 main_run.py -cmd get_friends -solr false -sf false -u solr_admin -p admin_2018

#python3 main_run.py -cmd start_crawling -solr false -sf false -u solr_admin -p admin_2018
#python3 main_run.py -cmd start_streaming_lang -solr false  -sf false -u solr_admin -p admin_2018 #todiscove later (terms with specific language)



#time lines crawler
#python3 main_run.py -cmd get_timelines -solr false -sf false -u solr_admin -p admin_2018

python3 twitter_crawler.py -cmd search -tf terms -solr false -sf false