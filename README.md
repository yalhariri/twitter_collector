# twitter_collector
Twitter Collector is a tool that help collecting twitter data, storing it in JSON file, indexing it by using Solr and index the already stored text tweets into Solr.



# To run the streamer:
python twitter_streamer.py -c ../.config/keys -cmd stream_terms -tf ../.config/terms


where 
## "../.config/keys" is the file where Twitter API keys exist.
## -cmd the command parameter. It could be one of the following:

### terms to look for tweets that have any term from the list of terms.

### users to look for tweets that have any of the screen names in the list.

### search to look for tweets that has a specific list of term(s)

### get_friends look for users followers and friends.

### get_timelines streaming the tweets that any of the screen name mentioned in.

## -tf is the parameter in which where do we expect to find the terms. ../.config/terms



is the file where the terms you are intersted to collect.