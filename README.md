# 1. Twitter Collector
Twitter Collector is a tool that help collecting twitter data, storing it in JSON file, indexing it by using Solr and index the already stored text tweets into Solr.


# 2. Twitter Streamer
Twitter streamer is a tool that help collecting twitter data, storing it in JSON file, and if required, indexing it by using Solr and index the already stored text tweets into Solr.


          
##   a.  To run the streamer :

```
python twitter_streamer.py -c ../.config/keys -cmd stream_terms -tf ../.config/terms
```

    1. -c is the parameter in which where do we expect to find the keys. 
        Example: "../.config/keys" is the file where Twitter API keys exist.

    2. -cmd the command parameter. 
        
        It could be one of the following:
        1. terms to look for tweets that have any term from the list of terms.
        2. users to look for tweets that have any of the screen names in the list.
        3. search to look for tweets that has a specific list of term(s)
        4. get_friends look for users followers and friends.
        5. get_timelines streaming the tweets that any of the screen name mentioned in.

        3. -tf is the parameter in which where do we expect to find the terms. 
            Example: "../.config/terms"
    
    