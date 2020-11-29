# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 13:58:01 2020

@author: virgi
"""

import reddit_handler 

out_folder = 'reddit_analysis'
category = {'gun':['guncontrol'], 'politics':['EnoughTrumpSpam','Fuckthealtright']}
start_date = '23/10/2020' #included
end_date = '27/10/2020' #ecluded
n_months = 3
#default post attributes
post_attributes = ['id','author', 'created_utc', 'num_comments', 'over_18', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
#default comment attributes
comment_attributes = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']

my_handler = reddit_handler.RedditHandler(out_folder, category, start_date, end_date, n_months=n_months, post_attributes=post_attributes, comment_attributes=comment_attributes)
my_handler.extract_data()