# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 13:58:01 2020

@author: virgi
"""
from src import reddit_handler

out_folder = 'reddit_analysis'
category = {'gun':['guncontrol'], 'politics':['EnoughTrumpSpam']}
start_date = '23/10/2019' #included
end_date = '27/01/2020' #excluded
n_months = 1 #time_period to consider: if you don't want it n_months = 0
#default post attributes
post_attributes = ['id','author', 'created_utc', 'num_comments', 'over_18', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
#default comment attributes
comment_attributes = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']

my_handler = reddit_handler.RedditHandler(out_folder, category, start_date, end_date, n_months=n_months, post_attributes=post_attributes, comment_attributes=comment_attributes)
my_handler.extract_data()
