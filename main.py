# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 13:58:01 2020

@author: virgi
"""
from src.reddit_handler import RedditHandler
from src.polarization_classifier import PolarizationClassifier
from src.textstatistics_generator import TextStatisticGenerator

# initializing RedditHandler
out_folder = 'RedditHandler_Outputs'
extract_post = True  # True if you want to extract Post data, False otherwise
extract_comment = True  # True if you want to extract Comment data, False otherwise
post_attributes = ['id', 'author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext', 'stickied',
                   'subreddit', 'subreddit_id', 'title']  # default
comment_attributes = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body',
                      'score']  # default
my_handler = RedditHandler(out_folder, extract_post, extract_comment, post_attributes=post_attributes,
                           comment_attributes=comment_attributes)

# extracting periodical data
start_date = '01/01/2021'
end_date = '31/01/2021'
category = {'finance': ['wallstreetbets']}
n_months = 1  # time_period to consider: if you don't want it n_months = 0
my_handler.extract_periodical_data(start_date, end_date, category, n_months)
my_handler.create_network(start_date, end_date, category)

# extracting user data
# users_list = ['17michela', 'BelleAriel', 'EschewObfuscation10'] # insert one or more Reddit username
# start_date = None # None if you want start extracting from Reddit beginning,
# otherwise specify a date in format %d/%m/%Y
# end_date = None # None if you want end extracting at today date, otherwise specify a date in format %d/%m/%Y
# my_handler.extract_user_data(users_list, start_date=start_date, end_date=end_date)

# initializing PolarizationClassifier
# category = {'finance': ['guncontrol']}
# start_date = '14/12/2021'
# end_date = '14/02/2019'
# file_model = 'Model/model_glove.json'
# file_weights = 'Model/model_glove.h5'
# file_tokenizer = 'Model/tokenizer_def.pickle'
# my_pol_classifier = PolarizationClassifier(out_folder, extract_post, extract_comment, category,
# start_date, end_date, file_model, file_weights, file_tokenizer)
# my_pol_classifier.compute_polarization()

# initializing TextStatisticGenerator
# category = {'gun':['guncontrol']}
# start_date = '14/12/2018'
# end_date = '14/02/2019'
# my_stats_generator = TextStatisticGenerator(out_folder, extract_post, extract_comment, category, start_date, end_date)
# my_stats_generator.extract_statistics()
