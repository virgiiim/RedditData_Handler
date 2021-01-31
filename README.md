# RedditHandler Documentation
RedditHandler is a Python module that allows to extract and clean Reddit data (i.e., posts and/or comments), as well as to create user interactions networks from extracted data.
## RedditHandler Object
**Parameters**
+ *out_folder* (str): path of the output folder
+ *extract_post* (bool): True if you want to extract Post data, False otherwise
+ *extract_comment* (bool): True if you want to extract Comment data, False otherwise
+ *post_attributes* (list): post's attributes to be selected. The default is ['id','author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
+ *comment_attributes* (list) : comment's attributes to be selected. The default is ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']

### RedditHandler.extract_periodical_data(start_date, end_date, categories, n_months):
extract Reddit data from a list of subreddits in a specific time-period.
**Parameters**
+ *start_date* (str): beginning date in format %d/%m/%Y
+ *end_date* (str): end date in format %d/%m/%Y
+ *categories* (dict): dict with category name as key and list of subreddits in that category as value
+ *n_months* (int): integer indicating the time period considered, if you don't want it n_months = 0
**Returns**
  ciaociaociao
### RedditHandler.extract_user_data(users_list, start_date=None, end_date=None):
extract data (i.e., posts and/or comments) of one or more Reddit users 
**Parameters**
+ *users_list* (list): list with Reddit users' usernames 
+ *start_date* (str): beginning date in format %d/%m/%Y, None if you want start extracting data from Reddit beginning (i.e., 23/06/2005)
+ *end_date* (str): end date in format %d/%m/%Y, None if you want end extracting data at today date
**Returns**
### RedditHandler.create_network(start_date, end_date, categories):
**Parameters**
+ *start_date* (str): beginning date in format %d/%m/%Y
+ *end_date* (str): end date in format %d/%m/%Y
+ *categories* (dict): dict with category name as key and list of subreddits in that category as value
**Returns**

