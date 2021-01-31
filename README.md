# RedditData_Handler
RedditHandler is a Python module that allows to extract and clean Reddit data (i.e., posts and/or comments), as well as to create user interactions networks from extracted data.
## RedditHandler Object
**Parameters**
+ *out_folder* (str): path of the output folder
+ *extract_post* (bool): True if you want to extract Post data, False otherwise
+ *extract_comment* (bool): True if you want to extract Comment data, False otherwise
+ *post_attributes* (list): post's attributes to be selected. The default is ['id','author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
+ *comment_attributes* (list) : comment's attributes to be selected. The default is ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']
        '
