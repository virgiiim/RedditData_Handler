import datetime
from dateutil.relativedelta import relativedelta
import time
import requests
import json
import pickle
import os

class RedditHandler:
    ''' 
    class responsible for extracting and processing reddit data and the creation of users' network
    '''
    def __init__(self, out_folder, categories, start_date, end_date, n_months=1, post_attributes=['author', 'created_utc', 'id', 'num_comments', 'over_18', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title'], comment_attributes=['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']):
        '''
        Parameters
        ----------
        out_folder : str
            path of the output folder
        categories: dict
            dict with category name as key and list of subreddits in that category as value
        start_date : str
            beginning date in format %d/%m/%Y
        end_date : str
            end date in format %d/%m/%Y
        n_months: int
            integer inicating the time period considered 
        post_attributes : list, optional
            post's attributes to be selected. The default is ['author', 'created_utc', 'id', 'num_comments', 'over_18', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title'].
        comment_attributes: list, optional
            comment's attributes to be selected. The default is ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']
        '''

        self.out_folder = out_folder
        if not os.path.exists(self.out_folder):
            os.mkdir(self.out_folder)
        self.categories = categories
        # transforming date in a suitable format for folder name (category)
        self.pretty_start_date = start_date.replace('/','-')
        self.pretty_end_date = end_date.replace('/','-')
        self.real_start_date = start_date # TODO metti nome piu carino
        # converting date from format %d/%m/%Y to UNIX timestamp as requested by API
        self.start_date = int(time.mktime(datetime.datetime.strptime(start_date, "%d/%m/%Y").timetuple()))
        self.end_date = int(time.mktime(datetime.datetime.strptime(end_date, "%d/%m/%Y").timetuple()))
        self.n_months = n_months
        self.post_attributes = post_attributes 
        self.comment_attributes = comment_attributes 
   
    def _post_request_API(self, start_date, end_date, subreddit):
        '''
        API REQUEST to pushishift.io/reddit/submission
        returns a list of 1000 dictionaries where each of them is a post 
        '''
        url = 'https://api.pushshift.io/reddit/search/submission?&size=500&after='+str(start_date)+'&before='+str(end_date)+'&subreddit='+str(subreddit)
        r = requests.get(url) # Response Object
        data = json.loads(r.text) # r.text is a JSON object, converted into dict
        return data['data'] # data['data'] contains list of posts  

    def _comment_request_API(self, start_date, end_date, subreddit):
        '''
        API REQUEST to pushishift.io/reddit/submission
        returns a list of 1000 dictionaries where each of them is a post 
        '''
        url = 'https://api.pushshift.io/reddit/search/comment?&size=500&after='+str(start_date)+'&before='+str(end_date)+'&subreddit='+str(subreddit)
        r = requests.get(url) # Response Object
        data = json.loads(r.text) # r.text is a JSON object, converted into dict
        return data['data'] # data['data'] contains list of posts  
    
    def extract_data(self):
        '''
        extract Reddit data from a list of subreddits in a specific time-period
        '''
        raw_data_folder = os.path.join(self.out_folder, 'post_raw_data')
        if not os.path.exists(raw_data_folder):
            os.mkdir(raw_data_folder)
        categories_keys = list(self.categories.keys())
        i = 0 #to iter over categories keys
        for category in self.categories.keys():
            print(f'Extracting category: {categories_keys[i]}')
            users = {} #users with post & comment shared in different subreddit belonging to the same category
            for sub in self.categories[category]:
                print(f'Extracting subbredit: {sub}')
                current_date_post = self.start_date
                current_date_comment = self.start_date
                # handling time-period
                end_period = datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y") + relativedelta(months=+n_months)
                period_post = (datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y"), end_period)
                period_comment = (datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y"), end_period)
                # first call to API
                posts = self._post_request_API(current_date_post, self.end_date, sub) 
                comments = self._comment_request_API(current_date_post, self.end_date, sub) 
                # extracting posts
                while len(posts) > 0: #collecting data until reaching the end_date
                    # TODO: check if sub exists!
                    for raw_post in posts: 
                        if raw_post['author'] not in ['[deleted]', 'AutoModerator']: # discarding data concerning removed users and moderators
                            user_id = raw_post['author']
                            if user_id not in users.keys():
                                users[user_id] = {'posts':[], 'comments':[]}
                            post = dict() #dict to store posts
                            # adding field category
                            post['category'] = category
                            # adding field date in a readable format
                            post['date'] = datetime.datetime.utcfromtimestamp(raw_post['created_utc']).strftime("%d/%m/%Y")
                            # adding field time_period in a readable format
                            if datetime.datetime.strptime(post['date'], "%d/%m/%Y") >= period_post[1]:
                                period_post = (period_post[1], period_post[1] + relativedelta(months=+n_months))
                            if self.n_months != 0: 
                                post['time_period'] = (period_post[0].strftime('%m/%d/%Y'), period_post[1].strftime('%m/%d/%Y')) 
                            else:
                                post['time_period'] = (datetime.datetime.utcfromtimestamp(self.start_date).strftime("%d/%m/%Y"),datetime.datetime.utcfromtimestamp(self.end_date).strftime("%d/%m/%Y"))
                            # selecting fields 
                            for attr in self.post_attributes: 
                                if attr not in raw_post.keys(): #handling missing values
                                    post[attr] = None
                                else:
                                    post[attr] = raw_post[attr]
                            users[user_id]['posts'].append(post)
                    current_date_post = posts[-1]['created_utc'] # taking the UNIX timestamp date of the last record extracted
                    posts = self._post_request_API(current_date_post, self.end_date, sub) 
                    pretty_current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime('%Y-%m-%d')
                    print(f'Extracted posts until date: {pretty_current_date_post}')
                # Extracting comments
                while len(comments) > 0:
                    for raw_comment in comments: 
                        if raw_comment['author'] not in ['[deleted]', 'AutoModerator']:
                            user_id = raw_comment['author']
                            if user_id not in users.keys():
                                users[user_id] = {'posts':[], 'comments':[]} 
                            comment = dict() # dict to store a comment
                            # adding field category
                            comment['category'] = category
                            # adding field date in a readable format
                            comment['date'] = datetime.datetime.utcfromtimestamp(raw_comment['created_utc']).strftime("%d/%m/%Y")
                            # adding time_period fieldin a readable format
                            if datetime.datetime.strptime(comment['date'], "%d/%m/%Y") >= period_comment[1]:
                                period_comment = (period_comment[1], period_comment[1] + relativedelta(months=+n_months))
                            if self.n_months != 0: 
                                comment['time_period'] = (period_comment[0].strftime('%m/%d/%Y'), period_comment[1].strftime('%m/%d/%Y')) 
                            else:
                                comment['time_period'] = (datetime.datetime.utcfromtimestamp(self.start_date).strftime("%d/%m/%Y"),datetime.datetime.utcfromtimestamp(self.end_date).strftime("%d/%m/%Y"))
                            # selecting fields
                            for attr in self.comment_attributes: 
                                if attr not in raw_comment.keys(): #handling missing values
                                    comment[attr] = None
                                else:
                                    comment[attr] = raw_comment[attr]
                            users[user_id]['comments'].append(comment)
                    current_date_comment = comments[-1]['created_utc'] # taking the UNIX timestamp date of the last record extracted
                    comments = self._comment_request_API(current_date_comment, self.end_date, sub) 
                    pretty_current_date_comment = datetime.datetime.utcfromtimestamp(current_date_comment).strftime('%Y-%m-%d')
                    print(f'Extracted comments until date: {pretty_current_date_comment}')
                print(f'Finished data extraction for subreddit {sub}')
            # Saving data: for each category a folder 
            path_category = os.path.join(raw_data_folder, f'{categories_keys[i]}_{self.pretty_start_date}_{self.pretty_end_date}')
            if not os.path.exists(path_category):
                os.mkdir(path_category)
            print('n_utenti in', categories_keys[i],':', len(list(users.keys())))
            # for each user in a category a json file
            for user in users: 
                user_filename = os.path.join(path_category, f'{user}.json')
                with open(user_filename, 'w') as fp:
                    json.dump(users[user], fp, sort_keys=True, indent=4)
            print('Done to extract data from category:', categories_keys[i])
            i+=1 #to iter over elements
    
    def clean_data(self):
        '''
        clean Reddit textual data with standard text preprocessing pipeline
        ''' 

    def create_network(self):
        '''
        crate users' interaction network based on comments 
        '''

if __name__ == '__main__':
    cwd = os.getcwd()
    out_folder = os.path.join(cwd, 'reddit_analysis')
    out_folder = 'reddit_analysis'
    category = {'gun':['guncontrol'], 'politics':['EnoughTrumpSpam','Fuckthealtright']}
    start_date = '23/10/2019'
    end_date = '27/01/2020'
    n_months = 1
    #default post attributes
    post_attributes = ['id','author', 'created_utc', 'num_comments', 'over_18', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
    #default comment attributes
    comment_attributes = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']
    my_handler = RedditHandler(out_folder, category, start_date, end_date, n_months=n_months, post_attributes=post_attributes, comment_attributes=comment_attributes)
    my_handler.extract_data()