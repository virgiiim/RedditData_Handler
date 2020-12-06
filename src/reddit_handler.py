import datetime
from dateutil.relativedelta import relativedelta
import time
import requests
import json
import random
import os
import pandas as pd
#for text cleaning
import string
import re
import shutil

class RedditHandler:
    ''' 
    class responsible for extracting and processing reddit data and the creation of users' network
    '''
    def __init__(self, out_folder, extract_post, extract_comment, categories, start_date, end_date, n_months=1, post_attributes=['id','author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title'], comment_attributes=['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']):
        '''
        Parameters
        ----------
        out_folder : str
            path of the output folder
        extract_post: bool
            True if you want to extract Post data, False otherwise
        extract_comment : bool
            True if you want to extract Comment data, False otherwise
        categories : dict
            dict with category name as key and list of subreddits in that category as value
        start_date : str
            beginning date in format %d/%m/%Y
        end_date : str
            end date in format %d/%m/%Y
        n_months : int
            integer inicating the time period considered 
        post_attributes : list, optional
            post's attributes to be selected. The default is ['id','author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
        comment_attributes : list, optional
            comment's attributes to be selected. The default is ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']
        '''

        self.out_folder = out_folder
        if not os.path.exists(self.out_folder):
            os.mkdir(self.out_folder)
        self.extract_post = extract_post
        self.extract_comment = extract_comment
        self.categories = categories
        # transforming date in a suitable format for folder name (category)
        self.pretty_start_date = start_date.replace('/','-')
        self.pretty_end_date = end_date.replace('/','-')
        self.real_start_date = start_date # TODO metti nome piu carino
        self.real_end_date = end_date # TODO metti nome piu carino
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
        try:
            r = requests.get(url) # Response Object
            time.sleep(random.random()*0.02) 
            data = json.loads(r.text) # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            return self._post_request_API(start_date, end_date, subreddit)
        return data['data'] # data['data'] contains list of posts   


    def _comment_request_API(self, start_date, end_date, subreddit):
        '''
        API REQUEST to pushishift.io/reddit/comment
        returns a list of 1000 dictionaries where each of them is a comment
        '''
        url = 'https://api.pushshift.io/reddit/search/comment?&size=500&after='+str(start_date)+'&before='+str(end_date)+'&subreddit='+str(subreddit)
        try:
            r = requests.get(url) # Response Object
            time.sleep(random.random()*0.02) 
            data = json.loads(r.text) # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            return self._comment_request_API(start_date, end_date, subreddit)
        return data['data'] # data['data'] contains list of comments  

        
    def _clean_raw_text(self, text):
        '''
        Clean raw post/comment text with standard preprocessing pipeline
        '''
        # Lowercasing text
        text = text.lower()
        # Removing not printable characters 
        text = ''.join(filter(lambda x:x in string.printable, text))
        # Removing XSLT tags
        text = re.sub(r'&lt;/?[a-z]+&gt;', '', text)
        text = text.replace(r'&amp;', 'and')
        text = text.replace(r'&gt;', '') # TODO: try another way to strip xslt tags
        # Removing newline, tabs and special reddit words
        text = text.replace('\n',' ')
        text = text.replace('\t',' ')
        text = text.replace('[deleted]','').replace('[removed]','')
        # Removing URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        # Removing numbers
        text = re.sub(r'\w*\d+\w*', '', text)
        # Removing Punctuation
        text = text.translate(str.maketrans('', '', string.punctuation))
        # Removing extra spaces
        text = re.sub(r'\s{2,}', " ", text)
        # Stop words? Emoji?
        return text

    
    def extract_data(self):
        '''
        extract Reddit data from a list of subreddits in a specific time-period
        '''
        raw_data_folder = os.path.join(self.out_folder, 'Categories_raw_data')
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
                end_period = datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y") + relativedelta(months=+self.n_months)
                period_post = (datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y"), end_period)
                period_comment = (datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y"), end_period)
                # first call to API
                posts = self._post_request_API(current_date_post, self.end_date, sub) 
                comments = self._comment_request_API(current_date_post, self.end_date, sub) 
     
                if self.extract_post:
                # extracting posts
                    while len(posts) > 0: #collecting data until reaching the end_date
                        # TODO: check if sub exists!
                        for raw_post in posts: 
                            if raw_post['author'] not in ['[deleted]', 'AutoModerator']: # discarding data concerning removed users and moderators
                                user_id = raw_post['author']
                                if user_id not in users.keys():
                                    if self.extract_post and self.extract_comment:
                                        users[user_id] = {'posts':[], 'comments':[]}
                                    else:
                                        users[user_id] = {'posts':[]}
                                post = dict() #dict to store posts
                                # adding field category
                                post['category'] = category
                                # adding field date in a readable format
                                post['date'] = datetime.datetime.utcfromtimestamp(raw_post['created_utc']).strftime("%d/%m/%Y")
                                # cleaning body field
                                merged_text = raw_post['title']+' '+raw_post['selftext']
                                post['clean_text'] = self._clean_raw_text(merged_text)
                                # adding field time_period in a readable format
                                if datetime.datetime.strptime(post['date'], "%d/%m/%Y") >= period_post[1]:
                                    period_post = (period_post[1], period_post[1] + relativedelta(months=+self.n_months))
                                if self.n_months != 0: 
                                    post['time_period'] = (period_post[0].strftime('%d/%m/%Y'), period_post[1].strftime('%d/%m/%Y')) 
                                else:
                                    post['time_period'] = (datetime.datetime.utcfromtimestamp(self.start_date).strftime("%d/%m/%Y"),datetime.datetime.utcfromtimestamp(self.end_date).strftime("%d/%m/%Y"))
                                # selecting fields 
                                for attr in self.post_attributes: 
                                    if attr not in raw_post.keys(): #handling missing values
                                        post[attr] = None
                                    elif (attr != 'selftext') and (attr != 'title'): # saving only clean text
                                        post[attr] = raw_post[attr]
                                users[user_id]['posts'].append(post)
                        current_date_post = posts[-1]['created_utc'] # taking the UNIX timestamp date of the last record extracted
                        posts = self._post_request_API(current_date_post, self.end_date, sub) 
                        pretty_current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime('%Y-%m-%d')
                        print(f'Extracted posts until date: {pretty_current_date_post}')
                if self.extract_comment:
                    # Extracting comments
                    while len(comments) > 0:
                        for raw_comment in comments: 
                            if raw_comment['author'] not in ['[deleted]', 'AutoModerator']:
                                user_id = raw_comment['author']
                                if user_id not in users.keys():
                                    if self.extract_post and self.extract_comment:
                                        users[user_id] = {'posts':[], 'comments':[]} 
                                    else:
                                        users[user_id] = {'comments':[]} 
                                comment = dict() # dict to store a comment
                                # adding field category
                                comment['category'] = category
                                # adding field date in a readable format
                                comment['date'] = datetime.datetime.utcfromtimestamp(raw_comment['created_utc']).strftime("%d/%m/%Y")
                                # cleaning body field
                                comment['clean_text'] = self._clean_raw_text(raw_comment['body'])
                                # adding time_period fieldin a readable format
                                if datetime.datetime.strptime(comment['date'], "%d/%m/%Y") >= period_comment[1]:
                                    period_comment = (period_comment[1], period_comment[1] + relativedelta(months=+self.n_months))
                                if self.n_months != 0: 
                                    comment['time_period'] = (period_comment[0].strftime('%d/%m/%Y'), period_comment[1].strftime('%d/%m/%Y')) 
                                else:
                                    comment['time_period'] = (datetime.datetime.utcfromtimestamp(self.start_date).strftime("%d/%m/%Y"),datetime.datetime.utcfromtimestamp(self.end_date).strftime("%d/%m/%Y"))
                                # selecting fields
                                for attr in self.comment_attributes: 
                                    if attr not in raw_comment.keys(): #handling missing values
                                        comment[attr] = None
                                    elif attr != 'body': # saving only clean text
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
            print('n_users in', categories_keys[i],':', len(list(users.keys())))
            # for each user in a category a json file
            for user in users: 
                user_filename = os.path.join(path_category, f'{user}.json')
                with open(user_filename, 'w') as fp:
                    json.dump(users[user], fp, sort_keys=True, indent=4)
            shutil.make_archive(path_category, 'zip', path_category) 
            shutil.rmtree(path_category)
            print('Done to extract data from category:', categories_keys[i])
            i+=1 #to iter over elements
    

    def create_network(self): 
        '''
        crate users' interaction network based on comments:
        type of network: directed and weighted by number of interactions 
        self loop are allowed
        '''
        if not self.extract_comment: # if user want to create users interactions networks it is necessary to extract also comments in extract_data()
            raise ValueError('To create users interactions Networks you have to set self.extract_comment to True')
        # TODO: now doesn't work if n_motnhs = 0
        # creating folder with network data
        user_network_folder = os.path.join(self.out_folder, 'Categories_networks')
        if not os.path.exists(user_network_folder):
            os.mkdir(user_network_folder)
        #
        path = os.path.join(self.out_folder, 'Categories_raw_data')
        categories = os.listdir(path) # returns list
        # unzip files
        unzipped_categories = list()
        for category in categories: 
            category_name = ''.join([i for i in category if i.isalpha()]).replace('zip','')
            if (category_name in list(self.categories.keys())) and (self.pretty_start_date in category) and (self.pretty_end_date in category):
                file_name = f'{path}\\{category}'
                unzipped_filename = file_name.replace(f'{path}','').replace('\\','').replace('.zip','')
                extract_dir = file_name.replace('.zip','')
                shutil.unpack_archive(file_name, extract_dir, 'zip') 
                unzipped_categories.append(unzipped_filename)
        print('unzipped:', unzipped_categories)

        # Saving data: for each category a folder 
        for category in unzipped_categories:
            network_category = os.path.join(user_network_folder, f'{category}')
            if not os.path.exists(network_category):
                os.mkdir(network_category)
            path_category = os.path.join(path, category) 
            users_list = os.listdir(path_category) # for each category a list of all file in that category (i mean of each user)
            period = (datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y"), datetime.datetime.strptime(self.real_start_date, "%d/%m/%Y") + relativedelta(months=+self.n_months))
            users = dict() #dict to store users posts_ids and comment in a period
            while period[1] <= datetime.datetime.strptime(self.real_end_date, "%d/%m/%Y"):
                print('PERIODO:', period[0],period[1])
                parent_cnt = 0
                for user in users_list: 
                    user_filename = os.path.join(path_category, user)
                    submission_ids = list()
                    comment_ids = list()
                    parent_ids = list()
                    with open(user_filename, 'r') as f:
                        user_data = json.load(f)
                        for comment in user_data['comments']:
                            if (datetime.datetime.strptime(comment['time_period'][0], "%d/%m/%Y") >= period[0]) and (datetime.datetime.strptime(comment['time_period'][1], "%d/%m/%Y") <= period[1]): 
                                #print(comment['time_period'])
                                comment_ids.append(comment['id'])
                                parent_ids.append(comment['parent_id'])
                        for post in user_data['posts']:
                            if (datetime.datetime.strptime(post['time_period'][0], "%d/%m/%Y") >= period[0]) and (datetime.datetime.strptime(post['time_period'][1], "%d/%m/%Y") <= period[1]): 
                                submission_ids.append(post['id'])
                    pretty_username = user.replace('.json','')
                    if (len(submission_ids) > 0) or (len(comment_ids) > 0):
                        users[pretty_username] = {'post_ids': submission_ids, 'comment_ids': comment_ids, 'parent_ids':parent_ids}
                    parent_cnt+=len(parent_ids)
                interactions = dict()
                nodes = set()
                for user in users:
                    for parent_id in users[user]['parent_ids']:
                        if "t3" in parent_id: #  it is a comment to a post
                            for other_user in users:
                                if parent_id.replace("t3_","") in users[other_user]['post_ids']:
                                    nodes.add(other_user)
                                    nodes.add(user)
                                    if user != other_user: # avoiding self loops
                                        if (user,other_user) not in interactions.keys(): 
                                            interactions[(user,other_user)] = 0
                                        interactions[(user,other_user)] +=1
                        elif "t1" in parent_id: # it is a comment to another comment
                            for other_user in users:
                                if parent_id.replace("t1_","") in users[other_user]['comment_ids']: 
                                    nodes.add(other_user)
                                    nodes.add(user)
                                    if user != other_user: # avoiding self loops
                                        if (user,other_user) not in interactions.keys():
                                            interactions[(user,other_user)] = 0
                                        interactions[(user,other_user)] +=1
                print('n_edges', len(interactions))
                print('n_nodes', len(nodes))
                # creating edge list csv file
                nodes_from = list()
                nodes_to = list()
                edges_weight = list()
                for interaction in interactions:
                    nodes_from.append(interaction[0])
                    nodes_to.append(interaction[1])
                    edges_weight.append(interactions[interaction])
                tmp = {'Source':nodes_from,'Target':nodes_to,'Weight':edges_weight}
                edge_list =  pd.DataFrame(tmp)
                # saving csv in category folder
                pretty_period0 = period[0].strftime('%d/%m/%Y').replace('/','-')
                pretty_period1 = period[1].strftime('%d/%m/%Y').replace('/','-')
                last_path = os.path.join(network_category, f'{category_name}_{pretty_period0}_{pretty_period1}.csv')
                edge_list.to_csv(last_path, index =False)
                users = dict() #emptying dict to store the next period
                period = (period[1], period[1] + relativedelta(months=+self.n_months)) #changing period

if __name__ == '__main__':
    cwd = os.getcwd()
    out_folder = os.path.join(cwd, 'RedditHandler_Outputs')
    out_folder = 'RedditHandler_Outputs'
    extract_post = True
    extract_comment = True
    category = {'gun':['guncontrol'], 'politics':['EnoughTrumpSpam','Fuckthealtright']}
    start_date = '20/12/2018'
    end_date = '20/01/2019'
    n_months = 1
    #default post attributes
    post_attributes = ['id','author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
    #default comment attributes
    comment_attributes = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']
    my_handler = RedditHandler(out_folder, extract_post, extract_comment, category, start_date, end_date, n_months=n_months, post_attributes=post_attributes, comment_attributes=comment_attributes)
    my_handler.extract_data()
    my_handler.create_network()
