import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import time
import requests
import json
import random
import os
import os.path
import shutil
import zipfile
import string
import re

__author__ = "Virginia Morini"


def clean_raw_text(text):
    """
    Clean raw post/comment text with standard preprocessing pipeline
    """
    # Lowercasing text
    text = text.lower()
    # Removing not printable characters
    text = ''.join(filter(lambda x: x in string.printable, text))
    # Removing XSLT tags
    text = re.sub(r'&lt;/?[a-z]+&gt;', '', text)
    text = text.replace(r'&amp;', 'and')
    text = text.replace(r'&gt;', '')
    # Removing newline, tabs and special reddit words
    text = text.replace('\n', ' ')
    text = text.replace('\t', ' ')
    text = text.replace('[deleted]', '').replace('[removed]', '')
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


class RedditHandler:
    """
    class responsible for extracting and processing reddit data and the creation of users' network
    """

    def __init__(self, out_folder, extract_post, extract_comment,
                 post_attributes=('id', 'author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score',
                                  'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title'),
                 comment_attributes=('id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id',
                                     'body', 'score')):
        """
        Parameters
        ----------
        out_folder : str
            path of the output folder
        extract_post: bool
            True if you want to extract Post data, False otherwise
        extract_comment : bool
            True if you want to extract Comment data, False otherwise
        post_attributes : list, optional
            post's attributes to be selected. The default is ['id','author', 'created_utc', 'num_comments', 'over_18',
            'is_self', 'score', 'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title']
        comment_attributes : list, optional
            comment's attributes to be selected. The default is ['id', 'author', 'created_utc', 'link_id',
            'parent_id', 'subreddit', 'subreddit_id', 'body', 'score']
        """

        self.out_folder = out_folder
        if not os.path.exists(self.out_folder):
            os.mkdir(self.out_folder)
        self.extract_post = extract_post
        self.extract_comment = extract_comment
        self.post_attributes = post_attributes
        self.comment_attributes = comment_attributes

    def __post_request_API_periodical(self, start_date, end_date, subreddit):
        """
        API REQUEST to pushishift.io/reddit/submission
        returns a list of 1000 dictionaries where each of them is a post
        """
        url = 'https://api.pushshift.io/reddit/search/submission?&size=500&after=' + str(start_date) + '&before=' + str(
            end_date) + '&subreddit=' + str(subreddit)
        try:
            r = requests.get(url)  # Response Object
            time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            return self.__post_request_API_periodical(start_date, end_date, subreddit)
        return data['data']  # data['data'] contains list of posts

    def __post_request_API_user(self, start_date, end_date, username):
        """
        API REQUEST to pushishift.io/reddit/submission
        returns a list of 1000 dictionaries where each of them is a post
        """
        url = 'https://api.pushshift.io/reddit/search/submission?&size=500&after=' + str(start_date) + '&before=' + str(
            end_date) + '&author=' + str(username)
        try:
            r = requests.get(url)  # Response Object
            time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            return self.__post_request_API_user(start_date, end_date, username)
        return data['data']  # data['data'] contains list of posts

    def __comment_request_API_periodical(self, start_date, end_date, subreddit):
        """
        API REQUEST to pushishift.io/reddit/comment
        returns a list of 1000 dictionaries where each of them is a comment
        """
        url = 'https://api.pushshift.io/reddit/search/comment?&size=500&after=' + str(start_date) + '&before=' + str(
            end_date) + '&subreddit=' + str(subreddit)
        try:
            r = requests.get(url)  # Response Object
            time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            return self.__comment_request_API_periodical(start_date, end_date, subreddit)
        return data['data']  # data['data'] contains list of comments

    def __comment_request_API_user(self, start_date, end_date, username):
        """
        API REQUEST to pushishift.io/reddit/comment
        returns a list of 1000 dictionaries where each of them is a comment
        """
        url = 'https://api.pushshift.io/reddit/search/comment?&size=500&after=' + str(start_date) + '&before=' + str(
            end_date) + '&author=' + str(username)
        try:
            r = requests.get(url)  # Response Object
            time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            return self.__comment_request_API_user(start_date, end_date, username)
        return data['data']  # data['data'] contains list of comments

    def extract_periodical_data(self, start_date, end_date, categories, n_months):
        """
        extract Reddit data from a list of subreddits in a specific time-period

        Parameters
        ----------
        start_date : str
            beginning date in format %d/%m/%Y
        end_date : str
            end date in format %d/%m/%Y
        categories : dict
            dict with category name as key and list of subreddits in that category as value
        n_months : int
            integer indicating the time period considered, if you don't want it n_months = 0
        """

        path_category = ""
        # formatting date
        pretty_start_date = start_date.replace('/', '-')
        pretty_end_date = end_date.replace('/', '-')
        real_start_date = start_date
        real_end_date = end_date
        # converting date from format %d/%m/%Y to UNIX timestamp as requested by API
        start_date = int(time.mktime(datetime.datetime.strptime(start_date, "%d/%m/%Y").timetuple()))
        end_date = int(time.mktime(datetime.datetime.strptime(end_date, "%d/%m/%Y").timetuple()))
        raw_data_folder = os.path.join(self.out_folder, 'Categories_raw_data')
        if not os.path.exists(raw_data_folder):
            os.mkdir(raw_data_folder)
        categories_keys = list(categories.keys())
        i = 0  # to iter over categories keys
        for category in categories:
            print(f'Extracting category: {categories_keys[i]}')
            users = dict()  # users with post & comment shared in different subreddit belonging to the same category
            for sub in categories[category]:
                print(f'Extracting subbredit: {sub}')
                current_date_post = start_date
                current_date_comment = start_date
                # handling time-period
                if n_months == 0:
                    period_post = (datetime.datetime.strptime(real_start_date, "%d/%m/%Y"),
                                   datetime.datetime.strptime(real_end_date, "%d/%m/%Y"))
                    period_comment = (datetime.datetime.strptime(real_start_date, "%d/%m/%Y"),
                                      datetime.datetime.strptime(real_end_date, "%d/%m/%Y"))
                else:
                    end_period = datetime.datetime.strptime(real_start_date, "%d/%m/%Y") + relativedelta(
                        months=+n_months)
                    period_post = (datetime.datetime.strptime(real_start_date, "%d/%m/%Y"), end_period)
                    period_comment = (datetime.datetime.strptime(real_start_date, "%d/%m/%Y"), end_period)

                # extracting posts
                if self.extract_post:
                    posts = self.__post_request_API_periodical(current_date_post, end_date, sub)  # first call to API
                    while len(
                            posts) > 0:  # collecting data until there are no more posts to extract in the time
                        # period considered
                        # TODO: check if sub exists!
                        for raw_post in posts:
                            # saving posts for each period
                            current_date = datetime.datetime.utcfromtimestamp(raw_post['created_utc']).strftime(
                                "%d/%m/%Y")
                            condition1_post = datetime.datetime.strptime(current_date, "%d/%m/%Y") >= period_post[1]
                            condition2_post = (datetime.datetime.strptime(current_date, "%d/%m/%Y") + relativedelta(
                                days=+1)) >= datetime.datetime.strptime(real_end_date, "%d/%m/%Y")
                            if condition1_post or condition2_post:
                                # Saving data: for each category a folder
                                path_category = os.path.join(raw_data_folder,
                                                             f'{categories_keys[i]}_{pretty_start_date}_'
                                                             f'{pretty_end_date}')
                                if not os.path.exists(path_category):
                                    os.mkdir(path_category)
                                pretty_period0_post = period_post[0].strftime('%d/%m/%Y').replace('/', '-')
                                pretty_period1_post = period_post[1].strftime('%d/%m/%Y').replace('/', '-')
                                path_period_category = os.path.join(path_category,
                                                                    f'{categories_keys[i]}_{pretty_period0_post}_'
                                                                    f'{pretty_period1_post}')
                                if not os.path.exists(path_period_category):
                                    os.mkdir(path_period_category)
                                # for each user in a period category a json file
                                for user in users:
                                    user_filename = os.path.join(path_period_category, f'{user}.json')
                                    if os.path.exists(user_filename):
                                        with open(user_filename) as fp:
                                            data = json.loads(fp.read())
                                            data['posts'].extend(users[user]['posts'])
                                        with open(user_filename, 'w') as fp:
                                            json.dump(data, fp, sort_keys=True, indent=4)
                                    else:
                                        with open(user_filename, 'w') as fp:
                                            json.dump(users[user], fp, sort_keys=True, indent=4)
                                # users = dict()
                                if condition1_post:
                                    period_post = (period_post[1], period_post[1] + relativedelta(months=+n_months))
                                    print('PERIOD_POST', period_post)
                                elif condition2_post:
                                    break

                            # collecting posts
                            if raw_post['author'] not in ['[deleted]',
                                                          'AutoModerator']:  # discarding data concerning removed
                                # users and moderators
                                user_id = raw_post['author']
                                post = dict()  # dict to store posts
                                # adding field category
                                post['category'] = category
                                # adding field date in a readable format
                                post['date'] = datetime.datetime.utcfromtimestamp(raw_post['created_utc']).strftime(
                                    "%d/%m/%Y")
                                # cleaning body field
                                merged_text = raw_post['title'] + ' ' + raw_post['selftext']
                                post['clean_text'] = clean_raw_text(merged_text)
                                # adding field time_period in a readable format
                                if n_months != 0:
                                    post['time_period'] = (
                                        period_post[0].strftime('%d/%m/%Y'), period_post[1].strftime('%d/%m/%Y'))
                                else:
                                    post['time_period'] = (
                                        datetime.datetime.utcfromtimestamp(start_date).strftime("%d/%m/%Y"),
                                        datetime.datetime.utcfromtimestamp(end_date).strftime("%d/%m/%Y"))
                                # selecting fields 
                                for attr in self.post_attributes:
                                    if attr not in raw_post:  # handling missing values
                                        post[attr] = None
                                    elif (attr != 'selftext') and (attr != 'title'):  # saving only clean text
                                        post[attr] = raw_post[attr]
                                if len(post['clean_text']) > 2:  # avoiding empty posts
                                    if user_id not in users:
                                        if self.extract_post and self.extract_comment:
                                            users[user_id] = {'posts': [], 'comments': []}
                                        else:
                                            users[user_id] = {'posts': []}
                                    users[user_id]['posts'].append(post)
                        current_date_post = posts[-1][
                            'created_utc']  # taking the UNIX timestamp date of the last record extracted
                        posts = self.__post_request_API_periodical(current_date_post, end_date, sub)
                        pretty_current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime(
                            '%Y-%m-%d')
                        print(f'Extracted posts until date: {pretty_current_date_post}')

                # extracting comments
                if self.extract_comment:
                    comments = self.__comment_request_API_periodical(current_date_comment, end_date,
                                                                     sub)  # first call to API
                    while len(comments) > 0:  # collecting data until there are no more comments to extract in the time
                        # period considered
                        for raw_comment in comments:
                            # saving comments for each period 
                            current_date = datetime.datetime.utcfromtimestamp(raw_comment['created_utc']).strftime(
                                "%d/%m/%Y")
                            condition1_comment = datetime.datetime.strptime(current_date, "%d/%m/%Y") >= period_comment[
                                1]
                            condition2_comment = (datetime.datetime.strptime(current_date, "%d/%m/%Y") + relativedelta(
                                days=+1)) >= datetime.datetime.strptime(real_end_date, "%d/%m/%Y")
                            if condition1_comment or condition2_comment:  # saving comment for period
                                # Saving data: for each category a folder
                                path_category = os.path.join(raw_data_folder,
                                                             f'{categories_keys[i]}_{pretty_start_date}_'
                                                             f'{pretty_end_date}')
                                if not os.path.exists(path_category):
                                    os.mkdir(path_category)
                                pretty_period0_comment = period_comment[0].strftime('%d/%m/%Y').replace('/', '-')
                                pretty_period1_comment = period_comment[1].strftime('%d/%m/%Y').replace('/', '-')
                                path_period_category = os.path.join(path_category,
                                                                    f'{categories_keys[i]}_{pretty_period0_comment}_'
                                                                    f'{pretty_period1_comment}')
                                if not os.path.exists(path_period_category):
                                    os.mkdir(path_period_category)
                                # for each user in a period category a json file
                                for user in users:
                                    user_filename = os.path.join(path_period_category, f'{user}.json')
                                    if os.path.exists(user_filename):
                                        with open(user_filename) as fp:
                                            data = json.loads(fp.read())
                                            data['comments'].extend(users[user]['comments'])
                                        with open(user_filename, 'w') as fp:
                                            json.dump(data, fp, sort_keys=True, indent=4)
                                    else:
                                        with open(user_filename, 'w') as fp:
                                            json.dump(users[user], fp, sort_keys=True, indent=4)

                                # users = dict()
                                if condition1_comment:
                                    period_comment = (
                                        period_comment[1], period_comment[1] + relativedelta(months=+n_months))
                                    print('PERIOD_COMMENT', period_comment)
                                elif condition2_comment:
                                    break

                            # collecting comments
                            if raw_comment['author'] not in ['[deleted]', 'AutoModerator']:
                                user_id = raw_comment['author']
                                comment = dict()  # dict to store a comment
                                # adding field category
                                comment['category'] = category
                                # adding field date in a readable format
                                comment['date'] = datetime.datetime.utcfromtimestamp(
                                    raw_comment['created_utc']).strftime("%d/%m/%Y")
                                # cleaning body field
                                comment['clean_text'] = clean_raw_text(raw_comment['body'])
                                # adding time_period field in a readable format
                                if n_months != 0:
                                    comment['time_period'] = (
                                        period_comment[0].strftime('%d/%m/%Y'), period_comment[1].strftime('%d/%m/%Y'))
                                else:
                                    comment['time_period'] = (
                                        datetime.datetime.utcfromtimestamp(start_date).strftime("%d/%m/%Y"),
                                        datetime.datetime.utcfromtimestamp(end_date).strftime("%d/%m/%Y"))
                                # selecting fields
                                for attr in self.comment_attributes:
                                    if attr not in raw_comment.keys():  # handling missing values
                                        comment[attr] = None
                                    elif attr != 'body':  # saving only clean text
                                        comment[attr] = raw_comment[attr]
                                if len(comment['clean_text']) > 2:  # avoiding empty comments
                                    if user_id not in users.keys():
                                        if self.extract_post and self.extract_comment:
                                            users[user_id] = {'posts': [], 'comments': []}
                                        else:
                                            users[user_id] = {'comments': []}
                                    users[user_id]['comments'].append(comment)
                        current_date_comment = comments[-1][
                            'created_utc']  # taking the UNIX timestamp date of the last record extracted
                        comments = self.__comment_request_API_periodical(current_date_comment, end_date, sub)
                        pretty_current_date_comment = datetime.datetime.utcfromtimestamp(current_date_comment).strftime(
                            '%Y-%m-%d')
                        print(f'Extracted comments until date: {pretty_current_date_comment}')
                print(f'Finished data extraction for subreddit {sub}')
            # zip category folder 
            shutil.make_archive(path_category, 'zip', path_category)
            shutil.rmtree(path_category)
            print('Done to extract data from category:', categories_keys[i])
            i += 1  # to iter over categories elements

    def extract_user_data(self, users_list, start_date=None, end_date=None):
        """
        extract data (i.e., posts and/or comments) of one or more Reddit users

        Parameters
        ----------
        users_list : list
            list with Reddit users' usernames
        start_date : str
            beginning date in format %d/%m/%Y, None if you want start extracting data from Reddit beginning
            (i.e., 23/06/2005)
        end_date : str
            end date in format %d/%m/%Y, None if you want end extracting data at today date

        """
        # creating folder to record user activities
        raw_data_folder = os.path.join(self.out_folder, 'User_data')
        if not os.path.exists(raw_data_folder):
            os.mkdir(raw_data_folder)
        # handling dates (i.e., when starting and ending extract data)
        if start_date is None:
            start_date = '23/06/2005'  # start_date = when Reddit was launched
        if end_date is None:
            end_date = date.today()
            end_date = end_date.strftime("%d/%m/%Y")  # end_date = current date
        # converting date from format %d/%m/%Y to UNIX timestamp as requested by API
        start_date = int(time.mktime(datetime.datetime.strptime(start_date, "%d/%m/%Y").timetuple()))
        end_date = int(time.mktime(datetime.datetime.strptime(end_date, "%d/%m/%Y").timetuple()))
        users = dict()
        for username in users_list:
            print("Begin data extraction for user:", username)
            current_date_post = start_date
            current_date_comment = start_date
            # extracting posts
            if self.extract_post:
                posts = self.__post_request_API_user(current_date_post, end_date,
                                                     username)  # first call to API # TODO change API
                while len(posts) > 0:  # collecting data until reaching the end_date
                    # TODO: check if sub exists!
                    for raw_post in posts:
                        user_id = raw_post['author']
                        if user_id not in users.keys():
                            if self.extract_post and self.extract_comment:
                                users[user_id] = {'posts': [], 'comments': []}
                            else:
                                users[user_id] = {'posts': []}
                        post = dict()  # dict to store posts
                        # adding field date in a readable format
                        post['date'] = datetime.datetime.utcfromtimestamp(raw_post['created_utc']).strftime("%d/%m/%Y")
                        # cleaning body field
                        try:
                            merged_text = raw_post['title'] + ' ' + raw_post['selftext']
                        except:
                            merged_text = raw_post['title']
                        post['clean_text'] = clean_raw_text(merged_text)
                        # selecting fields 
                        for attr in self.post_attributes:
                            if attr not in raw_post.keys():  # handling missing values
                                post[attr] = None
                            elif (attr != 'selftext') and (attr != 'title'):  # saving only clean text
                                post[attr] = raw_post[attr]
                        users[user_id]['posts'].append(post)
                    current_date_post = posts[-1][
                        'created_utc']  # taking the UNIX timestamp date of the last record extracted
                    posts = self.__post_request_API_user(current_date_post, end_date, username)
                    pretty_current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime(
                        '%Y-%m-%d')
                    print(f'Extracted posts until date: {pretty_current_date_post}')

            # extracting comments
            if self.extract_comment:
                comments = self.__comment_request_API_user(current_date_comment, end_date,
                                                           username)  # first call to API
                while len(comments) > 0:
                    for raw_comment in comments:
                        user_id = raw_comment['author']
                        if user_id not in users.keys():
                            if self.extract_post and self.extract_comment:
                                users[user_id] = {'posts': [], 'comments': []}
                            else:
                                users[user_id] = {'comments': []}
                        comment = dict()  # dict to store a comment
                        # adding field date in a readable format
                        comment['date'] = datetime.datetime.utcfromtimestamp(raw_comment['created_utc']).strftime(
                            "%d/%m/%Y")
                        # cleaning body field
                        comment['clean_text'] = clean_raw_text(raw_comment['body'])
                        # selecting fields
                        for attr in self.comment_attributes:
                            if attr not in raw_comment.keys():  # handling missing values
                                comment[attr] = None
                            elif attr != 'body':  # saving only clean text
                                comment[attr] = raw_comment[attr]
                        users[user_id]['comments'].append(comment)
                    current_date_comment = comments[-1][
                        'created_utc']  # taking the UNIX timestamp date of the last record extracted
                    comments = self.__comment_request_API_user(current_date_comment, end_date, username)
                    pretty_current_date_comment = datetime.datetime.utcfromtimestamp(current_date_comment).strftime(
                        '%Y-%m-%d')
                    print(f'Extracted comments until date: {pretty_current_date_comment}')
            print('Finish data extraction for user:', username)

        # saving data: for each user a json file
        for user in users:
            user_filename = os.path.join(raw_data_folder, f'{user}.json')
            with open(user_filename, 'w') as fp:
                json.dump(users[user], fp, sort_keys=True, indent=4)
        print('Done to extract data for all selected users', users_list)

    def create_network(self, start_date, end_date, categories):
        """
        create users' interaction network based on comments:
        type of network: directed and weighted by number of interactions
        self loops are not allowed

        Parameters
        ----------
        start_date : str
            beginning date in format %d/%m/%Y
        end_date : str
            end date in format %d/%m/%Y
        categories : dict
            dict with category name as key and list of subreddits in that category as value
        """

        # if user wants to create users interactions networks it is necessary to extract both posts and comments in
        # extract_data()
        if not self.extract_comment or not self.extract_post:
            raise ValueError('To create users interactions Networks you have to set self.extract_comment to True')
        # formatting date
        pretty_start_date = start_date.replace('/', '-')
        pretty_end_date = end_date.replace('/', '-')
        # creating folder with network data
        user_network_folder = os.path.join(self.out_folder, 'Categories_networks')
        if not os.path.exists(user_network_folder):
            os.mkdir(user_network_folder)
        path = os.path.join(self.out_folder, 'Categories_raw_data')
        _categories = os.listdir(path)  # returns list
        # unzip files
        unzipped_categories = list()
        for cat in _categories:
            category_name = ''.join([i for i in cat if i.isalpha()]).replace('zip', '')
            if (category_name in list(categories.keys())) and (pretty_start_date in cat) and (
                    pretty_end_date in cat):
                file_name = os.path.abspath(os.path.join(path, cat))
                if not zipfile.is_zipfile(file_name):
                    unzipped_filename = os.path.basename(file_name)
                    unzipped_categories.append(unzipped_filename)
                elif os.path.basename(file_name).replace('.zip', '') not in _categories:
                    unzipped_filename = os.path.basename(file_name).replace('.zip', '')
                    extract_dir = file_name.replace('.zip', '')
                    shutil.unpack_archive(file_name, extract_dir, 'zip')
                    unzipped_categories.append(unzipped_filename)
        print('unzipped:', unzipped_categories)
        if not unzipped_categories:
            raise ValueError(
                'No data avaiable for the selected time period and/or subreddits, please extract them through '
                'extract_periodical_data() before create_network() call')

        # Saving data: for each category a folder 
        for cat in unzipped_categories:
            network_category = os.path.join(user_network_folder, f'{cat}')
            if not os.path.exists(network_category):
                os.mkdir(network_category)
            path_category = os.path.join(path, cat)
            category_periods = os.listdir(
                path_category)  # for each category a list of all files (i.e., periods) in that category
            for period in category_periods:
                print('PERIOD:', period)
                path_period = os.path.join(path_category, period)
                users_list = os.listdir(path_period)
                users = dict()
                for user in users_list:
                    user_filename = os.path.join(path_period, user)
                    submission_ids = dict()
                    comment_ids = dict()
                    parent_ids = dict()
                    with open(user_filename, 'r') as f:
                        user_data = json.load(f)
                        for comment in user_data['comments']:
                            comment_ids[comment['id']] = None
                            parent_ids[comment['parent_id']] = None
                        for post in user_data['posts']:
                            submission_ids[post['id']] = None
                    pretty_username = user.replace('.json', '')
                    if (len(submission_ids.keys()) > 0) or (len(comment_ids.keys()) > 0):
                        users[pretty_username] = {'post_ids': submission_ids, 'comment_ids': comment_ids,
                                                  'parent_ids': parent_ids}
                interactions = dict()
                # nodes = set()
                for user in users:
                    for parent_id in users[user]['parent_ids']:
                        if "t3" in parent_id:  # it is a comment to a post
                            for other_user in users:
                                if parent_id.replace("t3_", "") in users[other_user]['post_ids']:
                                    if user != other_user:  # avoiding self loops
                                        # nodes.add(other_user)
                                        # nodes.add(user)
                                        if (user, other_user) not in interactions:
                                            interactions[(user, other_user)] = 0
                                        interactions[(user, other_user)] += 1
                        elif "t1" in parent_id:  # it is a comment to another comment
                            for other_user in users:
                                if parent_id.replace("t1_", "") in users[other_user]['comment_ids']:
                                    if user != other_user:  # avoiding self loops
                                        # nodes.add(other_user)
                                        # nodes.add(user)
                                        if (user, other_user) not in interactions:
                                            interactions[(user, other_user)] = 0
                                        interactions[(user, other_user)] += 1
                # print('n_edges', len(interactions))
                # print('n_nodes', len(nodes))
                # creating edge list csv file
                # nodes_from = list()
                # nodes_to = list()
                # edges_weight = list()
                # for interaction in interactions:
                #     nodes_from.append(interaction[0])
                #     nodes_to.append(interaction[1])
                #     edges_weight.append(interactions[interaction])
                # tmp = {'Source': nodes_from, 'Target': nodes_to, 'Weight': edges_weight}
                # edge_list = pd.DataFrame(tmp)
                # saving csv in category folder
                last_path = os.path.join(network_category, f'{period}.csv')
                with open(last_path, "w") as out:
                    for nds, w in interactions.items():
                        out.write(f"{nds[0]},{nds[1]},{w}\n")
                # edge_list.to_csv(last_path, index=False)


if __name__ == '__main__':
    # initializing RedditHandler
    cwd = os.getcwd()
    out_fold = os.path.join(cwd, 'RedditHandler_Outputs')
    ext_posts = True  # True if you want to extract Post data, False otherwise
    ext_comments = True  # True if you want to extract Comment data, False otherwise
    post_attr = ['id', 'author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext',
                 'stickied', 'subreddit', 'subreddit_id', 'title']  # default
    comment_attr = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body',
                    'score']  # default
    my_handler = RedditHandler(out_fold, ext_posts, ext_comments, post_attributes=post_attr,
                               comment_attributes=comment_attr)
    start = '13/12/2018'
    end = '13/02/2019'
    selected_categories = {'gun': ['guncontrol']}
    nmonths = 1  # time_period to consider: if you don't want it n_months = 0
    my_handler.extract_periodical_data(start, end, selected_categories, nmonths)
    my_handler.create_network(start, end, selected_categories)
    # extracting user data
    usernames = ['17michela', 'BelleAriel', 'EschewObfuscation10']  # insert one or more Reddit username

    # None if you want start extracting from Reddit beginning, otherwise specify a date in format %d/%m/%Y
    start = None

    # None if you want end extracting at today date, otherwise specify a date in format %d/%m/%Y
    end = None
    my_handler.extract_user_data(usernames, start_date=start, end_date=end)
