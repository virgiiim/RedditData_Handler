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
import glob
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
            # time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError,
                requests.exceptions.ChunkedEncodingError):
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
            # time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError,
                requests.exceptions.ChunkedEncodingError):
            return self.__post_request_API_user(start_date, end_date, username)
        return data['data']  # data['data'] contains list of posts

    def __comment_request_API_periodical(self, start_date, end_date, subreddit):
        """
        API REQUEST to pushishift.io/reddit/comment
        returns a list of 1000 dictionaries where each of them is a comment
        """
        url = 'https://api.pushshift.io/reddit/search/comment?&size=500&after=' + str(start_date) + '&before=' + str(
            end_date) + '&subreddit=' + str(subreddit)
        print(url)
        try:
            r = requests.get(url)  # Response Object
            # time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError,
                requests.exceptions.ChunkedEncodingError):
            print("err")
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
            # time.sleep(random.random() * 0.02)
            data = json.loads(r.text)  # r.text is a JSON object, converted into dict
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError,
                requests.exceptions.ChunkedEncodingError):
            return self.__comment_request_API_user(start_date, end_date, username)
        return data['data']  # data['data'] contains list of comments

    def __write_data(self, users, path):
        pass

    def __process_post(self, raw_post, category, is_post=True):
        user_id = raw_post['author']
        post = dict()  # dict to store posts
        # adding field category
        post['category'] = category
        # adding field date in a readable format
        post['date'] = datetime.datetime.utcfromtimestamp(raw_post['created_utc']).strftime(
            "%d/%m/%Y")
        # cleaning body field

        if is_post:
            merged_text = raw_post['title'] + ' ' + raw_post['selftext']
            post['clean_text'] = clean_raw_text(merged_text)
        else:
            post['clean_text'] = clean_raw_text(raw_post['body'])
            post['link_id'] = raw_post['link_id']
            post['parent_id'] = raw_post['parent_id']

        # selecting fields
        for attr in self.post_attributes:
            if attr not in raw_post:  # handling missing values
                post[attr] = None
            elif (attr != 'selftext') and (attr != 'title'):  # saving only clean text
                post[attr] = raw_post[attr]

        return user_id, post, raw_post['created_utc']

    @staticmethod
    def __save_data(users, path_period_category):
        # for each user in a period category a json file
        for user in users:
            user_filename = os.path.join(path_period_category, f'{user}.json')
            if os.path.exists(user_filename):
                with open(user_filename) as fp:
                    data = json.loads(fp.read())

                    for dt, posts in users[user]['posts'].items():
                        if dt in data['posts']:
                            data['posts'][dt].extend(posts)
                        else:
                            data['posts'][dt] = posts

                    for dt, coms in users[user]['comments'].items():
                        if dt in data['comments']:
                            data['comments'][dt].extend(coms)
                        else:
                            data['comments'][dt] = coms

                with open(user_filename, 'w') as fp:
                    json.dump(data, fp, sort_keys=True, indent=4)
            else:
                with open(user_filename, 'w') as fp:
                    json.dump(users[user], fp, sort_keys=True, indent=4)

    def extract_periodical_data(self, start_date, end_date, categories):

        # converting date from format %d/%m/%Y to UNIX timestamp as requested by API
        start_date = int(time.mktime(datetime.datetime.strptime(start_date, "%d/%m/%Y").timetuple()))
        end_date = int(time.mktime(datetime.datetime.strptime(end_date, "%d/%m/%Y").timetuple()))
        raw_data_folder = os.path.join(self.out_folder, 'Categories_raw_data')
        if not os.path.exists(raw_data_folder):
            os.mkdir(raw_data_folder)

        for category, subcats in categories.items():
            for sub in subcats:

                current_date_post = start_date

                if self.extract_post:
                    end_flag = True
                    users = dict()
                    old_current = current_date_post
                    while end_flag:

                        if current_date_post > end_date:
                            end_flag = False
                            path_category = self.__check_path(category, raw_data_folder)
                            self.__save_data(users, path_category)
                            continue

                        posts = self.__post_request_API_periodical(current_date_post, end_date, sub)

                        if len(posts) == 0:
                            current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime(
                                "%d/%m/%Y")
                            current_date_post = datetime.datetime.strptime(current_date_post, "%d/%m/%Y") + \
                                                relativedelta(days=+1)
                            current_date_post = int((current_date_post - datetime.datetime(1970, 1, 1)).total_seconds())
                            continue

                        for raw_post in posts:

                            if raw_post['author'] in ['[deleted]', 'AutoModerator']:
                                continue

                            user, pdescr, current_date_post = self.__process_post(raw_post, category, is_post=True)

                            if user in users:

                                if pdescr['date'] in users[user]['posts']:
                                    users[user]['posts'][pdescr['date']].append(pdescr)
                                else:
                                    users[user]['posts'][pdescr['date']] = [pdescr]
                            else:
                                users[user] = {'posts': {pdescr['date']: [pdescr]}, 'comments': {}}

                        pretty_current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime(
                            '%Y-%m-%d')

                        if pretty_current_date_post != old_current:
                            print(f'Extracted posts until date: {pretty_current_date_post}')
                            old_current = pretty_current_date_post
                            path_category = self.__check_path(category, raw_data_folder)
                            self.__save_data(users, path_category)
                            users = dict()

                if self.extract_comment:
                    current_date_post = start_date

                    end_flag = True
                    users = dict()
                    old_current = current_date_post
                    while end_flag:

                        if current_date_post > end_date:
                            end_flag = False
                            path_category = self.__check_path(category, raw_data_folder)
                            self.__save_data(users, path_category)
                            continue

                        posts = self.__comment_request_API_periodical(current_date_post, end_date, sub)

                        if len(posts) == 0:
                            current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime(
                                "%d/%m/%Y")
                            current_date_post = datetime.datetime.strptime(current_date_post, "%d/%m/%Y") + \
                                                relativedelta(days=+1)
                            current_date_post = int((current_date_post - datetime.datetime(1970, 1, 1)).total_seconds())
                            continue

                        for raw_post in posts:

                            if raw_post['author'] in ['[deleted]', 'AutoModerator']:
                                continue

                            user, pdescr, current_date_post = self.__process_post(raw_post, category, is_post=False)

                            if user in users:
                                if pdescr['date'] in users[user]['comments']:
                                    users[user]['comments'][pdescr['date']].append(pdescr)
                                else:
                                    users[user]['comments'][pdescr['date']] = [pdescr]
                            else:
                                users[user] = {'posts': {}, 'comments': {pdescr['date']: [pdescr]}}

                        pretty_current_date_post = datetime.datetime.utcfromtimestamp(current_date_post).strftime(
                            '%Y-%m-%d')

                        if pretty_current_date_post != old_current:
                            print(f'Extracted comments until date: {pretty_current_date_post}')
                            old_current = pretty_current_date_post
                            path_category = self.__check_path(category, raw_data_folder)
                            self.__save_data(users, path_category)
                            users = dict()

    @staticmethod
    def __check_path(category, raw_data_folder):
        path_category = os.path.join(raw_data_folder, f'{category}')

        if not os.path.exists(path_category):
            os.mkdir(path_category)

        return path_category

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
                old_current = ""
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
                    if pretty_current_date_post != old_current:
                        print(f'Extracted posts until date: {pretty_current_date_post}')
                        old_current = pretty_current_date_post

            # extracting comments
            if self.extract_comment:
                old_current = ""
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
                    if pretty_current_date_comment != old_current:
                        print(f'Extracted comments until date: {pretty_current_date_comment}')
                        old_current = pretty_current_date_comment

            print('Finish data extraction for user:', username)

        # saving data: for each user a json file
        for user in users:
            user_filename = os.path.join(raw_data_folder, f'{user}.json')
            with open(user_filename, 'w') as fp:
                json.dump(users[user], fp, sort_keys=True, indent=4)
        print('Done to extract data for all selected users', users_list)

    def create_network(self, categories):

        if not self.extract_comment or not self.extract_post:
            raise ValueError('To create users interactions Networks you have to set self.extract_comment to True')

        user_network_folder = os.path.join(self.out_folder, 'Categories_networks')
        if not os.path.exists(user_network_folder):
            os.mkdir(user_network_folder)
        path = os.path.join(self.out_folder, 'Categories_raw_data')

        for category in categories:
            post_to_author = {}

            users_path = os.path.join(path, category)

            users_files = glob.glob(f"{users_path}{os.sep}*.json")
            with open(os.path.join(user_network_folder, f"{category}.csv"), "w") as out:
                for user_file in users_files:
                    with open(user_file) as fp:
                        data = json.loads(fp.read())
                        for dt, comments in data['comments'].items():
                            for comment in comments:
                                res = f"{comment['id']},{comment['parent_id'].split('_')[1]},{comment['author']},,{dt}\n"
                                post_to_author[comment['id']] = comment['author']
                                out.write(res)
                        for _, posts in data['posts'].items():
                            for post in posts:
                                post_to_author[post['id']] = post['author']

            with open(os.path.join(user_network_folder, f"{category}.csv")) as f:
                with open(os.path.join(user_network_folder, f"{category}_complete.csv"), "w") as out:
                    for row in f:
                        row = row.split(",")
                        try:
                            tid = post_to_author[row[1]]
                            res = f"{row[0]},{row[1]},{row[2]},{tid},{row[4]}"
                            out.write(res)
                        except:
                            pass
            os.remove(os.path.join(user_network_folder, f"{category}.csv"))


if __name__ == '__main__':
    # initializing RedditHandler
    cwd = os.getcwd()
    out_fold = 'RedditHandler_Outputs'
    ext_posts = True  # True if you want to extract Post data, False otherwise
    ext_comments = True  # True if you want to extract Comment data, False otherwise
    post_attr = ['id', 'author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext',
                 'stickied', 'subreddit', 'subreddit_id', 'title']  # default
    comment_attr = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body',
                    'score']  # default
    my_handler = RedditHandler(out_fold, ext_posts, ext_comments, post_attributes=post_attr,
                               comment_attributes=comment_attr)
    start = '1/01/2020'
    end = '4/01/2020'
    selected_categories = {'finance': ['wallstreetbets']}
    my_handler.extract_periodical_data(start, end, selected_categories)
    my_handler.create_network(selected_categories)
    # extracting user data
    # usernames = ['17michela', 'BelleAriel', 'EschewObfuscation10']  # insert one or more Reddit username

    # None if you want start extracting from Reddit beginning, otherwise specify a date in format %d/%m/%Y
    # start = None

    # None if you want end extracting at today date, otherwise specify a date in format %d/%m/%Y
    # end = None
    # my_handler.extract_user_data(usernames, start_date=start, end_date=end)
