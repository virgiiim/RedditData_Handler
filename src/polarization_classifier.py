import pickle
import time
import datetime
import json
import os
import shutil
import zipfile
import statistics
import re
import stop_words
import numpy as np
import pandas as pd
from stop_words import get_stop_words
from keras.models import model_from_json
from keras.models import load_model
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences


def remove_stopWords(s):
    stop_words = get_stop_words('en')
    s = ' '.join(word for word in s.split() if word not in stop_words)
    return s


class PolarizationClassifier(object):

    def __init__(self, out_folder, extract_post, extract_comment, category, start_date, end_date, file_model,
                 file_weights, file_tokenizer):
        """
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
        file_model: .json
            Glove Word Embeddings + LSTM Model
        file_weights: .h5
            Model's weights
        file_tokenizer: .pickle
            Model's tokenizer
        """

        self.out_folder = out_folder
        if not os.path.exists(self.out_folder):
            os.mkdir(self.out_folder)
        self.extract_post = extract_post
        self.extract_comment = extract_comment
        self.categories = category
        # transforming date in a suitable format for folder name (category)
        self.pretty_start_date = start_date.replace('/', '-')
        self.pretty_end_date = end_date.replace('/', '-')
        # loading model and weights
        json_file = open(file_model, 'r')
        loaded_model_json = json_file.read()
        json_file.close()
        self.model = model_from_json(loaded_model_json)
        self.model.load_weights(file_weights)
        # loading tokenizer
        with open(file_tokenizer, 'rb') as handle:
            self.tokenizer = pickle.load(handle)

    def compute_polarization(self):
        """
        For each category, for each time period, for each user collcting their texts (posts/comments)
        (i.e., 'clean_text' field) and then computing his polarization score
        """
        # creating folder with avg polaization score for each user
        user_polscore_folder = os.path.join(self.out_folder, 'Polarization_scores')
        if not os.path.exists(user_polscore_folder):
            os.mkdir(user_polscore_folder)
        path = os.path.join(self.out_folder, 'Categories_raw_data')
        category_files = os.listdir(path)  # returns list
        # unzip files
        unzipped_categories = list()
        for category in category_files:
            category_name = ''.join([i for i in category if i.isalpha()]).replace('zip', '')
            if (category_name in list(self.categories.keys())) and (self.pretty_start_date in category) and (
                    self.pretty_end_date in category):
                file_name = os.path.abspath(os.path.join(path, category))
                if not zipfile.is_zipfile(file_name):
                    unzipped_filename = os.path.basename(file_name)
                    unzipped_categories.append(unzipped_filename)
                elif os.path.basename(file_name).replace('.zip', '') not in category_files:
                    unzipped_filename = os.path.basename(file_name).replace('.zip', '')
                    extract_dir = file_name.replace('.zip', '')
                    shutil.unpack_archive(file_name, extract_dir, 'zip')
                    unzipped_categories.append(unzipped_filename)
        print('unzipped:', unzipped_categories)

        # collecting texts for each category,period,user
        for category in unzipped_categories:
            polscore_category = os.path.join(user_polscore_folder, f'{category}')
            if not os.path.exists(polscore_category):
                os.mkdir(polscore_category)
            path_category = os.path.join(path, category)
            category_periods = os.listdir(
                path_category)  # for each category a list of all files (i.e., periods) in that category
            for period in category_periods:
                print('PERIOD:', period)
                path_period = os.path.join(path_category, period)
                users_list = os.listdir(path_period)
                users_pol = dict()
                # collecting user texts 
                for user in users_list:
                    user_filename = os.path.join(path_period, user)
                    texts = list()
                    with open(user_filename, 'r') as f:
                        user_data = json.load(f)
                        if self.extract_comment:
                            for comment in user_data['comments']:
                                texts.append(comment['clean_text'])
                        if self.extract_post:
                            for post in user_data['posts']:
                                texts.append(post['clean_text'])
                    pretty_username = user.replace('.json', '')
                    # computing polarization score for each user' content
                    results = self._predict_prob(texts)
                    pol_scores = [float(item) for sublist in results for item in sublist]
                    user_avg_pol = round(statistics.mean(pol_scores), 2)
                    # discretizing polarization scores in 3 category right, neutral, left
                    if user_avg_pol >= 0.6:
                        label = 'right'
                    elif user_avg_pol <= 0.4:
                        label = 'left'
                    else:
                        label = 'neutral'
                    users_pol[pretty_username] = (user_avg_pol, label)
                nodes = list()
                labels = list()
                for user in users_pol:
                    nodes.append(user)
                    labels.append(users_pol[user][1])
                _tmp = {'Id': nodes, 'Political_leaning': labels}
                node_labels = pd.DataFrame(_tmp)
                print('n_users:', len(users_pol))
                last_path = os.path.join(polscore_category, f'{period}.csv')
                node_labels.to_csv(last_path, index=False)
                # saving for each period a json file with username as key and (avg_polarization_score, label) as value
                period_filename = os.path.join(polscore_category, f'{period}.json')
                with open(period_filename, 'w') as fp:
                    json.dump(users_pol, fp, sort_keys=True, indent=4)

    def _predict_prob(self, submissions):
        """
        Given a list of sentences in input, this method remove stop words from them, tokenize and vectorize them
        and finally return the prediction/polarization score for each sentence [0-1]

        :param submissions:
        """
        # remove stop words
        submissions = [remove_stopWords(x) for x in submissions]
        # tokenize and vectorize sequences
        encoded_docs_test = self.tokenizer.texts_to_sequences(submissions)
        # padding sequences
        padded_docs_test = pad_sequences(encoded_docs_test, maxlen=350, padding='post')
        return self.model.predict_proba(padded_docs_test)

    def _predict_class(self, submissions):
        """
        Given a list of sentences in input, this method return the class of each sentence:
        0 if the sentence has a prediction score <= 0.5, 1 otherwise

        :param submissions:
        """
        pred_prob = self._predict_prob(submissions)
        return np.round(pred_prob)


if __name__ == '__main__':
    cwd = os.getcwd()
    out_fold = os.path.join(cwd, '../RedditHandler_Outputs')
    ext_post = True
    ext_comment = True
    categories = {'gun': ['guncontrol'], 'politics': ['EnoughTrumpSpam', 'Fuckthealtright']}
    start = '13/12/2018'
    end = '13/02/2019'
    f_model = 'Model/model_glove.json'
    f_weights = 'Model/model_glove.h5'
    f_tokenizer = 'Model/tokenizer_def.pickle'

    my_pol_classifier = PolarizationClassifier(out_fold, ext_post, ext_comment, categories, start,
                                               end, f_model, f_weights, f_tokenizer)
    my_pol_classifier.compute_polarization()
