import os
import json
import pandas as pd
import shutil
import zipfile
from nltk.tokenize import word_tokenize
import nltk
import statistics
import operator
import time
import datetime
from lexicalrichness import LexicalRichness
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from nrclex import NRCLex

class TextStatisticGenerator():

    def __init__(self, out_folder, extract_post, extract_comment, category, start_date, end_date):
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
        '''
       
        self.out_folder = out_folder
        if not os.path.exists(self.out_folder):
            os.mkdir(self.out_folder)
        self.extract_post = extract_post
        self.extract_comment = extract_comment
        self.categories = category
        # transforming date in a suitable format for folder name (category)
        self.pretty_start_date = start_date.replace('/','-')
        self.pretty_end_date = end_date.replace('/','-')  

    def _Lancaster_Sensorimotor_lexicon(self, tokenized_text, LNA_lexicon, LNP_lexicon, LNA, LNP):

        for word in tokenized_text:
            if word in list(LNP_lexicon.keys()):
                LNP[LNP_lexicon[word]]+=1
            if word in list(LNA_lexicon.keys()):
                LNA[LNA_lexicon[word]]+=1
        return LNA, LNP

    
    def _taboo_lexicon(self, tokenized_text, taboo_lexicon, taboo_rate): # range: [1-9]
        taboo = list()
        for word in tokenized_text:
            if word in list(taboo_lexicon.keys()):
                taboo.append(taboo_lexicon[word])
        if taboo:
            taboo_rate.append(round(statistics.mean(taboo),2))
        return taboo_rate

    def _VAD_lexicon(self, tokenized_text, VAD_Lexicon_Arousal, VAD_Lexicon_Dominance, VAD_Lexicon_Valence, VAD_dominance, VAD_arousal, VAD_valence):
        arousal, dominance, valence = ([] for i in range(3))
        for word in tokenized_text:
            if word in list(VAD_Lexicon_Arousal.keys()):
                arousal.append(VAD_Lexicon_Arousal[word])
            if word in list(VAD_Lexicon_Dominance.keys()):
                dominance.append(VAD_Lexicon_Dominance[word])
            if word in list(VAD_Lexicon_Valence.keys()):
                valence.append(VAD_Lexicon_Valence[word])

        if arousal:
            VAD_arousal.append(round(statistics.mean(arousal),2))
        if dominance:
            VAD_dominance.append(round(statistics.mean(dominance),2))
        if valence:
            VAD_valence.append(round(statistics.mean(valence),2))
        
        return VAD_dominance, VAD_arousal, VAD_valence

    def _NRCL_affect_lexicon(self, text, NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust):
        # NRCLex
        text_object = NRCLex(text)
        if text_object.affect_frequencies['anticip'] != 0.0:
            NRCL_anticipation.append(text_object.affect_frequencies['anticip'])
        if text_object.affect_frequencies['positive'] != 0.0:
            NRCL_positive.append(text_object.affect_frequencies['positive'])
        if text_object.affect_frequencies['surprise'] != 0.0:
            NRCL_surprise.append(text_object.affect_frequencies['surprise'])
        if text_object.affect_frequencies['trust'] != 0.0:
            NRCL_trust.append(text_object.affect_frequencies['trust'])
        if text_object.affect_frequencies['joy'] != 0.0:
            NRCL_joy.append(text_object.affect_frequencies['joy'])
        if text_object.affect_frequencies['fear'] != 0.0:
            NRCL_fear.append(text_object.affect_frequencies['fear'])
        if text_object.affect_frequencies['anger'] != 0.0:
            NRCL_anger.append(text_object.affect_frequencies['anger'])
        if text_object.affect_frequencies['negative'] != 0.0:
            NRCL_negative.append(text_object.affect_frequencies['negative'])
        if text_object.affect_frequencies['sadness'] != 0.0:
            NRCL_sadness.append(text_object.affect_frequencies['sadness'])
        if text_object.affect_frequencies['disgust'] != 0.0:
            NRCL_disgust.append(text_object.affect_frequencies['disgust'])

        return  NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust



    def _sentiment_analysis(self ,text, vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity):
        # VADER
        analyzer = SentimentIntensityAnalyzer()
        vs = analyzer.polarity_scores(text)
        vader_positive.append(vs['pos'])
        vader_negative.append(vs['neg'])
        vader_neutral.append(vs['neu'])
        vader_compound.append(vs['compound']) # The compound score is computed by summing the valence scores of each word in the lexicon, adjusted according to the rules, and then normalized to be between -1 (most extreme negative) and +1 (most extreme positive)
        # TEXTBLOB
        text = TextBlob(text)
        text.sentiment 
        textblob_polarity.append(text.sentiment.polarity) # Polarity is float which lies in the range of [-1,1] where 1 means positive statement and -1 means a negative statement.
        textblob_subjectivity.append(text.sentiment.subjectivity) # Subjective sentences generally refer to personal opinion, emotion or judgment whereas objective refers to factual information. Subjectivity is also a float which lies in the range of [0,1].
        
        return vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity
    
    def _compute_lexicalRichness(self, text, word_count, unique_words_cnt, lexical_diversity):
        lex = LexicalRichness(text)
        # word count
        word_count.append(lex.words)
        # unique term count
        unique_words_cnt.append(lex.terms)
        # measure of Textual Lexical Diversity 
        lexical_diversity.append(float(lex.terms)/float(lex.words))
        
        return word_count, unique_words_cnt, lexical_diversity  
   

    def extract_statistics(self):
        '''
        for each category, for each time period, for each user collcting their texts (posts/comments) (i.e., 'clean_text' field) and then computing a features vector containing text's statistics
        '''
        # open VAD affect Lexicon
        with open('psycholing_features_rates/VAD_Lexicon_Arousal.json') as fp:
            VAD_Lexicon_Arousal = json.loads(fp.read())
        with open('psycholing_features_rates/VAD_Lexicon_Dominance.json') as fp:
            VAD_Lexicon_Dominance = json.loads(fp.read())
        with open('psycholing_features_rates/VAD_Lexicon_Valence.json') as fp:
            VAD_Lexicon_Valence = json.loads(fp.read())
        # open Taboo Lexicon
        with open('psycholing_features_rates/Taboo_Words_and_Rate.json') as fp:
            taboo_lexicon = json.loads(fp.read())
        # open Lancaster Sensorimotor Lexicon
        with open('psycholing_features_rates/Lancaster_Norms_Action.json') as fp:
            LNA_lexicon = json.loads(fp.read())
        with open('psycholing_features_rates/Lancaster_Norms_Perceptual.json') as fp:
            LNP_lexicon = json.loads(fp.read())
        # creating folder with avg polaization score for each user
        user_textstats_folder = os.path.join(self.out_folder, 'Text_Statistics')
        if not os.path.exists(user_textstats_folder):
            os.mkdir(user_textstats_folder)
        path = os.path.join(self.out_folder, 'Categories_raw_data')
        categories = os.listdir(path) # returns list
        # unzip files
        unzipped_categories = list()
        for category in categories: 
            category_name = ''.join([i for i in category if i.isalpha()]).replace('zip','')
            if (category_name in list(self.categories.keys())) and (self.pretty_start_date in category) and (self.pretty_end_date in category):
                file_name = os.path.abspath(os.path.join(path,category))
                if not zipfile.is_zipfile(file_name):
                    unzipped_filename = os.path.basename(file_name)
                    unzipped_categories.append(unzipped_filename)
                elif os.path.basename(file_name).replace('.zip','') not in categories:
                    unzipped_filename = os.path.basename(file_name).replace('.zip','')
                    extract_dir = file_name.replace('.zip','')
                    shutil.unpack_archive(file_name, extract_dir, 'zip') 
                    unzipped_categories.append(unzipped_filename)
        print('unzipped:', unzipped_categories)

        # collecting texts for each category,period,user
        for category in unzipped_categories:
            textstats_category = os.path.join(user_textstats_folder, f'{category}')
            if not os.path.exists(textstats_category):
                os.mkdir(textstats_category)
            path_category = os.path.join(path, category) 
            category_periods = os.listdir(path_category) # for each category a list of all files (i.e., periods) in that category
            for period in category_periods:
                print('PERIOD:', period)
                path_period = os.path.join(path_category,period)
                users_list = os.listdir(path_period)
                users_stats = dict()
                # collecting user texts 
                for user in users_list: 
                    user_filename = os.path.join(path_period, user)
                    # lexical richness measures
                    word_count, unique_words_cnt, lexical_diversity = ([] for i in range(3))                  
                    # Vader sentiment analysis
                    vader_positive, vader_negative, vader_neutral, vader_compound = ([] for i in range(4))
                    # TextBlob sentiment analysis: polarity & subjectivity
                    textblob_polarity, textblob_subjectivity = ([] for i in range(2))
                    # NRCLex to measure emotional affects
                    NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust = ([] for i in range(10))
                    # VAD lexicon to measure dominance, arousal, valence
                    VAD_dominance, VAD_arousal, VAD_valence = ([] for i in range(3))
                    # Taboo Dataset to measure taboo rate
                    taboo_rate = list()
                    # Lancaster Sensorimotor Lexicon
                    LNP = {'Visual':0, 'Olfactory':0, 'Haptic':0, 'Auditory':0, 'Interoceptive':0, 'Gustatory':0}
                    LNA = {'Hand_arm':0, 'Mouth':0, 'Head':0, 'Torso':0, 'Foot_leg':0}
                    with open(user_filename, 'r') as f:
                        user_data = json.load(f)
                        if self.extract_comment:
                            for comment in user_data['comments']:
                                if len(comment['clean_text']) > 0:
                                    tokenized_text = word_tokenize(comment['clean_text'])
                                    word_count, unique_words_cnt, lexical_diversity = self._compute_lexicalRichness(comment['clean_text'], word_count, unique_words_cnt, lexical_diversity)
                                    vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity = self._sentiment_analysis(comment['clean_text'], vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity)
                                    NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust = self._NRCL_affect_lexicon(comment['clean_text'], NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust)
                                    VAD_dominance, VAD_arousal, VAD_valence = self._VAD_lexicon(tokenized_text, VAD_Lexicon_Arousal, VAD_Lexicon_Dominance, VAD_Lexicon_Valence, VAD_dominance, VAD_arousal, VAD_valence )
                                    taboo_rate = self._taboo_lexicon(tokenized_text, taboo_lexicon, taboo_rate)
                                    LNA, LNP = self._Lancaster_Sensorimotor_lexicon(tokenized_text, LNA_lexicon, LNP_lexicon, LNA, LNP)
                        if self.extract_post:
                            for post in user_data['posts']:
                                if len(post['clean_text']) > 0:
                                    tokenized_text = word_tokenize(post['clean_text'])
                                    word_count, unique_words_cnt, lexical_diversity = self._compute_lexicalRichness(post['clean_text'], word_count, unique_words_cnt, lexical_diversity)
                                    vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity = self._sentiment_analysis(post['clean_text'], vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity)
                                    NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust = self._NRCL_affect_lexicon(post['clean_text'], NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust)
                                    VAD_dominance, VAD_arousal, VAD_valence = self._VAD_lexicon(tokenized_text, VAD_Lexicon_Arousal, VAD_Lexicon_Dominance, VAD_Lexicon_Valence, VAD_dominance, VAD_arousal, VAD_valence )
                                    taboo_rate = self._taboo_lexicon(tokenized_text, taboo_lexicon, taboo_rate)
                                    LNA, LNP = self._Lancaster_Sensorimotor_lexicon(tokenized_text, LNA_lexicon, LNP_lexicon, LNA, LNP)
                    pretty_username = user.replace('.json','')
                    try:
                        avg_NRCL_positive = round(statistics.mean(NRCL_positive),2)
                    except:
                        avg_NRCL_positive = 0
                    try:
                        avg_NRCL_negative = round(statistics.mean(NRCL_negative),2)
                    except:
                        avg_NRCL_negative = 0
                    try:
                        avg_NRCL_anticipation = round(statistics.mean(NRCL_anticipation),2)
                    except:
                        avg_NRCL_anticipation = 0
                    try:
                        avg_NRCL_surprise = round(statistics.mean(NRCL_surprise),2)
                    except:
                        avg_NRCL_surprise = 0
                    try:
                        avg_NRCL_trust = round(statistics.mean(NRCL_trust),2)
                    except:
                        avg_NRCL_trust = 0
                    try:
                        avg_NRCL_joy = round(statistics.mean(NRCL_joy),2)
                    except:
                        avg_NRCL_joy = 0
                    try:
                        avg_NRCL_fear = round(statistics.mean(NRCL_fear),2)
                    except:
                        avg_NRCL_fear = 0
                    try:
                        avg_NRCL_anger = round(statistics.mean(NRCL_anger),2)
                    except:
                        avg_NRCL_anger = 0
                    try:
                        avg_NRCL_sadness = round(statistics.mean(NRCL_positive),2)
                    except:
                        avg_NRCL_sadness = 0
                    try:
                        avg_NRCL_disgust = round(statistics.mean(NRCL_disgust),2)
                    except:
                        avg_NRCL_disgust = 0
                    try:
                        avg_VAD_valence = round(statistics.mean(VAD_valence),2)
                    except:
                        avg_VAD_valence = 0
                    try:
                        avg_VAD_arousal = round(statistics.mean(VAD_arousal),2)
                    except:
                        avg_VAD_arousal = 0
                    try:
                        avg_VAD_dominance = round(statistics.mean(VAD_dominance),2)
                    except:
                        avg_VAD_dominance = 0
                    try:
                        avg_taboo_rate = round(statistics.mean(taboo_rate),2)
                    except:
                        avg_taboo_rate = 0

                    # user dict with avgs metrics
                    users_stats[pretty_username] = {'avg_word_count': round(statistics.mean(word_count),2), 'avg_unique_words': round(statistics.mean(unique_words_cnt),2), 
                    'avg_lexical_diversity': round(statistics.mean(lexical_diversity),2), 'avg_vader_positive': round(statistics.mean(vader_positive),2), 
                    'avg_vader_negative': round(statistics.mean(vader_negative),2), 'avg_vader_neutral': round(statistics.mean(vader_neutral),2), 'avg_vader_compound': round(statistics.mean(vader_compound),2), 
                    'avg_textblob_polarity': round(statistics.mean(textblob_polarity),2), 'avg_textblob_subjectivity': round(statistics.mean(textblob_subjectivity),2), 'avg_NRCL_positive': avg_NRCL_positive,
                    'avg_NRCL_negative': avg_NRCL_negative, 'avg_NRCL_anticipation': avg_NRCL_anticipation, 'avg_NRCL_surprise': avg_NRCL_surprise,
                    'avg_NRCL_trust': avg_NRCL_trust, 'avg_NRCL_joy': avg_NRCL_joy, 'avg_NRCL_fear': avg_NRCL_fear, 'avg_NRCL_anger': avg_NRCL_anger,
                    'avg_NRCL_sadness': avg_NRCL_sadness, 'avg_NRCL_disgust': avg_NRCL_disgust, 'avg_VAD_dominance': avg_VAD_dominance,
                    'avg_VAD_arousal': avg_VAD_arousal,'avg_VAD_valence': avg_VAD_valence, 'avg_taboo_rate': avg_taboo_rate, 'LNA_majority_class': max(LNA.items(), key=operator.itemgetter(1))[0], 'LNP_majority_class': max(LNP.items(), key=operator.itemgetter(1))[0]}

                nodes, word_count, unique_words_cnt, lexical_diversity, vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity, NRCL_positive, NRCL_negative, NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, NRCL_disgust, VAD_dominance, VAD_arousal, VAD_valence, taboo_rate, LNA, LNP = ([] for i in range(26))    
                for user in users_stats:
                    nodes.append(user)
                    word_count.append(users_stats[user]['avg_word_count'])
                    unique_words_cnt.append(users_stats[user]['avg_unique_words'])
                    lexical_diversity.append(users_stats[user]['avg_lexical_diversity'])
                    vader_positive.append(users_stats[user]['avg_vader_positive'])
                    vader_negative.append(users_stats[user]['avg_vader_negative'])
                    vader_neutral.append(users_stats[user]['avg_vader_neutral'])
                    vader_compound.append(users_stats[user]['avg_vader_compound'])
                    textblob_polarity.append(users_stats[user]['avg_textblob_polarity'])
                    textblob_subjectivity.append(users_stats[user]['avg_textblob_subjectivity'])
                    NRCL_positive.append(users_stats[user]['avg_NRCL_positive'])
                    NRCL_negative.append(users_stats[user]['avg_NRCL_negative'])
                    NRCL_anticipation.append(users_stats[user]['avg_NRCL_anticipation'])
                    NRCL_surprise.append(users_stats[user]['avg_NRCL_surprise'])
                    NRCL_trust.append(users_stats[user]['avg_NRCL_trust'])
                    NRCL_joy.append(users_stats[user]['avg_NRCL_joy'])
                    NRCL_fear.append(users_stats[user]['avg_NRCL_fear'])
                    NRCL_anger.append(users_stats[user]['avg_NRCL_anger'])
                    NRCL_sadness.append(users_stats[user]['avg_NRCL_sadness'])
                    NRCL_disgust.append(users_stats[user]['avg_NRCL_disgust'])
                    VAD_dominance.append(users_stats[user]['avg_VAD_dominance'])
                    VAD_arousal.append(users_stats[user]['avg_VAD_arousal'])
                    VAD_valence.append(users_stats[user]['avg_VAD_valence'])
                    taboo_rate.append(users_stats[user]['avg_taboo_rate'])
                    LNA.append(users_stats[user]['LNA_majority_class'])
                    LNP.append(users_stats[user]['LNP_majority_class'])

                _tmp = {'Id': nodes, 'avg_word_count': word_count, 'avg_unique_words': unique_words_cnt, 'avg_lexical_diversity': lexical_diversity, 'avg_vader_positive': vader_positive, 'avg_vader_negative': vader_negative, 'avg_vader_neutral': vader_neutral, 'avg_vader_compound': vader_compound, 'avg_textblob_polarity': textblob_polarity, 'avg_textblob_subjectivity': textblob_subjectivity,
                        'avg_NRCL_positive': NRCL_positive, 'avg_NRCL_negative': NRCL_negative, 'avg_NRCL_anticipation': NRCL_anticipation, 'avg_NRCL_surprise': NRCL_surprise,
                        'avg_NRCL_trust': NRCL_trust, 'avg_NRCL_joy': NRCL_joy,  'avg_NRCL_fear': NRCL_fear, 'avg_NRCL_anger': NRCL_anger, 'avg_NRCL_sadness': NRCL_sadness, 'avg_NRCL_disgust': NRCL_disgust, 'avg_VAD_arousal': VAD_arousal, 'avg_VAD_dominance': VAD_dominance, 'avg_VAD_valence': VAD_valence, 'avg_taboo_rate': taboo_rate, 'LNA_majority_class': LNA, 'LNP_majority_class': LNP}
                node_labels = pd.DataFrame(_tmp)
                print('n_users:', len(users_stats))
                last_path = os.path.join(textstats_category, f'{period}.csv')
                node_labels.to_csv(last_path, index = False)
                # saving for each period a json file with username as key and (avg_polarization_score, label) as value
                period_filename = os.path.join(textstats_category, f'{period}.json')
                with open(period_filename, 'w') as fp:
                    json.dump(users_stats, fp, sort_keys=True, indent=4)

if __name__ == '__main__':
    cwd = os.getcwd()
    out_folder = os.path.join(cwd, 'RedditHandler_Outputs')
    out_folder = 'RedditHandler_Outputs'
    extract_post = True
    extract_comment = True
    category = {'gun':['guncontrol'], 'politics':['EnoughTrumpSpam','Fuckthealtright']}
    start_date = '13/12/2018'
    end_date = '13/02/2019'
    my_stats_generator = TextStatisticGenerator(out_folder, extract_post, extract_comment, category, start_date, end_date)
    my_stats_generator.extract_statistics()


