import os
import json
import pandas as pd
import shutil
import zipfile
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import statistics
import operator
import time
import datetime
from lexicalrichness import LexicalRichness
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from nrclex import NRCLex
from nltk.corpus import stopwords


class TextStatisticGenerator(object):

    def __init__(self, out_folder, extract_post, extract_comment, category, start_date, end_date):
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

    @staticmethod
    def _Lancaster_Sensorimotor_lexicon(tokenized_text, LNA_lexicon, LNP_lexicon):
        LNP = {'Visual': 0, 'Olfactory': 0, 'Haptic': 0, 'Auditory': 0, 'Interoceptive': 0, 'Gustatory': 0}
        LNA = {'Hand_arm': 0, 'Mouth': 0, 'Head': 0, 'Torso': 0, 'Foot_leg': 0}
        for word in tokenized_text:
            if word in list(LNP_lexicon.keys()):
                LNP[LNP_lexicon[word]] += 1
            if word in list(LNA_lexicon.keys()):
                LNA[LNA_lexicon[word]] += 1
        return LNA, LNP

    @staticmethod
    def _taboo_lexicon(tokenized_text, taboo_lexicon):  # range: [1-9]
        taboo = list()
        for word in tokenized_text:
            if word in list(taboo_lexicon.keys()):
                taboo.append(taboo_lexicon[word])
        if taboo:
            taboo_rate = round(statistics.mean(taboo), 2)
        else:
            taboo_rate = 0
        return taboo_rate

    @staticmethod
    def _VAD_lexicon(tokenized_text, VAD_Lexicon_Arousal, VAD_Lexicon_Dominance, VAD_Lexicon_Valence):
        VAD = dict()
        arousal, dominance, valence = [], [], []
        for word in tokenized_text:
            if word in list(VAD_Lexicon_Arousal.keys()):
                arousal.append(VAD_Lexicon_Arousal[word])
            if word in list(VAD_Lexicon_Dominance.keys()):
                dominance.append(VAD_Lexicon_Dominance[word])
            if word in list(VAD_Lexicon_Valence.keys()):
                valence.append(VAD_Lexicon_Valence[word])
        if arousal:
            VAD['arousal'] = round(statistics.mean(arousal), 2)
        else:
            VAD['arousal'] = 0
        if dominance:
            VAD['dominance'] = round(statistics.mean(dominance), 2)
        else:
            VAD['dominance'] = 0
        if valence:
            VAD['valence'] = round(statistics.mean(valence), 2)
        else:
            VAD['valence'] = 0

        return VAD

    @staticmethod
    def _NRCL_affect_lexicon(text):
        # NRCLex
        text_object = NRCLex(text)

        return text_object.affect_frequencies

    @staticmethod
    def _compute_lexicalRichness(text, lemmatized_test, word_count, unique_words_cnt, lexical_diversity):
        lex = LexicalRichness(text)
        lex_lemmatized = LexicalRichness(lemmatized_test)
        # word count
        word_count.append(lex.words)
        # unique term count
        unique_words_cnt.append(lex.terms)
        # measure of Textual Lexical Diversity 
        lexical_diversity.append(float(lex_lemmatized.terms) / float(lex_lemmatized.words))

        return word_count, unique_words_cnt, lexical_diversity

    @staticmethod
    def _pos_tagger(nltk_tag):
        if nltk_tag.startswith('J'):
            return wordnet.ADJ
        elif nltk_tag.startswith('V'):
            return wordnet.VERB
        elif nltk_tag.startswith('N'):
            return wordnet.NOUN
        elif nltk_tag.startswith('R'):
            return wordnet.ADV
        else:
            return None

    def extract_statistics(self):
        """
        for each category, for each time period, for each user collcting their texts (posts/comments)
        (i.e., 'clean_text' field) and then computing a features vector containing text's statistics
        """
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
        # Initiating Lemmatizer
        lemmatizer = WordNetLemmatizer()
        # creating folder with avg polaization score for each user
        user_textstats_folder = os.path.join(self.out_folder, 'Text_Statistics')
        if not os.path.exists(user_textstats_folder):
            os.mkdir(user_textstats_folder)
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
            textstats_category = os.path.join(user_textstats_folder, f'{category}')
            if not os.path.exists(textstats_category):
                os.mkdir(textstats_category)
            path_category = os.path.join(path, category)
            category_periods = os.listdir(
                path_category)  # for each category a list of all files (i.e., periods) in that category
            for period in category_periods:
                print('PERIOD:', period)
                path_period = os.path.join(path_category, period)
                users_list = os.listdir(path_period)
                users_stats = dict()
                # collecting user texts 
                for user in users_list:
                    user_filename = os.path.join(path_period, user)
                    # all user texts
                    all_tokenized_texts = list()
                    all_filtered_words = list()
                    all_filtered_texts = ''
                    # lexical richness measures
                    word_count, unique_words_cnt, lexical_diversity = [], [], []
                    # Vader sentiment analysis
                    vader_positive, vader_negative, vader_neutral, vader_compound = [], [], [], []
                    # TextBlob sentiment analysis: polarity & subjectivity
                    textblob_polarity, textblob_subjectivity = [], []
                    with open(user_filename, 'r') as f:
                        user_data = json.load(f)
                        if self.extract_comment:
                            for comment in user_data['comments']:
                                if len(comment['clean_text']) > 0:
                                    correct_text = comment['clean_text']
                                    # tokenize text
                                    tokenized_text = word_tokenize(correct_text)
                                    all_tokenized_texts.extend(tokenized_text)
                                    # find the POS tag for each token 
                                    pos_tagged = nltk.pos_tag(tokenized_text)
                                    wordnet_tagged = list(map(lambda x: (x[0], self._pos_tagger(x[1])), pos_tagged))
                                    # lemmatize text
                                    lemmatized_text = list()
                                    for word, tag in wordnet_tagged:
                                        if tag is None:
                                            # if there is no available tag, append the token as is 
                                            lemmatized_text.append(word)
                                        else:
                                            # else use the tag to lemmatize the token 
                                            lemmatized_text.append(lemmatizer.lemmatize(word, tag))
                                    lemmatized_text = " ".join(lemmatized_text)
                                    # removing stopwords
                                    filtered_words = [word for word in tokenized_text if
                                                      word not in stopwords.words('english')]
                                    all_filtered_words.extend(filtered_words)
                                    filtered_text = ' '.join(word for word in filtered_words)
                                    all_filtered_texts += ' ' + filtered_text
                                    word_count, unique_words_cnt, lexical_diversity = self._compute_lexicalRichness(
                                        correct_text, lemmatized_text, word_count, unique_words_cnt, lexical_diversity)
                                    vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, \
                                    textblob_subjectivity = sentiment_analysis(
                                        correct_text, vader_positive, vader_negative, vader_neutral, vader_compound,
                                        textblob_polarity, textblob_subjectivity)

                        if self.extract_post:
                            for post in user_data['posts']:
                                if len(post['clean_text']) > 0:
                                    correct_text = post['clean_text']
                                    # tokenize test
                                    tokenized_text = word_tokenize(correct_text)
                                    all_tokenized_texts.extend(tokenized_text)
                                    # find the POS tag for each token 
                                    pos_tagged = nltk.pos_tag(tokenized_text)
                                    wordnet_tagged = list(map(lambda x: (x[0], self._pos_tagger(x[1])), pos_tagged))
                                    # lemmatize text
                                    lemmatized_text = list()
                                    for word, tag in wordnet_tagged:
                                        if tag is None:
                                            # if there is no available tag, append the token as is 
                                            lemmatized_text.append(word)
                                        else:
                                            # else use the tag to lemmatize the token 
                                            lemmatized_text.append(lemmatizer.lemmatize(word, tag))
                                    lemmatized_text = " ".join(lemmatized_text)
                                    filtered_words = [word for word in tokenized_text if
                                                      word not in stopwords.words('english')]
                                    all_filtered_words.extend(filtered_words)
                                    filtered_text = ' '.join(word for word in filtered_words)
                                    all_filtered_texts += ' ' + filtered_text
                                    word_count, unique_words_cnt, lexical_diversity = self._compute_lexicalRichness(
                                        correct_text, lemmatized_text, word_count, unique_words_cnt, lexical_diversity)
                                    vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, \
                                    textblob_subjectivity = sentiment_analysis(
                                        correct_text, vader_positive, vader_negative, vader_neutral, vader_compound,
                                        textblob_polarity, textblob_subjectivity)

                    pretty_username = user.replace('.json', '')
                    LNA, LNP = self._Lancaster_Sensorimotor_lexicon(all_tokenized_texts, LNA_lexicon, LNP_lexicon)
                    taboo_rate = self._taboo_lexicon(all_filtered_words, taboo_lexicon)
                    VAD = self._VAD_lexicon(all_filtered_words, VAD_Lexicon_Arousal, VAD_Lexicon_Dominance,
                                            VAD_Lexicon_Valence)
                    NRCL_affect_frequencies = self._NRCL_affect_lexicon(all_filtered_texts)

                    # user dict with avgs metrics
                    users_stats[pretty_username] = {'avg_word_count': round(statistics.mean(word_count), 2),
                                                    'avg_unique_words': round(statistics.mean(unique_words_cnt), 2),
                                                    'avg_lexical_diversity': round(statistics.mean(lexical_diversity),
                                                                                   2),
                                                    'avg_vader_positive': round(statistics.mean(vader_positive), 2),
                                                    'avg_vader_negative': round(statistics.mean(vader_negative), 2),
                                                    'avg_vader_neutral': round(statistics.mean(vader_neutral), 2),
                                                    'avg_vader_compound': round(statistics.mean(vader_compound), 2),
                                                    'avg_textblob_polarity': round(statistics.mean(textblob_polarity),
                                                                                   2),
                                                    'avg_textblob_subjectivity': round(
                                                        statistics.mean(textblob_subjectivity), 2),
                                                    'avg_NRCL_positive': round(NRCL_affect_frequencies['positive'], 2),
                                                    'avg_NRCL_negative': round(NRCL_affect_frequencies['negative'], 2),
                                                    'avg_NRCL_anticipation': round(NRCL_affect_frequencies['anticip'],
                                                                                   2),
                                                    'avg_NRCL_surprise': round(NRCL_affect_frequencies['surprise'], 2),
                                                    'avg_NRCL_trust': round(NRCL_affect_frequencies['trust'], 2),
                                                    'avg_NRCL_joy': round(NRCL_affect_frequencies['joy'], 2),
                                                    'avg_NRCL_fear': round(NRCL_affect_frequencies['fear'], 2),
                                                    'avg_NRCL_anger': round(NRCL_affect_frequencies['anger'], 2),
                                                    'avg_NRCL_sadness': round(NRCL_affect_frequencies['sadness'], 2),
                                                    'avg_NRCL_disgust': round(NRCL_affect_frequencies['disgust'], 2),
                                                    'avg_VAD_dominance': VAD['dominance'],
                                                    'avg_VAD_arousal': VAD['arousal'],
                                                    'avg_VAD_valence': VAD['valence'], 'avg_taboo_rate': taboo_rate,
                                                    'cnt_LNA_Hand_arm': LNA['Hand_arm'], 'cnt_LNA_Mouth': LNA['Mouth'],
                                                    'cnt_LNA_Head': LNA['Head'],
                                                    'cnt_LNA_Torso': LNA['Torso'], 'cnt_LNA_Foot_leg': LNA['Foot_leg'],
                                                    'cnt_LNP_Visual': LNP['Visual'],
                                                    'cnt_LNP_Olfactory': LNP['Olfactory'],
                                                    'cnt_LNP_Haptic': LNP['Haptic'],
                                                    'cnt_LNP_Auditory': LNP['Auditory'],
                                                    'cnt_LNP_Interoceptive': LNP['Interoceptive'],
                                                    'cnt_LNP_Gustatory': LNP['Gustatory']}

                nodes, word_count, unique_words_cnt, lexical_diversity, vader_positive, vader_negative, vader_neutral, \
                vader_compound, textblob_polarity, textblob_subjectivity, NRCL_positive, NRCL_negative, \
                NRCL_anticipation, NRCL_surprise, NRCL_trust, NRCL_joy, NRCL_fear, NRCL_anger, NRCL_sadness, \
                NRCL_disgust, VAD_dominance, VAD_arousal, VAD_valence, taboo_rate, LNA_handarm, \
                LNA_mouth, LNA_head, LNA_torso, LNA_footleg, LNP_visual, LNP_olfactory, LNP_haptic, \
                LNP_auditory, LNP_interoceptive, LNP_gustatory = ([] for _ in range(35))

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
                    LNA_footleg.append(users_stats[user]['cnt_LNA_Foot_leg'])
                    LNA_handarm.append(users_stats[user]['cnt_LNA_Hand_arm'])
                    LNA_head.append(users_stats[user]['cnt_LNA_Head'])
                    LNA_mouth.append(users_stats[user]['cnt_LNA_Mouth'])
                    LNA_torso.append(users_stats[user]['cnt_LNA_Torso'])
                    LNP_auditory.append(users_stats[user]['cnt_LNP_Auditory'])
                    LNP_gustatory.append(users_stats[user]['cnt_LNP_Gustatory'])
                    LNP_haptic.append(users_stats[user]['cnt_LNP_Haptic'])
                    LNP_interoceptive.append(users_stats[user]['cnt_LNP_Interoceptive'])
                    LNP_olfactory.append(users_stats[user]['cnt_LNP_Olfactory'])
                    LNP_visual.append(users_stats[user]['cnt_LNP_Visual'])

                _tmp = {'Id': nodes, 'avg_word_count': word_count, 'avg_unique_words': unique_words_cnt,
                        'avg_lexical_diversity': lexical_diversity, 'avg_vader_positive': vader_positive,
                        'avg_vader_negative': vader_negative, 'avg_vader_neutral': vader_neutral,
                        'avg_vader_compound': vader_compound, 'avg_textblob_polarity': textblob_polarity,
                        'avg_textblob_subjectivity': textblob_subjectivity,
                        'avg_NRCL_positive': NRCL_positive, 'avg_NRCL_negative': NRCL_negative,
                        'avg_NRCL_anticipation': NRCL_anticipation, 'avg_NRCL_surprise': NRCL_surprise,
                        'avg_NRCL_trust': NRCL_trust, 'avg_NRCL_joy': NRCL_joy, 'avg_NRCL_fear': NRCL_fear,
                        'avg_NRCL_anger': NRCL_anger, 'avg_NRCL_sadness': NRCL_sadness,
                        'avg_NRCL_disgust': NRCL_disgust, 'avg_VAD_arousal': VAD_arousal,
                        'avg_VAD_dominance': VAD_dominance, 'avg_VAD_valence': VAD_valence,
                        'avg_taboo_rate': taboo_rate,
                        'cnt_LNA_Foot_leg': LNA_footleg, 'cnt_LNA_Hand_arm': LNA_handarm, 'cnt_LNA_Head': LNA_head,
                        'cnt_LNA_Mouth': LNA_mouth, 'cnt_LNA_Torso': LNA_torso, 'cnt_LNP_Auditory': LNP_auditory,
                        'cnt_LNP_Gustatory': LNP_gustatory, 'cnt_LNP_Haptic': LNP_haptic,
                        'cnt_LNP_Interoceptive': LNP_interoceptive, 'cnt_LNP_Olfactory': LNP_olfactory,
                        'cnt_LNP_Visual': LNP_visual}
                node_labels = pd.DataFrame(_tmp)
                print('n_users:', len(users_stats))
                last_path = os.path.join(textstats_category, f'{period}.csv')
                node_labels.to_csv(last_path, index=False)
                # saving for each period a json file with username as key and (avg_polarization_score, label) as value
                period_filename = os.path.join(textstats_category, f'{period}.json')
                with open(period_filename, 'w') as fp:
                    json.dump(users_stats, fp, sort_keys=True, indent=4)
                print("--- %s seconds ---" % (time.time() - start_time))


def sentiment_analysis(text, vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity,
                       textblob_subjectivity):
    # VADER
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(text)
    vader_positive.append(vs['pos'])
    vader_negative.append(vs['neg'])
    vader_neutral.append(vs['neu'])
    # The compound score is computed by summing the valence scores of each word in the lexicon,
    # adjusted according to the rules, and then normalized to be between -1 (most extreme negative) and +1 (most
    # extreme positive)
    vader_compound.append(vs['compound'])
    # TEXTBLOB
    text = TextBlob(text)
    text.sentiment
    # Polarity is float which lies in the range of [-1,1] where 1 means positive statement and -1 means a
    # negative statement.
    textblob_polarity.append(text.sentiment.polarity)
    # Subjective sentences generally refer to personal opinion, emotion or judgment whereas objective refers to
    # factual information. Subjectivity is also a float which lies in the range of [0,1].
    textblob_subjectivity.append(text.sentiment.subjectivity)

    return vader_positive, vader_negative, vader_neutral, vader_compound, textblob_polarity, textblob_subjectivity


if __name__ == '__main__':
    cwd = os.getcwd()
    out_fold = os.path.join(cwd, '../RedditHandler_Outputs')
    ext_post = True
    ext_comment = True
    categories = {
        'anxiety': ['Anxiety', 'Anxietyhelp', 'anxietysuccess', 'anxietysupporters', 'socialanxiety', 'HealthAnxiety',
                    'ptsd', 'PTSDCombat', 'CPTSD', 'traumatoolbox', 'PanicParty', 'domesticviolence']}
    start = '01/05/2018'
    end = '01/07/2018'
    start_time = time.time()
    my_stats_generator = TextStatisticGenerator(out_fold, ext_post, ext_comment, categories, start,
                                                end)
    my_stats_generator.extract_statistics()
