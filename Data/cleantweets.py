"""
Script to clean the raw data.
"""
import os
import glob
import pickle
import re
import string
import sys

import langid
import nltk
import pandas as pd

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
TWEET_FILES = os.path.join(CURRENT_DIR, 'Tweets/Tweets-*.csv')
BOTSCORE_FILE = os.path.join(CURRENT_DIR, 'Users/botscores.pickle')

def normalize_tweets(tweets):
	"""
	Normalize the tweets.
	Returns:
		[non-lemmatiezed normalized tweets], [lemmatized and normalized tweets]
	"""
	url_regex = re.compile(
		r'^(?:http|ftp)s?://' # http:// or https://
		r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
		r'localhost|' #localhost...
		r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
		r'(?::\d+)?' # optional port
		r'(?:/?|[/?]\S+)$', re.IGNORECASE)
	tokenizer = nltk.tokenize.TweetTokenizer()
	lemmatizer = nltk.WordNetLemmatizer()
	printable_chars = set(string.printable)
	punctuations = set(string.punctuation)

	def _normalize_tweet(tweet_text):
		words, lemmatiezed_words, hashtags = [], [], set()
		clean_tweet = ''.join(filter(lambda c: c in printable_chars, tweet_text))
		for token, tag in nltk.pos_tag(tokenizer.tokenize(clean_tweet.lower())):
			if len(token) < 2 or url_regex.search(token):
				continue
			elif all(char in punctuations for char in token):
				continue
			elif token[0] == '@':
				token = '@person' #Replace all friend tags with a common token.
				tag = 'NNP'
			elif token[0] == '#':
				hashtags.add(token)
			words.append(token)
			lemmatiezed_words.append(_lemmatizeToken(token, tag))
		return words, lemmatiezed_words, hashtags

	def _lemmatizeToken(token, tag):
		tag = {
			'N': nltk.corpus.wordnet.NOUN,
			'V': nltk.corpus.wordnet.VERB,
			'R': nltk.corpus.wordnet.ADV,
			'J': nltk.corpus.wordnet.ADJ
		}.get(tag[0], nltk.corpus.wordnet.NOUN)
		return lemmatizer.lemmatize(token, tag)

	normalized_text, normalized_text_lemmatized, hashtags = [], [], []
	for i, x in enumerate(tweets):
		y = _normalize_tweet(x)
		normalized_text.append(y[0])
		normalized_text_lemmatized.append(y[1])
		hashtags.append(y[2])
		if i % 10000 == 0:
			print(f'Finished normalizing {i} tweets.')
	return normalized_text, normalized_text_lemmatized, hashtags

def clean_tweets(filename):
	df = pd.read_csv(filename, dtype={'CreatedAt': str, 'Text': str,
				'Id': 'int64', 'UserId': 'int64'},
				parse_dates=['CreatedAt'])
	print(f'Loaded file. {df.shape[0]} tweets found.')

	normalized_tweets = normalize_tweets(df.Text)
	df['NormalizedText'] = normalized_tweets[0]
	df['NormalizedTextLemmatized'] = normalized_tweets[1]
	df['HashTags'] = normalized_tweets[2]
	print('Normalized text.')

	english_tweets = df[df.apply(lambda x: langid.classify(x.Text)[0] == 'en', axis=1)]
	print(f'Removied non-english tweets. {english_tweets.shape[0]} tweets remain.')

	with open(BOTSCORE_FILE, 'rb') as file_handle:
		botscores = pickle.load(file_handle)
	bot_users = [int(user) for user in botscores if botscores[user]['scores']['universal'] >= 4 or botscores[user]['scores']['english'] >= 4]
	non_bots = english_tweets[~english_tweets.UserId.isin(bot_users)]
	print(f'Removed bots. {non_bots.shape[0]} tweets remain.')
	return non_bots

def main():
	dfs = []
	for filename in glob.glob(TWEET_FILES):
		print(filename)
		dfs.append(clean_tweets(filename))
	total = pd.concat(dfs, axis=0, ignore_index=True)
	total.to_pickle(os.path.join(CURRENT_DIR, 'Tweets/cleanTweets.pickle'))

if __name__ == '__main__':
	main()
