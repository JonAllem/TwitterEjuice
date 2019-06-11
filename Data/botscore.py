"""
Script to score users as bot or not using botometer.
"""
import os
import csv
import threading
import pickle

import botometer

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def get_botscores(start, end):
	"""
	Apply botometer on file Users/AllUsers.csv.
	Args:
		start (int): The line number of the first user to read from.
		end (int): The line number of the last to read.
	Returns the botometer result per user.
	"""
	users_file = os.path.join(CURRENT_DIR, 'Users\\AllUsers.csv')
	with open(users_file, 'r', newline='') as file_handle:
		users = []
		reader = csv.reader(file_handle, delimiter=',')
		for i, row in enumerate(reader):
			if i >= start and i < end:
				users.append(row[0])

	bom = botometer.Botometer(
		mashape_key='',
		consumer_key='',
		consumer_secret='',
		botometer_api_url='https://botometer-pro.p.mashape.com',
		wait_on_ratelimit=True
	)

	results = {}
	for i, user in enumerate(users):
		try:
			score_dict = bom.check_account(user)
			results[user] = {
				'cap': score_dict['cap'],
				'scores': {
					'english': score_dict['display_scores']['english'],
					'universal': score_dict['display_scores']['universal']
				}
			}
			if i % 50 == 0:
				print(f'Processsed {start}-{end}:{i} users.')
		except Exception as e:
			print(f'Exception on user {user}')
	print(f'Finished {start}-{end}')
	return results

class ThreadWithReturnValue(threading.Thread):
	def __init__(self, group=None, target=None, name=None,
				 args=(), kwargs={}, Verbose=None):
		threading.Thread.__init__(self, group, target, name, args, kwargs)
		self._return = None

	def run(self):
		if self._target is not None:
			self._return = self._target(*self._args,
												**self._kwargs)

	def join(self, *args):
		threading.Thread.join(self, *args)
		return self._return

def main():
	thread1 = ThreadWithReturnValue(target=get_botscores, args=(1, 2200))
	thread2 = ThreadWithReturnValue(target=get_botscores, args=(2200, 4400))
	thread3 = ThreadWithReturnValue(target=get_botscores, args=(4400, 6684))

	thread1.start()
	thread2.start()
	thread3.start()

	final_result = {**thread1.join(), **thread2.join(), **thread3.join()}
	pickle_file = os.path.join(CURRENT_DIR, 'Users\\botscores.pickle')
	with open(pickle_file, 'wb') as file_handle:
		pickle.dump(final_result, file_handle)

if __name__ == '__main__':
	main()
