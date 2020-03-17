import argparse
import os

from src.project1.api import get_data

#from time import sleep
#from src.project1.api import load_ES

APP_KEY = os.environ['APP_KEY']

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--page_size", type = int)
	parser.add_argument("--num_pages", type = int) 
	parser.add_argument("--output",type= str)

	args = parser.parse_args()


	get_data(APP_KEY, args.page_size, args.num_pages, args.output)