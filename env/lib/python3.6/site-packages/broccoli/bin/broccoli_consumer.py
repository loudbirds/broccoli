#!/usr/bin/env python

import os
import sys

from broccoli.consumer import Consumer
from broccoli.consumer_options import ConsumerConfig
from broccoli.consumer_options import OptionParserHandler
from broccoli.utils import load_class


def err(s):
	sys.stderr.write('\033[91m%s\033[0m\n' % s)


def load_broccoli(path):
	try:
		return load_class(path)
	except:
		cur_dir = os.getcwd()
		if cur_dir not in sys.path:
			sys.path.insert(0, cur_dir)
			return load_broccoli(path)
		err('Error importing {}'.format(path))
		raise


def consumer_main():
	parser_handler = OptionParserHandler()
	parser = parser_handler.get_option_parser()
	options, args = parser.parse_args()

	if len(args) == 0:
		err('Error: 	missing import path to `Broccoli` instance')
		err('Example: 	broccoli_queue.py app_example.broccoli')
		sys.exit(1)

	options = {k: v for k, v in options.__dict__.items() 
			   if v is not None}
	config = ConsumerConfig(**options)
	config.validate()

	broccoli_instance = load_broccoli(args[0])
	# TODO: replace this with a logging decorator that does this
	config.setup_logger()

	consumer = Consumer(broccoli_instance, **config.values)
	consumer.run()



if __name__ == '__main__':
	consumer_main()