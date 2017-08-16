import logging
import webbrowser
import csv
import time
import re
import sys
import os
from datetime import datetime, timedelta
import itertools
import argparse
from collections import OrderedDict

# Example Usage:
#
#
#
#
#
#

def map_redirects(redirects_map_file):
	# Create return array with first row of keys set
	redirects_dict = {}

	# Pop off CSV Legend
	redirects_legend = next(redirects_map_file)

	for row in redirects_map_file:
		line = row.split(",")

		logging.info("Storing redirect for %s in dict", line[0]);
		redirects_dict[ line[0] ] = line[1].replace("\n", '')

	return redirects_dict

def get_redirect(url, redirects_dict):
	logging.info("Getting redirect URL for %s", url);

	# Default to false
	redirect_url = False

	for pattern, result_url in redirects_dict.items():
		if pattern in url:
			redirect_url = result_url
			# found one, can exit for loop now
			break

	return redirect_url


def parse_command_line_options():
	"""
	Parses arguments from the command line and returns them in the form of an ArgParser object.
	"""
	parser = argparse.ArgumentParser(description='Map Google crawl error exports to redirect URLs, exporting a CSV that works with WPCOM Legacy Redirector.')

	# Exported CSV file from Google Search Console
	parser.add_argument('file', type=str, help='Path to CSV export of Google crawl errors. The pageUrl must be the first item in every row.')

	# Pass custom redirect mappings for output
	parser.add_argument('--redirect_map', type=str, default='wp_redirect_mapping.csv', help='File path of a config file containing settings for this Search Console property.')

	# Custom output location for exported CSV mappings file
	parser.add_argument('--output_location', type=str, default='exports/', help='The folder output the wp_redirects.csv file for wpcom-legacy-redirector.')

	return parser.parse_args()


def main():
	"""
	Fetch and parse all command line options.
	Dispatch queries to the GSC API.
	"""
	args = parse_command_line_options()

	# Load CSV export file containing all the crawl errors
	loaded_file = open(args.file, 'r')

	# Pop off CSV Legend - TODO
	# export_map_legend = next(loaded_file)
	# export_map_legend = re.sub('[^a-zA-Z0-9-_,*.]', '', export_map_legend)
	# export_map_legend = export_map_legend.split(',')

	# store file name for output later
	file_name = os.path.basename(args.file).replace('.csv', '')

	logging.info("Loaded CSV Export")

	# Load CSV redirect mapping file
	redirects_map_file = open(args.redirect_map, 'r')
	redirects_dict = map_redirects(redirects_map_file)

	# Setup output variables
	output = []
	output.append(['/old/path','/new/path'])
	output_file = os.path.join(
		args.output_location,
		"wp_redirects_{}.csv".format( file_name )
	)

	# Setup leftover variables
	leftovers = []
	# Add legend for original export to our leftovers array,
	# mismatches will be stored here later & we want to retain the structure.
	# TODO
	# leftovers.append(export_map_legend)

	leftovers_file = os.path.join(
		args.output_location,
		"wp_redirects_nomatch_{}.csv".format( file_name )
	)

	# Loop through each row
	for row in loaded_file:
		# Parse row and generate new row for output
		line = row.split(',')
		print(line)

		# Returns false when no redirect url is found in the dict, if found it returns the matched redirect URL from the dict
		redirect_url = get_redirect(line[0], redirects_dict)

		# If row doesn't fall into predefined category, store entire row in leftovers instead.
		if False == redirect_url:
			leftovers.append([ line[0] ])
			continue

		bad_url = '/' + line[0]
		output.append([ bad_url, redirect_url ])

	# Close our open files
	loaded_file.close()
	redirects_map_file.close()

	# print(leftovers)
	# print(output)

	# Save WP output to CSV file
	with open(output_file, 'w', newline="", encoding="utf-8-sig") as file_handle:
		print('creating: ', output_file)
		csvwriter = csv.writer(file_handle)
		csvwriter.writerows(output)

	# Save leftovers output
	with open(leftovers_file, 'w', newline="", encoding="utf-8-sig") as file_handle:
		print('creating: ', leftovers_file)
		csvwriter = csv.writer(file_handle)
		csvwriter.writerows(leftovers)

	logging.info("Task Complete")



if __name__ == '__main__':
	main()