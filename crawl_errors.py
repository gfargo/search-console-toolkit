import logging
import webbrowser
from flatten_json import flatten_json
from pandas.io.json import json_normalize
import csv
import json
import time
import sys
import os
from datetime import datetime, timedelta
import itertools
import argparse
from collections import OrderedDict
import httplib2
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

WEBMASTER_CREDENTIALS_FILE_PATH = "webmaster_credentials.dat"

# Example Usage:
#
# Pull all reports:
# python3 crawl_errors.py site_url
#
# e.g.
# python3 crawl_errors.py https://www.additudemag.com/
#
#
# Pull specific category:
# python3 crawl_errors.py site_url --category category
#
# e.g.
# python3 crawl_errors.py https://www.additudemag.com/ --category notFound


def rate_limit(max_per_minute):
	"""
	Decorator function to prevent more than x calls per minute of any function
	Args:
		max_per_minute. Numeric type.
		The maximum number of times the function should run per minute.
	"""
	min_interval = 60.0 / float(max_per_minute)
	def decorate(func):
		last_time_called = [0.0]
		def rate_limited_function(*args, **kwargs):
			elapsed = time.clock() - last_time_called[0]
			wait_for = min_interval - elapsed
			if wait_for > 0:
				time.sleep(wait_for)
			ret = func(*args, **kwargs)
			last_time_called[0] = time.clock()
			return ret
		return rate_limited_function
	return decorate


def acquire_new_oauth2_credentials(secrets_file):
	"""
	Args:
		secrets_file. The file path to a JSON file of client secrets, containing:
			client_id; client_secret; redirect_uris; auth_uri; token_uri.
	Returns:
		credentials for use with Google APIs
	"""
	flow = flow_from_clientsecrets(
		secrets_file,
		scope="https://www.googleapis.com/auth/webmasters.readonly",
		redirect_uri="http://localhost")
	auth_uri = flow.step1_get_authorize_url()
	webbrowser.open(auth_uri)
	print("Please enter the following URL in a browser " + auth_uri)
	auth_code = input("Enter the authentication code: ")
	credentials = flow.step2_exchange(auth_code)
	return credentials


def load_oauth2_credentials(secrets_file):
	"""
	Args:
		secrets_file. The file path to a JSON file of client secrets.
	Returns:
		credentials for use with Google APIs.
	Side effect:
		If the secrets file did not exist, fetch the appropriate credentials and create a new one.
	"""
	storage = Storage(WEBMASTER_CREDENTIALS_FILE_PATH)
	credentials = storage.get()
	if credentials is None or credentials.invalid:
		credentials = acquire_new_oauth2_credentials(secrets_file)
	storage.put(credentials)
	return credentials


def create_search_console_client(credentials):
	"""
	The search console client allows us to perform queries against the API.
	To create it, pass in your already authenticated credentials

	Args:
		credentials. An object representing Google API credentials.
	Returns:
		service. An object used to perform queries against the API.
	"""
	http_auth = httplib2.Http()
	http_auth = credentials.authorize(http_auth)
	service = build('webmasters', 'v3', http=http_auth)
	return service


def generate_filters(**kwargs):
	"""
	Yields a filter list for each combination of the args provided.
	"""
	kwargs = OrderedDict((k, v) for k, v in kwargs.items() if v)
	dimensions = kwargs.keys()
	values = list(kwargs.values())
	for vals in itertools.product(*values):
		yield [{
			'dimension': dim,
			'operator': 'equals',
			'expression': val} for dim, val in zip(dimensions, vals)
			  ]


@rate_limit(200)
def execute_request(service, property_uri, category, platform, max_retries=5, wait_interval=4,
					retry_errors=(503, 500)):
	"""
	Executes a searchanalytics request.
	Args:
		service: The webmasters service object/client to use for execution.
		property_uri: Matches the URI in Google Search Console.
		request: The request to be executed.
		max_retries. Optional. Sets the maximum number of retry attempts.
		wait_interval. Optional. Sets the number of seconds to wait between each retry attempt.
		retry_errors. Optional. Retry the request whenever these error codes are encountered.
	Returns:
		An array of response rows.
	"""

	# print([method for method in dir(service) if callable(getattr(service, method))])

	response = None
	retries = 0
	while retries <= max_retries:
		try:
			response = service.urlcrawlerrorssamples().list(siteUrl=property_uri, category=category, platform=platform).execute()
		except HttpError as err:
			decoded_error_body = err.content.decode('utf-8')
			json_error = json.loads(decoded_error_body)
			if json_error['error']['code'] in retry_errors:
				time.sleep(wait_interval)
				retries += 1
				continue
		break
	return response

def parse_response(response, platform):
	logging.info("Parsing response for %s", platform);
	# Create return array with first row of keys set
	output = []
	output.append(['pageUrl', 'platform', 'last_crawled', 'first_detected', 'responseCode', 'linkedFrom'])

	if response is not None and 'urlCrawlErrorSample' in response:

		# Loop through each json object in response
		for response_object in response['urlCrawlErrorSample']:

			# ensure we have a valid error sample to work with
			if response_object is not None:

				# Default some variables
				linkedFromUrls = ', '.join(response_object['urlDetails']['linkedFromUrls']) if 'urlDetails' in response_object else ""
				responseCode = response_object['responseCode'] if 'responseCode' in response_object else ""

				# Generate CSV output row and append to returned array
				output_row = [ response_object['pageUrl'], platform, response_object['last_crawled'], response_object['first_detected'], responseCode, linkedFromUrls]
				output.append(output_row)

	return output


def parse_command_line_options():
	"""
	Parses arguments from the command line and returns them in the form of an ArgParser object.
	"""
	parser = argparse.ArgumentParser(description="Query the Google Search Console API for crawl errors.")
	parser.add_argument('property_uri', type=str, help='The property URI to query. Must exactly match a property URI in Google Search Console')

	parser.add_argument('--secrets_file', type=str, default='credentials.json', help='File path of your Google Client ID and Client Secret')
	parser.add_argument('--config_file', type=str, help='File path of a config file containing settings for this Search Console property.')
	parser.add_argument('--output_location', type=str, help='The folder output location of the script.', default="")
	parser.add_argument('--url_type', type=str, help='A string to add to the beginning of the file', default="")
	# parser.add_argument('--max-rows-per-day', '-n', type=int, default=500, help='The maximum number of rows to return for each day in the range')

	filters = parser.add_argument_group('filters')
	filters.add_argument('--category', type=str, help='The crawl error category parameter. See https://developers.google.com/webmaster-tools/search-console-api-original/v3/urlcrawlerrorssamples/list')
	filters.add_argument('--platform', type=str, help='The user agent type (platform) that made the request. For example: web')
	return parser.parse_args()


def main():
	"""
	Fetch and parse all command line options.
	Dispatch queries to the GSC API.
	"""
	args = parse_command_line_options()

	# Prepare the API service
	credentials = load_oauth2_credentials(args.secrets_file)
	service = create_search_console_client(credentials)

	# Check for custom category
	categories = [ args.category ] if args.category is not None else [ "authPermissions", "flashContent", "manyToOneRedirect", "notFollowed", "notFound", "other", "roboted", "serverError", "soft404" ]

	for category in categories:

		output_file = os.path.join(
			args.output_location,
			"{}_{}.csv".format(category, datetime.today())
		)

		# check for custom platform
		platforms = [ args.platform ] if args.platform is not None else [ "mobile", "smartphoneOnly", "web" ]

		for platform in platforms:

			# Make API request for current category and platform
			response = execute_request(service, args.property_uri, category, platform)

			csv_rows = parse_response(response, platform)

		# Write out our results to CSV file
		with open(output_file, 'w', newline="", encoding="utf-8-sig") as file_handle:
			csvwriter = csv.writer(file_handle)
			csvwriter.writerows(csv_rows)

		logging.info("Query for %s complete", "{}_{}.csv".format(args.category, args.platform))


if __name__ == '__main__':
	main()