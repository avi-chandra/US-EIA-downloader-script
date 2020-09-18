# Import modules required for this script
import os
import sys
import json
import pandas
import urllib3
import argparse
import tabulate


class EiaManager:
	def __init__(self, api_key, base_url, http_pool, frequency_dict):
		"""
		Parameters
		----------
		api_key : str
			EIA API key for authentication
		base_url : str
			The URL for api request
		http_pool : urllib poolmanager object
			The urllib object for web request connections
		frequency_dict : dict
			Dictionary which defines value of frequency at which data is available
			{'annual':'A', 'monthly':'M'}
		"""
		self.api_key = api_key
		self.base_url = base_url
		self.http_pool = http_pool
		self.frequency_dict = frequency_dict

	@staticmethod
	def info():
		print("Current version supports only categories under electricity data set from EIA")
		print("User can select one category at a time to traverse through all child category and download data")
		print("Categories available are")
		print(
			"To download the series data under any category run the script in this form eia.py -download ${category_id}")
		print(
			"For any category id with download option if there is no any series the script will exit displaying categories list")

	def web_request(self, url, payload):
		"""
		Function executes api call and returns json response

		Parameters
		----------
		url : str
			EIA exact API url to fetch data
		payload : dict
			The parameters need to be passed with url to get the data

		Returns
		-------
		json
			a json which contains the data from api in the form of dictionary 

		"""
		try:
			response = self.http_pool.request('GET', url, fields=payload)
		except Exception as e:
			print(e)
			sys.exit()
		json_response = json.loads(response.data.decode('utf-8'))
		if json_response.get('data',None):
			return json_response.get('data').get('error')
		else:
			return json_response

	def eia_category(self, category_id):
		"""
		Function executes api call to fetch category_id and returns parsed json response

		Parameters
		----------
		category_id : int
			category id required to identify the categories available on the eia web

		Returns
		-------
		json
			a parsed json which contains the category and series id

		"""

		url = self.base_url + 'category/'
		payload = {
			'api_key': self.api_key,
			'category_id': category_id
		}

		categories_dict = self.web_request(url, payload)

		return categories_dict

	def eia_series(self, series_id):
		"""
		Function executes api call to fetch series_id and returns parsed json response

		Parameters
		----------
		series_id : str
			series id required to identify the series available on the eia web to fetch data

		Returns
		-------
		json
			a parsed json which contains the data corresponding to the series under selected category

		"""

		url = self.base_url + 'series/'
		payload = {
			'api_key': self.api_key,
			'series_id': series_id
		}

		series_dict = self.web_request(url, payload)

		return series_dict

	def download_data(self, category_id, combined_file=True):
		"""
		Function executes api call to download all the series for given category_id

		Parameters
		----------
		category_id : int
			category id required to identify the categories available on the eia web to fetch underlying series data
		combined_file : boolean
			if True saves data for all series id in a single file

		Returns
		-------
		csv
			a parsed csv file will be saved in current working director
		"""
		category_call = self.eia_category(category_id)
		if category_call.get('category').get("childseries", None):
			series_list = category_call.get('category').get('childseries', None)
			category_name = category_call.get('category').get('name', None)
			series_id_list = [i.get('series_id') for i in series_list]
			combined_data = []
			for series_id in series_id_list:
				print(f"Downloading data for {series_id}")
				series_data = self.eia_series(series_id)
				series_name, frequency, units = [
					series_data.get('series')[0]['name'], series_data.get('series')[0]['f']
					, series_data.get('series')[0]['units']
				]
				data_df = pandas.DataFrame(series_data.get('series')[0]['data'], columns=['period', 'value'])
				data_df['series_name'] = series_name
				data_df['frequency'] = frequency
				data_df['units'] = units
				data_df = data_df[['series_name', 'period', 'value', 'frequency', 'units']]
				if combined_file:
					combined_data.append(data_df)
				else:
					data_df.to_csv(os.path.join(os.getcwd(), f"{series_name}.csv"), index=False)
			if combined_file:
				pandas.concat(combined_data).to_csv(os.path.join(os.getcwd(), f"{category_name}.csv"), index=False)
		else:
			print("No series data available under this category")

		return os.getcwd()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-k', action='store', dest='api_key', help='Set a api key for authentication')
	parser.add_argument('-d', action='store_true', dest='download_flag', default=False, help='Set a download to true')
	parser.add_argument('-c', action='store', dest='category_id', type=int, help='Store a category id value for which '
																				'data to be download')
	args = parser.parse_args()
	HTTP_POOL = urllib3.PoolManager()
	BASE_URL = 'http://api.eia.gov/'
	FREQUENCY_DICT = {'annual': 'A', 'monthly': 'M', 'quarterly': 'Q'}
	API_KEY = args.api_key if args.api_key else sys.exit("API KEY missing please register here "
													  "https://www.eia.gov/opendata/register.php for API KEY ")
	eia = EiaManager(API_KEY, BASE_URL, HTTP_POOL, FREQUENCY_DICT)
	ecategory = eia.eia_category(0)
	if type(ecategory) == str:
		print(ecategory)
	else:
		if args.download_flag:
			if args.category_id:
				print(f"Downloading data for category_id {args.category_id}")
				download_path = eia.download_data(args.category_id)
				print(f"Data downloaded to {download_path}")
			else:
				print("With download option category id is required")
		else:
			print(tabulate.tabulate(ecategory.get('category')['childcategories'], headers="keys", tablefmt="grid"))
			main_categories = [i.get('category_id') for i in ecategory.get('category')['childcategories']]
			flag = True
			while flag:
				user_response = input("Select category id or type quit and enter to exit : ")
				try:
					if (user_response != 'quit') and (user_response.isdigit()):
						cid = int(user_response)
						child_categories = eia.eia_category(cid)
						if child_categories.get('category')['childcategories']:
							print(tabulate.tabulate(child_categories.get('category')['childcategories'],
													headers="keys", tablefmt="grid"))
							if child_categories.get('category')['childseries']:
								print("""
								In addition with child categories there are 
								series data also available listed below
								""")
								print(tabulate.tabulate(child_categories.get('category')['childseries'],
														headers="keys", tablefmt="grid"))
						else:
							print(f"All available series_id in category_id : {cid} are")
							print("""User can download data for various combinations available
									Downloaded data will be saved as CSV in current working directory""")
							print(tabulate.tabulate(child_categories.get('category')['childseries'],
													headers="keys"))
					else:
						flag = False
				except Exception as e:
					print(e)
