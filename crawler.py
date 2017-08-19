import requests
import json
from pprint import pprint

base_url = 'https://pt.wikipedia.org/w/api.php?'
query_url = base_url + 'action=query&format=json'
backlinks_url = query_url + '&list=backlinks&blredirect=1&blnamespace=0&bllimit=max'
properties_url = query_url + '&prop=info'

# Jota_Quest
# 91593
# "0|4101637"

def get_article_id_from_title(title):
	endpoint_url = properties_url + '&titles=Jota_Quest'
	json_response = requests.get(endpoint_url).json()

	for element in json_response["query"]["pages"]:
		return element

def query_parent_articles_by_title(title, paging = ''):
	endpoint_url = backlinks_url + '&bltitle={}'.format(title) + ('&blcontinue={}'.format(paging) if len(paging) != 0 else '')
	json_response = requests.get(endpoint_url).json()

	print json.dumps(json_response, indent = 4)

	# return json_response["query"]["backlinks"][1]["title"]

def query_parent_articles_by_id(id, paging = ''):
	endpoint_url = backlinks_url + '&blpageid={}'.format(id) + ('&blcontinue={}'.format(paging) if len(paging) != 0 else '')
	json_response = requests.get(endpoint_url).json()

	print json.dumps(json_response, indent = 4)

	# return json_response["query"]["backlinks"][1]["title"]	

# query_parent_articles_by_id(91593)
# query_parent_articles_by_title('Jota_Quest')
print get_article_id_from_title('Jota_Quest')
# title = query_parent_articles('Jota_Quest')
