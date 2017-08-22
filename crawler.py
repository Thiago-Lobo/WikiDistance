# -*- coding: utf-8 -*-

import requests
import json
from pprint import pprint
from py2neo import Graph, Relationship, authenticate, Node
from datetime import datetime as dt
import random

local_cache = []

base_url = 'https://pt.wikipedia.org/w/api.php'
query_url = base_url + '?action=query&format=json'
backlinks_url = query_url + '&list=backlinks&blredirect=1&blnamespace=0&bllimit=max'
properties_url = query_url + '&prop=info|links'
children_url = query_url + '&prop=info|links&inprop=url&plnamespace=0&pllimit=max'

db_host = 'localhost'
db_port = '7474'

graph = None
transaction = None

crawler_start_url = 'https://pt.wikipedia.org/wiki/Brasil'

api_request_counter = 0
visits_counter = 0

def initialize_neo4j():
	global graph

	authenticate(db_host + ':' + db_port, "neo4j", "1234")
	graph = Graph('http://{0}:{1}/db/data/'.format(db_host, db_port))

	graph.run("CREATE CONSTRAINT ON (a:Article) ASSERT a.page_id IS UNIQUE")
	graph.run("CREATE CONSTRAINT ON (a:Article) ASSERT a.title IS UNIQUE")

def update_cache():
	global local_cache

	result = graph.data("MATCH (n:Article) WHERE n.visited=false RETURN n")

	local_cache = [x["n"]["title"] for x in result]

	print "[{}] Local cache updated. Cache size: {}".format(current_time_string(), len(local_cache))

def current_time_string():
	return dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")

# TESTAR
def parse_title_from_url(url):
	search_tag = '/wiki/'
	return u"{}".format(url[(url.index(search_tag) + len(search_tag)):]).encode('utf-8')

def query_article_data_by_title(title, paging = '', get_metadata = True):
	global api_request_counter

	endpoint_url = children_url + '&titles=' + title + (('&plcontinue=' + paging) if len(paging) != 0 else '')
	print "[{}] Getting from URL: {}".format(current_time_string(), endpoint_url)

	json_response = requests.get(endpoint_url).json()

	api_request_counter += 1

	page_id = json_response["query"]["pages"].items()[0][0]

	if page_id == "-1":
		return -1

	if "links" in json_response["query"]["pages"][page_id]:
		result = json_response["query"]["pages"][page_id]["links"]
	else:
		result = []

	if "continue" in json_response:
		result += query_article_data_by_title(title, json_response["continue"]["plcontinue"], False)

	if get_metadata:
		result = {
			'links': result,
			'metadata': {
				'page_id': page_id,
				'full_url': json_response["query"]["pages"][page_id]["fullurl"],
				'title': title
			}
		}

	return result

def push_article(metadata, visited = False):
	node = Node("Article", title = metadata["title"])
	graph.merge(node)

	if not node["visited"]:
		if "page_id" in metadata:
			node["page_id"] = metadata["page_id"]

		if "full_url" in metadata:
			node["full_url"] = metadata["full_url"]

		node["visited"] = visited

		node.push()

	return node

def relate_articles(parent, child):
	graph.merge(Relationship(parent, "CONTAINS", child))

def delete_article(title):
	graph.run("MATCH (a:Article) WHERE a.title=\"{}\" DETACH DELETE a".format(title))

def visit_article(title):
	global visits_counter

	data = query_article_data_by_title(title)

	visits_counter += 1

	if title in local_cache:
		local_cache.remove(title)

	if data == -1:
		delete_article(title)
		return

	parent_article = push_article(data["metadata"])

	print "[{}] Found {} child articles for article with title: '{}'".format(current_time_string(), len(data["links"]), title)

	for counter, article in enumerate(data["links"]):
		if article["title"] in local_cache:
			continue

		if article["title"] == data["metadata"]["title"]:
			continue

		metadata = {'title': article["title"]}
		local_cache.append(article["title"])
		child_article = push_article(metadata)
		relate_articles(parent_article, child_article)

	parent_article = push_article(data["metadata"], visited = True)
		
def query_article_count():
	result = graph.data("MATCH (n:Article) RETURN count(n)")

	return result[0].items()[0][1] 

def get_unvisited_article_title():
	result = graph.data("MATCH (a:Article) WHERE a.visited = false return a limit 100")
	
	if len(result) == 0:
		return "_____1noarticle"

	return u"{}".format(result[random.randint(0, len(result) - 1)]["a"]["title"]).encode("utf-8")

def start():
	global visits_counter

	random.seed(dt.now())
	initialize_neo4j()
	update_cache()

	if query_article_count() == 0:
		visit_article(parse_title_from_url(crawler_start_url))

	unvisited_title = get_unvisited_article_title()

	while unvisited_title != "_____1noarticle":
		try:
			if visits_counter != 0 and visits_counter % 10000 == 0:
				update_cache()
			visit_article(unvisited_title)
			unvisited_title = get_unvisited_article_title()
		except Exception, e:
			print u"str(e)"
			pass

start()
