import requests
import json
from pprint import pprint
from py2neo import Graph, Relationship, authenticate, Node
from datetime import datetime as dt

base_url = 'https://pt.wikipedia.org/w/api.php'
query_url = base_url + '?action=query&format=json'
backlinks_url = query_url + '&list=backlinks&blredirect=1&blnamespace=0&bllimit=max'
properties_url = query_url + '&prop=info|links'
children_url = query_url + '&prop=info|links&inprop=url&plnamespace=0&pllimit=max'

db_host = '192.168.1.4'
db_port = '7474'

graph = None

crawler_start_url = 'https://pt.wikipedia.org/wiki/Brasil'

api_request_counter = 0

def initialize_neo4j():
	global graph

	authenticate(db_host + ':' + db_port, "neo4j", "1234")
	graph = Graph('http://{0}:{1}/db/data/'.format(db_host, db_port))

	graph.run("CREATE CONSTRAINT ON (a:Article) ASSERT a.page_id IS UNIQUE")
	graph.run("CREATE CONSTRAINT ON (a:Article) ASSERT a.title IS UNIQUE")

def current_time_string():
	return dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def parse_title_from_url(url):
	search_tag = '/wiki/'
	return url[(url.index(search_tag) + len(search_tag)):]

def query_article_data_by_title(title, paging = '', get_metadata = True):
	global api_request_counter

	endpoint_url = children_url + '&titles=' + title + (('&plcontinue=' + paging) if len(paging) != 0 else '')
	print u"[{}] Getting from URL: {}".format(current_time_string(), endpoint_url).encode('utf-8')

	json_response = requests.get(endpoint_url).json()

	api_request_counter += 1

	page_id = json_response["query"]["pages"].items()[0][0]
	result = json_response["query"]["pages"][page_id]["links"]

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

def push_article(metadata, visited):
	node = Node("Article", title = metadata["title"])
	graph.merge(node)

	# print ">> Node:"
	# for key, value in dict(node).items():
	# 	print u"{}: {}".format(key, value)

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

def visit_article(title):
	data = query_article_data_by_title(title)

	parent_article = push_article(data["metadata"], True)

	print u"[{}] Found {} child articles for article with title: '{}'".format(current_time_string(), len(data["links"]), title).encode('utf-8')

	for counter, article in enumerate(data["links"]):
		print u"[{}] Pushing child article {}/{} with title: '{}'".format(current_time_string(), counter, len(data["links"]), article["title"]).encode('utf-8')

		if article["title"] == data["metadata"]["title"]:
			print u"[{}] Skipping loop".format(current_time_string()).encode('utf-8')
			continue

		metadata = {'title': article["title"]}
		child_article = push_article(metadata, False)
		relate_articles(parent_article, child_article)
		
def query_article_count():
	result = graph.data("MATCH (n:Article) RETURN count(n)")

	return result[0].items()[0][1] 

def get_unvisited_article():
	result = graph.data("MATCH (a:Article) WHERE a.visited = false return a limit 1")
	
	if len(result) == 0:
		return "_____1noarticle"

	return result[0]["a"]["title"]

def start():
	initialize_neo4j()
	
	if query_article_count() == 0:
		visit_article(parse_title_from_url(crawler_start_url))

	unvisited_title = get_unvisited_article()

	while unvisited_title != "_____1noarticle":
		visit_article(unvisited_title)
		unvisited_title = get_unvisited_article()

start()
