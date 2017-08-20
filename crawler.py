import requests
import json
from pprint import pprint
from py2neo import Graph, Relationship, authenticate, Node

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

current_generation = 1

def initialize_neo4j():
	global graph

	authenticate(db_host + ':' + db_port, "neo4j", "1234")
	graph = Graph('http://{0}:{1}/db/data/'.format(db_host, db_port))

	graph.run("CREATE CONSTRAINT ON (a:Article) ASSERT a.page_id IS UNIQUE")
	graph.run("CREATE CONSTRAINT ON (a:Article) ASSERT a.title IS UNIQUE")

def parse_title_from_url(url):
	search_tag = '/wiki/'
	return url[(url.index(search_tag) + len(search_tag)):]

def query_child_articles_by_title(title, paging = '', get_metadata = True):
	global api_request_counter

	endpoint_url = children_url + '&titles=' + title + (('&plcontinue=' + paging) if len(paging) != 0 else '')
	json_response = requests.get(endpoint_url).json()

	api_request_counter += 1

	page_id = json_response["query"]["pages"].items()[0][0]
	result = json_response["query"]["pages"][page_id]["links"]

	if "continue" in json_response:
		result += query_child_articles_by_title(title, json_response["continue"]["plcontinue"], False)

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

def push_article(metadata, generation, visited):
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

		if not "generation" in node:
			node["generation"] = generation

		node["visited"] = visited

		node.push()

	return node

def relate_articles(parent, child):
	graph.merge(Relationship(parent, "CONTAINS", child))

def crawl_parent_and_children(title, parent_generation):
	data = query_child_articles_by_title(title)

	parent_article = push_article(data["metadata"], parent_generation, True)

	print u">> Found {} child articles for article with title: '{}'".format(len(data["links"]), title)

	for counter, article in enumerate(data["links"]):
		print u">> Pushing child article {}/{} with title: '{}'".format(counter, len(data["links"]), article["title"])

		if article["title"] == data["metadata"]["title"]:
			print u">> Skipping loop"
			continue

		metadata = {'title': article["title"]}
		child_article = push_article(metadata, parent_generation + 1, False)
		relate_articles(parent_article, child_article)
		
def start():
	initialize_neo4j()
	crawl_parent_and_children(parse_title_from_url(crawler_start_url), 0)

start()

# n = graph.find_one("Article", 'title', 'Brasil')
