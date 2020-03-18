import json
from sodapy import Socrata
from datetime import datetime
from elasticsearch import Elasticsearch

def create_and_update_index(index_name, doc_type):
	es = Elasticsearch()
	try:
		es.indices.create(index=index_name)
	except Exception:
		pass

	es.indices.put_mapping(
		index = index_name,
		doc_type = doc_type,
		body = {
			doc_type{
				"properties": {"issue_date": {"type": "date"}, }
			}
		}
	)
	return es


def load_ES(results, es):
	for result in results:
		result["issue_date"] = datetime.strptime(
			result["issue_date"],
			'%m/%d/%Y',
			)

		res = es.index(index="violationparking-index", doc_type="violations", body = result, id = '_id')
		print(res['result'])


def get_data(APP_KEY, page_size, num_pages, output):
	try:
		client = Socrata('data.cityofnewyork.us', APP_KEY)
		es = create_and_update_index('violationparking-index', 'violations')


		if num_pages == '':
			num_row = client.get('nc67-uf89', select='COUNT(*)') #count num of row in dataset

			print("SOMETHING")

			total = int(num_row[0]['COUNT'])
			num_pages = (total / page_size) #get the num of pages 
			return num_pages


		else: 
			#es = Elasticsearch()

			for i in range (num_pages):
				data = client.get('nc67-uf89', limit=page_size, offset=i*page_size)

				if output != '':
					with open(output, 'a') as fout:
						for result in data:
							fout.write(json.dumps(result) + '\n')
							
							print("HERE")

							load_ES(result, es)

				else:
					for result in data:
						print(result + '\n')


	except Exception as e:
		print(f'Something went wrong {e}')
		raise 



"""
def load_ES():

	result = get_data()

	es = Elasticsearch()

	doc = {
		'author': 'Lesley',
		'text': 'Testing Elasticsearch',
		'timestamp': datetime.now(),
	}

	for r in result:
		_id = r['plate']
		ex.index(index='violationparking-index', doc_type='violations', id=_id, body=doc)
"""