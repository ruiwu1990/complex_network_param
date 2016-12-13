import subprocess
import sys
import os
import json
import networkx as nx
from networkx.utils import (powerlaw_sequence, create_degree_sequence)
from networkx.readwrite import json_graph

app_path = os.path.dirname(os.path.abspath(__file__))

def exec_apache_spark_scala():
	'''
	this function run scala script
	'''
	log_path = app_path + '/scala_log.txt'
	err_log_path = app_path + '/scala_err_log.txt'
	# result_file = app_path + '/rf_result.txt'
	tag = '-i'
	exec_file = 'import_graph.scala'
	command = ['spark-shell',tag,exec_file]
	# execute the model
	with open(log_path, 'wb') as process_out, open(log_path, 'rb', 1) as reader, open(err_log_path, 'wb') as err_out:
		process = subprocess.Popen(
			command, stdout=process_out, stderr=err_out, cwd=app_path)

	# this waits the process finishes
	process.wait()
	return True

def parse_result_file(filename):
	'''
	this file parse result file
	and return json file
	'''
	fp = open(filename,'r')
	# .strip() remove  \n
	degree_distribution = fp.readline().strip()
	chosen_closeness_cen = fp.readline().strip()
	hop_distribution = fp.readline().strip()
	fp.close()
	degree_distribution_arr = degree_distribution.split("&&")
	degree_distribution_arr.pop()

	hop_distribution_arr = hop_distribution.split(',')

	# maybe add indegree and outdegree thing
	dict_info = {'degree_distribution':degree_distribution_arr,\
				 'chosen_closeness_cen':chosen_closeness_cen,\
				 'hop_distribution':hop_distribution_arr}

	return json.dumps(dict_info)

def generate_scale_free_power_law_graph(num,exp,seed):
	'''
	this function generates a scale free with power law
	graph and write it into a file with .net format
	'''
	sequence = create_degree_sequence(num, powerlaw_sequence, exponent=exp)
	graph = nx.configuration_model(sequence, seed=seed)
	loops = graph.selfloop_edges()
	json_str = json_graph.dumps(graph)
	dict_graph = json.loads(json_str)
	output_file = open('scale_free_power_law.net','w')
	# write nodes
	total_node_num = len(dict_graph['nodes'])
	output_file.write('*Vertices '+str(total_node_num)+'\n')
	count = 1
	for item in dict_graph['nodes']:
		# +1 coz id starts with 0, it should start from 1
		output_file.write('  '+str(count)+'  '+str(item['id']+1)+'\n')
		count = count + 1
	# write edges, links
	output_file.write('*Edges'+'\n')
	for item in dict_graph['links']:
		# +1 coz source and target starts with 0, it should start from 1
		output_file.write(str(item['source']+1)+' '+str(item['target']+1)+' 1'+'\n')
	output_file.close()