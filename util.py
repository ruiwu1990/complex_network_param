import subprocess
import sys
import os
import json

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