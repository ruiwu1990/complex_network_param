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