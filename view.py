from flask import Flask, render_template, send_from_directory, request
import util
import os
app = Flask(__name__)

app_path = os.path.dirname(os.path.abspath(__file__))

@app.route('/')
def index():
	util.exec_apache_spark_scala()
	return render_template('index.html')

@app.route('/api/get_result')
def get_result_data():
	filename = 'result.txt'
	return util.parse_result_file(filename)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')