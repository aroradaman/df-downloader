from flask import Flask
from flask import request
import threading
import mainframe
import os
app = Flask(__name__)

download_manager = mainframe.downloader()
config = download_manager.config

download_manager.start_background_sync_server()
download_manager.start_background_sync_client()

@app.route("/home")
def home():
	content = """<html>
			<head><title>DOWNLOADER</title>
			</head>
			<body>
			<!--
			<script src="http://cdnjs.cloudflare.com/ajax/libs/jquery/2.1.1/jquery.min.js" type="text/javascript">
			</script>;
			-->
			<h3>Enter url/filename here</h3><br>			
			<form action="/distributor" method="post">
			<h1>URL</h1> - <input type="text" name="url"><br>
			<h1>FILE</h1> - <input type="text" name="filename">
			<input type="submit">Submit</button>
			</body>
			</html>"""
	return content


@app.route("/distributor",methods=['POST'])
def distributor():
	url = request.form["url"]
	filename = request.form["filename"]
	threading.Thread(target=download_manager.distributor,args=(url,filename)).start()
	return '...'

@app.route("/local_init",methods=['POST'])
def local_init():
	start = int(request.form['start'])
	end = int(request.form['end'])
	reporting_ip = request.form['reporting_ip']
	url = request.form["url"]
	file_name = request.form["file_name"]
	threading.Thread(target=download_manager.local_init,args=(start,end,url,file_name,reporting_ip)).start()
	return '...'

@app.route("/fetch_local_data",methods=['POST'])
def fetch_local_data():
	start = int(request.form['start'])
	local_id = request.form['local_id']
	ip = request.form['ip']	
	file_name = request.form["file_name"]
	threading.Thread(target=download_manager.fetch_local_data,args=(local_id,ip,start,file_name)).start()
	return '...'

@app.route("/local_transfer",methods=['POST'])
def local_transfer() :
	local_id = request.form['local_id']
	with open(os.path.join(os.getcwd(),local_id),'rb') as f :
		content = f.read()
	os.unlink(os.path.join(os.getcwd(),local_id))
	return content

app.run(host=config['server_ip'], port=config['server_port'],debug=True)	