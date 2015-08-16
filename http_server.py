from flask import Flask
from flask import request
import threading
import mainframe
import os
app = Flask(__name__)

config = mainframe.get_config()
mainframe.open_echo_zone(config['server_ip'],config['server_port'])
download_manager = mainframe.downloader(config)

@app.route("/home")
def hello():
	content = """<html>
			<head><title>DOWNLOADER</title>
			</head>
			<body>
			<!--
			<script src="http://cdnjs.cloudflare.com/ajax/libs/jquery/2.1.1/jquery.min.js" type="text/javascript">
			</script>;
			-->
			<h3>Enter url/filename here</h3><br>
			<form method="GET" action="http://""" + config['home_ip'] + """:""" + str(config['server_port']) + """/distributor">
			<h1>URL</h1> - <input type="text" name="url"><br>
			<h1>FILE</h1> - <input type="text" name="filename">
			<input type="submit">Submit</button>
			</body>
			</html>"""
	return content

@app.route("/distributor")
def distributor():
	url = request.args.get("url")
	filename = request.args.get("filename")
	threading.Thread(target=download_manager.distributor,args=(url,filename)).start()
	return '...'

@app.route("/local_init")
def local_init():
	start = int(request.args.get('start'))
	end = int(request.args.get('end'))
	reporting_ip = request.args.get('reporting_ip')	
	url = request.args.get("url")
	file_name = request.args.get("file_name")
	threading.Thread(target=download_manager.local_init,args=(start,end,url,file_name,reporting_ip)).start()
	return '...'

@app.route("/fetch_local_data")
def fetch_local_data():
	start = int(request.args.get('start'))
	local_id = request.args.get('local_id')
	ip = request.args.get('ip')	
	url = request.args.get("url")
	file_name = request.args.get("file_name")
	threading.Thread(target=download_manager.fetch_local_data,args=(local_id,ip,start,file_name)).start()
	return '...'

@app.route("/local_transfer")
def local_transfer() :
	local_id = request.args.get('local_id')
	with open(os.path.join(os.getcwd(),local_id),'rb') as f :
		content = f.read()
	os.unlink(os.path.join(os.getcwd(),local_id))
	return content

app.run(host=config['server_ip'], port=config['server_port'],debug=True)	