from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import os
import thread
import urllib2
from operator import itemgetter
import socket
import time
import urllib
import random
import sqlite3
import glob

print '\nDF Downloader v-1.6.0\n'

print 'All rights reserved\n\tDaman Arora\n\tRavi Shankar Pandey\n\tSiddharatha Sahai'

print '\nNote ##\n\tUnauthorized redistribution of this software may lead to legal actions\n'


PORT = 6969

testPort = random.randrange(7000,50000)
testPass = random.randrange(7000,50000)
passBroadRecv = random.randrange(100,10000000)
passBroadSend = random.randrange(100,10000000)

print 'Checking network configuration\n'
time.sleep(1.5)

def homeBroadcast() :
	global testPort
	global testPass
	time.sleep(0.5)
	try :
		s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
		s.sendto(str(testPass),('<broadcast>',testPort))
	except socket.error as error :
		pass

thread.start_new_thread(homeBroadcast,())
gen = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
gen.bind(('',testPort))
gen.settimeout(1)

try :
	data , address = gen.recvfrom(100)
	if data == str(testPass) :
		HOME = address[0]
except socket.error as error :
	HOME = 'localhost'

try :
	HOME = address[0]
except  NameError :
	HOME = 'localhost'

print 'Configuration Completed'

if '192.' in HOME :
	print 'Connected via Wirless Network IP -  ' +  HOME

elif '172.' in HOME :
	print 'Connected via LAN  IP - ' +  HOME

else :
	print 'No access to a network - Please connect to a network and retry\n'  
	time.sleep(2)
	exit()

content = '<script type="text/javascript">window.location = "http://' + HOME + ':' + str(PORT) + '/";</script>'

with open('start.html','w') as f :
        f.write(content)

os.system('start start.html')
isLocalBusy = False

threads =  25 

threadCounter = 0
partList = []
serveList = []


def broadcastListener() :
	global passBroadRecv
	global passBroadSend
	global PORT
	s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	s.bind(('',PORT-10))
	while True :
		data , address = s.recvfrom(500)
		if data == str(passBroadRecv) :
			s.sendto(str(passBroadSend),address)

thread.start_new_thread(broadcastListener,())

class MyHandler(BaseHTTPRequestHandler):
	def do_GET(self):
		global partList
		global threads
		global threadCounter
		global PORT 
		global HOME
		global serveList
		self.send_response(200)

		if self.path == '/' :
			content =  '''
<html>
<head><title>DF Downloader v-1.6.0</title>
</head>
<body>
<h1>Welcome DF Downloader v-1.6.0</h1>
<h2>High speed downloads just a click away !!!!! </h2>
<h2><a href = "http://''' + HOME + ''':''' + str(PORT) + '''/newDownload"> Click here to start new download </a></h2>
<h2><a href = "http://''' + HOME + ''':''' + str(PORT) + '''/viewDownloads"> Click here to view downloads </a></h2>
<h2><a href = "http://''' + HOME + ''':''' + str(PORT) + '''/terms&condition"> Click here to view Terms and Condition </a></h2>
</body>
</html>
'''
			self.send_header('Content-type','text/html')
			self.end_headers()
			self.wfile.write(content)
			return
		if 'terms&condition' in self.path :
			content = '''<br>
<b>1. GRANT OF RIGHTS and RESTRICTIONS ON USE</b><br>
You and  are granted a nonexclusive, nontransferable, limited right to access and use for indivdual purposes the Online Services and Materials made available to you.<br><br>
<b>2. RPRODUCTION and REDISTRIBUTION</b><br>
Any reproduction or redistribution of the software will lead to legal consequences

'''
			self.send_header('Content-type','text/html')
			self.end_headers()
			self.wfile.write(content)
		if 'viewDownloads' in self.path :
			con = sqlite3.connect('logs')
			cur = con.cursor()
			try :
				cur.execute('SELECT * FROM logs')
				logs = cur.fetchall()
			except sqlite3.OperationalError :
				logs = []
			con.close()
			content = """<html><script type="text/javascript">
					setTimeout(function(){
					location = ''
					},200)
					</script><body background="goku.jpg"><centre><table border='2' width="100%" ><tr><th>File Name</th><th>Size
					</th><th> % downloaded </th><th>Clients</th><th>URL</th></tr>"""
			for item in logs :
				content += '<tr height="10%"><th>'
				content += item[0]
				content += '</th><th>'
				content += str(item[1])
				content += '</th><th>'
				content += str(item[2]*100/(threads*item[3]))
				content += ' % </th><th>'
				content += str(item[3])
				content += '</th><th>'
				content += item[4]
				content += '</th></tr>'
			content += "</table><centre></body></html>"
			self.send_header('Content-type','text/html')
			self.end_headers()
			self.wfile.write(content)
			return

		if 'newDownload' in self.path :
			content = """<html>
			<head><title>DOWNLOADER</title>
			</head>
			<body>
			<br>
			<form method="GET" action="http://localhost:""" + str(PORT) + """/downloadmanager">
			<h3>URL : <input type="text" id="letitbeuniqueurl" name="letitbeuniqueurl" size="100">
			<h3>File Name : <input type="text" name="letitbeuniquefilename" size="40">
			<input type="submit"></button>
			</div>
			</body>
			</html>"""
			self.send_header('Content-type','text/html')
			self.end_headers()
			self.wfile.write(content)
			return

		if 'downloadmanager' in self.path :
			url = self.path.split('letitbeuniqueurl=')[1].split('&filename')[0]
			file_name = self.path.split('letitbeuniquefilename=')[1]
			serveList.append([file_name, url, [], [] ])
			thread.start_new_thread(distDown,(url,file_name))
			content = '<script type="text/javascript">window.location = "http://localhost:' + str(PORT) + '/viewDownloads";</script>'
			self.send_header( 'Content-type', 'text/html')
			self.end_headers()
			self.wfile.write(content)

		if 'completedFor' in self.path :			
			path = self.path.split('?')[1].split('&')
			ip = path[1].split('=')[1]
			file_name = path[0].split('=')[1]
			thread.start_new_thread(notifyComplete,(ip,file_name,start))
	
		if 'fetchHandler' in self.path :
			file_name = self.path.split('=')[1]
			with open(os.path.join(os.getcwd(),file_name),'rb') as f :
				content = f.read()
			self.send_header( 'Content-type', 'text/html')
			self.end_headers()
			self.wfile.write(content)
			os.unlink(os.path.join(os.getcwd(),file_name))
			i = 0
			for item in partList :
				if item[4] == file_name :
					break
				i+=1
			partList.remove(partList[i])

		if 'fetchChild' in self.path :
			path = self.path.split('?')[1].split('&')
			ip = path[1].split('=')[1]
			file_name = path[0].split('=')[1]
			client_number = int(path[2].split('=')[1])
			thread.start_new_thread(goFetch,(ip,file_name,client_number))

		if 'countIncrement' in self.path :
			con = sqlite3.connect('logs')
			cur = con.cursor()
			cur.execute('SELECT * FROM logs where file = ?',(self.path.split('===')[1],))
			log = cur.fetchall()[0]
			cur.execute('DELETE FROM logs WHERE file = ?',(log[0],))
			cur.execute('INSERT INTO logs values(?,?,?,?,?)',(log[0],log[1],log[2]+1,log[3],log[4]))
			con.commit()
			con.close()


		if 'startdownload' in self.path :
			path = self.path.split('?')[1].split('&')
			start = int(path[0].split('=')[1])
			end = int(path[1].split('=')[1])
			url = path[2].split('=')[1]
			url = urllib.unquote(url)
			ip = path[3].split('=')[1]
			file_name = path[4].split('=')[1]
			self_ip = path[5].split('=')[1]
			client_number = path[6].split('=')[1]
			partList.append([start, end, url, ip, file_name, HOME , 0 , [] , client_number])
			workSplit = []
			frag = (end - start)/threads
			for i in range(threads) :
				if i== 0 :
					workSplit.append([start,start+frag])
				else :
					workSplit.append([start+1,start+frag])
				start += frag

			workSplit[-1][1] = end
			for i in range(threads) :
				thread.start_new_thread(downloader,(i,workSplit[i][0],workSplit[i][1],url,ip,file_name,client_number))

			self.send_header("Content-type", "text/html")
			self.end_headers()

	def log_request(self, code=None, size=None):
		pass

	def log_message(self, format, *args):
		pass


def distDown(url,file_name) :
	global HOME
	global clientList
	global PORT
	global partList
	global passBroadRecv
	global passBroadSend
	url = urllib2.unquote(url)
	req = urllib2.urlopen(url)
	meta = req.info()
	size = int(meta["Content-Length"])
	activeClients = []
	s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
	Pass = random.randrange(1,500)
	s.sendto(str(passBroadRecv),('<broadcast>',PORT-10))
	start = time.time()
	s.settimeout(0.3)
	temp = []
	while True :
		if time.time() - start > 1.5 :
			break
		try :
			temp.append(s.recvfrom(500))
		except socket.error as error  :
			pass
	for item in temp :
		if item[0] == str(passBroadSend) :
			activeClients.append(item[1][0])
	for item in serveList :
		if item[0] == file_name :
			item[2] = activeClients
	workSplit = []
	frag = size/len(activeClients)
	start = 0
	con = sqlite3.connect('logs')
	cur = con.cursor()
	try :
		cur.execute('CREATE TABLE logs ( file varchar(100) , size int , count int,clients int, url varchar(500))')
	except sqlite3.OperationalError :
		pass
	cur.execute('SELECT * FROM logs WHERE file = ?',(file_name,))
	logs = cur.fetchall()
	if len(logs)==0 :
		cur.execute('INSERT INTO logs values(?,?,?,?,?)',(file_name,size,0,len(activeClients),url))
		con.commit()
		con.close()
		for i in range(threads) :
			if i== 0 :
				workSplit.append([start,start+frag])
			else :
				workSplit.append([start+1,start+frag])
			start += frag
		workSplit[-1][1] = size
		print '\n##############   DOWNLOAD DISTRIBUTION     ###################\n'
		for i in range(len(activeClients)) :
			urllib2.urlopen("http://" + activeClients[i] + ":" + str(PORT) + "/startdownload?start=" + str(workSplit[i][0]) + "&end=" + str(workSplit[i][1]) + "&url=" + urllib.quote(url) +  "&ip=" + HOME + "&file_nmae=" + file_name + "&self_ip=" + activeClients[i] + '&client_num=' +str(i) )
			print str(i+1) + '.  ' + ' CLIENT - ' +  str(activeClients[i])  + ' START - ' + str(workSplit[i][0]) + ' END - ' + str(workSplit[0][1])
		print '\n###################			 ###################   \n'
	else :
		print '\n########################## File already exists -- ' + logs[0][0] + ' #####\n'


def downloader(thread_num,START,END,url,IP,file_name,client_number) :
	global PORT
	global serveList
	global HOME
	content = ''
	req = urllib2.Request(url)
	req.headers["Range"]='bytes='+str(START)+'-'+str(END)
	f = urllib2.urlopen(req)
	while True :
		data = f.read()
		if not data :
			break
		content += data

	if thread_num < 10 :
		with open(file_name + '0' + str(thread_num) + '.temp' , 'wb') as f :
			f.write(content)
	else :
		with open(file_name + str(thread_num) + '.temp' , 'wb') as f :
			f.write(content)

	new_url = 'http://' + IP + ':' + str(PORT) + '/countIncrement?file===' + file_name
	urllib2.urlopen(new_url)
	for item in partList :
		if item[4] == file_name :
			item[6] += 1
			print str(item[6]*100/threads) + '% done for ' + item[4] + '  ...........'
			if item[6] == threads :
				thread.start_new_thread(combineFiles,(file_name,item[3],client_number))
				item[6] = 0


def goFetch(IP,file_name,client_number) :
	global PORT 
	global serveList
	url = 'http://' + IP + ':' + str(PORT) + '/fetchHandler?file_name=' + file_name
	f = urllib2.urlopen(url)
	content = ''
	while True :
		data = f.read()
		if not data :
			break
		content += data

	if client_number < 10 :
		file_ = file_name + '000' + str(client_number)
	elif client_number < 100 and client_number >= 10 :
		file_ = file_name + '00' + str(client_number)
	elif client_number < 1000 and client_number >= 100 :
		file_ = file_name + '0' + str(client_number)

	with open(file_ + '.temp','wb') as f :
		f.write(content)
	print '\nFetching url ' , url , '\n'
	for item in serveList :
		if item[0] == file_name :
			item[3].append([str(client_number)])
			if len(item[2]) == len(item[3]):
				thread.start_new_thread(assembler,(file_name,))


def assembler(file_name) :
	print "\nEnterered global assembler \n"
	global serveList
	i = 0
	for item in serveList :
		if item[0] == file_name :
			dataList = item[3]
			break
		i+=1

	content = ''
	files = glob.glob('*.temp')
	files_ = []
	for item in files :
		if file_name in item :
			files_.append(item)
	files_.sort()
	with open (file_name,'wb') as output :
		for inner in files_ :
			print inner
			with open(inner,'rb') as f :
				 for line in f :
					output.write(line)
	for item in files_ :
		os.unlink(item)
	print "\nAssembled Successfully \n"
	serveList.remove(serveList[i])

def combineFiles(file_name,IP,client_number) :
	print "\nEnterered local assembler \n"
	global threads
	global partList
	global HOME
	global PORT
	files = glob.glob('*.temp')
	files_ = []
	for item in files :
		if file_name in item :
			files_.append(item)
	files_.sort()
	with open (file_name,'wb') as output :
		for inner in files_ :
			print inner
			with open(inner,'rb') as f :
				 for line in f :
					output.write(line)
	for item in files_ :
		os.unlink(item)
	for item in partList :
		if item[4] == file_name :
			start = item[0]
	url = "http://" + IP + ":" + str(PORT) + "/fetchChild?file=" + file_name + "&ip=" + HOME + "&client=" + str(client_number)
	urllib2.urlopen(url)


server = HTTPServer(('',PORT), MyHandler)
server.serve_forever()

