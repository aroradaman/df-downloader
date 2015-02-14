from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import os
import thread
import urllib2
from operator import itemgetter
import socket
import time
import urllib
import random

print '\nDF Downloader v-1.6.0\n'

PORT = 6969

testPort = random.randrange(7000,50000)
testPass = random.randrange(7000,50000)


print 'Checking Network Configuration  ..  ..   .   \n'

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

print 'Configuration Completed\n'

if '192.' in HOME :
	print 'Connected via Wirless Network IP -  ' +  HOME

elif '172.' in HOME :
	print 'Connected via LAN  IP - ' +  HOME

else :
	print 'No access to a network - Please connect to a network and retry\n'  
	time.sleep(2)
	exit()
isLocalBusy = False

threads =  25 

threadCounter = 0
partList = []
serveList = []


def broadcastListener() :
	global PORT
	s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	s.bind(('',PORT+1))
	while True :
		data , address = s.recvfrom(500)
		if data == '!@#$%^' :
			s.sendto('!@#$%^',address)

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

		if 'index' in self.path :
			content = """<html>
			<head><title>DOWNLOADER</title>
			</head>
			<body>
			<!--
			<script src="http://cdnjs.cloudflare.com/ajax/libs/jquery/2.1.1/jquery.min.js" type="text/javascript">
			</script>;
			-->
			<h3>Enter url/filename here</h3><br>
			<form method="GET" action="http://""" + HOME + """:6969/downloadmanager">
			<input type="text" name="url"><br>
			<input type="text" name="filename">
			<input type="submit">Submit</button>
			</body>
			</html>"""
			self.send_header('Content-type','text/html')
			self.end_headers()
			self.wfile.write(content)
			return

		if 'downloadmanager' in self.path :
			x = self.path.split('?')[1].split('&')
			url = self.path.split('url=')[1].split('&filename')[0]
			file_name = self.path.split('filename=')[1]			
			serveList.append([file_name, url, [], [] ])
			thread.start_new_thread(distDown,(url,file_name))

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
			start = path[2].split('=')[1]
			thread.start_new_thread(goFetch,(ip,file_name,start))http://192.168.1.19:6969/

		if 'startdownload' in self.path :
			path = self.path.split('?')[1].split('&')
			start = int(path[0].split('=')[1])
			end = int(path[1].split('=')[1])
			url = path[2].split('=')[1]
			url = urllib.unquote(url)
			#url.replace('!P@R#O$B%L!E@M#','&')
			ip = path[3].split('=')[1]
			file_name = path[4].split('=')[1]
			self_ip = path[5].split('=')[1]
			partList.append([start, end, url, ip, file_name, HOME , 0 , []])
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
				thread.start_new_thread(downloader,(i,workSplit[i][0],workSplit[i][1],url,ip,file_name))

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
	url = urllib2.unquote(url)
	req = urllib2.urlopen(url)
	meta = req.info()
	size = int(meta["Content-Length"])
	activeClients = []
	s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
	s.sendto('!@#$%^',('<broadcast>',PORT+1))
	start = time.time()
	s.settimeout(0.2)
	temp = []
	while True :
		if time.time() - start > 1.5 :
			break
		try :
			temp.append(s.recvfrom(500))
		except socket.error as error  :
			pass
	for item in temp :
		if item[0] == '!@#$%^' :
			activeClients.append(item[1][0])
	for item in serveList :
		if item[0] == file_name :
			item[2] = activeClients
	workSplit = []
	frag = size/len(activeClients)
	start = 0
	for i in range(threads) :
		if i== 0 :
			workSplit.append([start,start+frag])
		else :
			workSplit.append([start+1,start+frag])
		start += frag
	workSplit[-1][1] = size
	print '\n##########    DOWNLOAD DISTRIBUTION   #################\n'
	for i in range(len(activeClients)) :
		urllib2.urlopen("http://" + activeClients[i] + ":" + str(PORT) + "/startdownload?start=" + str(workSplit[i][0]) + "&end=" + str(workSplit[i][1]) + "&url=" + urllib.quote(url) + "&ip=" + HOME + "&file_nmae=" + file_name + "&self_ip=" + activeClients[i])
		print str(i+1) + '.  ' + ' CLIENT - ' +  str(activeClients[i])  + '    START - ' + str(workSplit[i][0]) + ' END - ' + str(workSplit[i][1])
	print '\n###################				 ###################   \n'

def downloader(thread_num,START,END,url,IP,file_name) :
	global isLocalBusy
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
	while True :
		if not isLocalBusy :
			isLocalBusy = True
			break
		else :
			print '.............busy.......'
			continue
	for item in partList :
		if file_name == item[4] :
			item[7].append([START,content])
	isLocalBusy = False
	for item in partList :
		if item[4] == file_name :
			item[6] += 1
			print str(item[6]*100/threads) + '% done for ' + item[4] + '  ...........'
			if item[6] == threads :
				thread.start_new_thread(combineFiles,(file_name,item[3]))
				item[6] = 0


def goFetch(IP,file_name,start) :
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
	print '\nFetching url ' , url , '\n'
	for item in serveList :
		if item[0] == file_name :
			item[3].append([int(start),content])
	for item in serveList :
		if item[0] == file_name :			
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
	dataList = sorted(dataList,key=itemgetter(0))
	for item  in dataList :
		content += item[1]

	with open('COMPLETED__'+file_name,'wb') as f :
		f.write(content)
	print "\nAssembled Successfully \n"
	serveList.remove(serveList[i])

def combineFiles(file_name,IP) :
	print "\nEnterered local assembler \n"
	global threads
	global partList
	global HOME
	global PORT
	for item in partList :
		if item[4] == file_name :
			dataList = item[7]
	dataList = sorted(dataList,key=itemgetter(0))
	content = ''
	for item in dataList :
		content += item[1]http://192.168.1.19:6969/
	with open(file_name,'wb') as f :
		f.write(content)
	print '\nCompleted For' , IP , '\n'
	for item in partList :
		if item[4] == file_name :
			start = item[0]
	url = "http://" + IP + ":" + str(PORT) + "/fetchChild?file=" + file_name + "&ip=" + HOME + "&start=" + str(start)
	urllib2.urlopen(url)


server = HTTPServer(('',PORT), MyHandler)
server.serve_forever()

