from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import os
import thread
import urllib2
from operator import itemgetter
import socket
import time
import urllib
import random
import hashlib
import threading
import json
import bson
import time

def md5(string) :
	m = hashlib.md5()
	m.update(str(string))
	return m.hexdigest()

def distDown(url,file_name) :
	global HOME
	global clientList
	global PORT
	global localList	
	url = urllib2.unquote(url)
	req = urllib2.urlopen(url)
	meta = req.info()
	size = int(meta["Content-Length"])
	activeClients = []
	temp = []
	s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	#s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
	for ip in FRIENDS :
		s.sendto('!@#$%^',(ip,PORT+1))
		s.settimeout(0.2)
		try :
			temp.append(s.recvfrom(500))
		except socket.error as error :
			print error
			pass
	for item in temp :
		if item[0] == '!@#$%^' :
			activeClients.append(item[1][0])
	globallist[md5(file_name)]['activeClients'] = activeClients
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
	print '\n###################				 ###################\n'

def downloader(thread_num,START,END,url,IP,file_name) :
	global isLocalBusy
	global PORT
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
	locallist[md5(file_name)]['completed'] += 1
	locallist[md5(file_name)][str(thread_num)]['data'] = [START,content]
	print str(locallist[md5(file_name)]['completed']*100/threads) + '% done for ' + file_name + '  ...........'
	isLocalBusy = False
	if locallist[md5(file_name)]['completed'] == threads :
		threading.Thread(target=combineFiles,args=(file_name,locallist[md5(file_name)]['ip'])).start()
	

def goFetch(IP,file_name,start) :
	global PORT 
	url = 'http://' + IP + ':' + str(PORT) + '/fetchHandler?file_name=' + file_name
	f = urllib2.urlopen(url)
	content = ''
	while True :
		data = f.read()
		if not data :
			break
		content += data
	print '\nFetching url ' , url , '\n'
	globallist[md5(file_name)]['data'].append([int(start),content])
	if len(globallist[md5(file_name)]['data']) == len(globallist[md5(file_name)]['activeClients']):
		threading.Thread(target=assembler,args=(file_name,)).start()


def assembler(file_name) :
	print "\nEnterered global assembler \n"
	global globallist
	content = ''
	dataList = sorted(globallist[md5(file_name)]['data'],key=itemgetter(0))
	for item  in dataList :
		content += item[1] 	
	with open('COMPLETED___'+file_name,'wb') as f :
		f.write(content)
	print "\nAssembled Successfully \n"
	globallist.pop(md5(file_name),None)

def combineFiles(file_name,IP) :
	print "\nEnterered local assembler \n"
	global threads
	global localList
	global HOME
	global PORT
	dataList = [ locallist[md5(file_name)][str(i)]['data'] for i in range(threads) ]
	dataList = sorted(dataList,key=itemgetter(0))
	content = ''
	for item in dataList :
		content += item[1]#http://192.168.1.19:6969/
	with open(file_name,'wb') as f :
		f.write(content)
	print '\nCompleted For' , IP , '\n'
	start = locallist[md5(file_name)]['0']['start']
	url = "http://" + IP + ":" + str(PORT) + "/fetchChild?file=" + file_name + "&ip=" + HOME + "&start=" + str(start)
	counter = 2
	while True :
		try :
			urllib2.urlopen(url)
			break
		except Exception as error :
			print error
			print 'Going to sleep for ' + str(counter) + ' seconds' 
			time.sleep(counter)
			counter += 3
	locallist.pop(md5(file_name),None)

def sync() :
	while True :
		global locallist
		global globallist	
		time.sleep(1.5)
		content = bson.dumps(locallist)
		with open('LocLog','w') as f :
			f.write(content)
		content = bson.dumps(globallist)
		with open('GlobLog','w') as f :
			f.write(content)


def statusListener() :
	global PORT
	s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	s.bind(('',PORT+1))
	while True :
		data , address = s.recvfrom(500)
		if data == '!@#$%^' :
			s.sendto('!@#$%^',address)

print '\nDF Downloader v-1.6.0\n'

PORT = 6969

with open('homeIP','r') as  f :
	HOME = f.read().strip()

print HOME


with open('friends','r') as  f :
	FRIENDS = f.read().strip().split(',')

FRIENDS.append(HOME)
FRIENDS = list(set(FRIENDS))

isLocalBusy = False

threads =  25

threadCounter = 0
localList = []

try :
	with open('LocLog','r') as f :
		content = f.read()
	locallist = bson.loads(content)
	for key in locallist.keys() :
		for i in range(threads) :
			if locallist[key][str(i)]['data'] == [] :
				threading.Thread(target=downloader,args=(i,locallist[key][str(i)]['start'],locallist[key][str(i)]['end'],locallist[key]['url'],locallist[key]['ip'],locallist[key]['file'])).start()
except Exception as error :
	print error
	locallist = {}

try :
	with open('GlobLog','r') as f :
		globallist = bson.loads(f.read())
except :
	globallist = {}


threading.Thread(target=sync,args=()).start()


threading.Thread(target=statusListener,args=()).start()

class MyHandler(BaseHTTPRequestHandler):
	def do_GET(self):
		global localList
		global threads
		global threadCounter
		global PORT 
		global HOME
		global globalList
		self.send_response(200)

		if 'home' in self.path :
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
			globallist.update({md5(file_name):{'url':url,'activeClients':[],'data':[]}})
			threading.Thread(target=distDown,args=(url,file_name)).start()

		if 'completedFor' in self.path :
			path = self.path.split('?')[1].split('&')
			ip = path[1].split('=')[1]
			file_name = path[0].split('=')[1]
			threading.Thread(target=notifyComplete,args=(ip,file_name,start)).start()

		if 'fetchHandler' in self.path :
			file_name = self.path.split('=')[1]
			with open(os.path.join(os.getcwd(),file_name),'rb') as f :
				content = f.read()
			self.send_header( 'Content-type', 'text/html')
			self.end_headers()
			self.wfile.write(content)
			os.unlink(os.path.join(os.getcwd(),file_name))
			i = 0
			for item in localList :
				if item[4] == file_name :
					break
				i+=1

		if 'fetchChild' in self.path :
			path = self.path.split('?')[1].split('&')
			ip = path[1].split('=')[1]
			file_name = path[0].split('=')[1]
			start = path[2].split('=')[1]
			thread.start_new_thread(goFetch,(ip,file_name,start))#http://192.168.1.19:6969/

		if 'startdownload' in self.path :
			path = self.path.split('?')[1].split('&')
			start = int(path[0].split('=')[1])
			end = int(path[1].split('=')[1])
			url = path[2].split('=')[1]
			url = urllib.unquote(url)
			#url.replace('!P@R#O$B%L!E@M#','&')
			ip = path[3].split('=')[1]
			file_name = path[4].split('=')[1]
			#os.mkdir(md5(file_name))
			self_ip = path[5].split('=')[1]
			locallist.update({md5(file_name):{}})
			#localList.append([start, end, url, ip, file_name, HOME , 0 , []])
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
				locallist[md5(file_name)].update({'completed':0,'url':url,'ip':ip,'file':file_name,'HOME':HOME,str(i):{'start':workSplit[i][0],'end':workSplit[i][1],'data':[]}})
				threading.Thread(target=downloader,args=(i,workSplit[i][0],workSplit[i][1],url,ip,file_name)).start()

			self.send_header("Content-type", "text/html")
			self.end_headers()
		

	def log_request(self, code=None, size=None):
		pass

	def log_message(self, format, *args):
		pass


server = HTTPServer(('',PORT), MyHandler)
server.serve_forever()

