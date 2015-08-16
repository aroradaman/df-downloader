import json
import os
import threading
import socket
import hashlib
import urllib2
import urllib
from operator import itemgetter
import os

def md5(string) :
	m = hashlib.md5()
	m.update(str(string))
	return m.hexdigest()

def echo_zone(ip,port) :
	port += 1
	echo_server = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	echo_server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
	echo_server.bind((ip,port))
	while True :
		data, address = echo_server.recvfrom(1500)
		echo_server.sendto(md5(data),address)

def open_echo_zone(ip,port) :
	threading.Thread(target=echo_zone,args=(ip,port)).start()

def check_if_online(peer_lists,port) :
	check_client = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	temp = []
	active_peers = []
	for peer in peer_lists :
		check_client.sendto('!@#$%^',(peer,port+1))
		check_client.settimeout(0.2)
		try :
			temp.append(check_client.recvfrom(500))
		except socket.error as error :
			pass
	for item in temp :
		if item[0] == md5('!@#$%^') :
			active_peers.append(item[1][0])
	return active_peers

def get_config() :
	with open('config.json','r') as f :
		return json.loads(f.read())

class downloader() :
	def __init__(self,config) :
		self.config = config
		self.global_dict = {}
		self.local_dict = {}

	def distributor(self,url,filename) :
		url = urllib2.unquote(url)
		req = urllib2.urlopen(url)
		meta = req.info()
		try :
			size = int(meta["Content-Length"])
		except KeyError :
			pass
		active_peers = check_if_online(self.config['friends'],self.config['server_port'])
		self.global_dict.update({ md5(filename): {'range':[],'data':[],'active_peers':active_peers}})	
		workSplit = []
		frag = size/len(active_peers)	
		start = 0
		for i in range(self.config['threads']) :	
			if i== 0 :
				workSplit.append([start,start+frag])
			else :
				workSplit.append([start+1,start+frag])
			start += frag
		workSplit[-1][1] = size	
		print '\n##########    DOWNLOAD DISTRIBUTION   #################\n'
		for i in range(len(active_peers)) :
			self.global_dict[md5(filename)]['range'].append([int(workSplit[i][0]),int(workSplit[i][1])])
			req_url = "http://" + active_peers[i] + ":" + str(self.config['server_port']) + "/local_init?start=" + str(workSplit[i][0]) + "&end=" + str(workSplit[i][1]) + "&url=" + urllib.quote(url) + "&reporting_ip=" + self.config['home_ip'] + "&file_name=" + filename + "&self_ip=" + active_peers[i]
			urllib2.urlopen(req_url)
			print str(i+1) + '.  ' + ' CLIENT - ' +  str(active_peers[i])  + '    START - ' + str(workSplit[i][0]) + ' END - ' + str(workSplit[i][1])
		print '\n###################				 ###################\n'

	def local_init(self,start,end,url,file_name,reporting_ip) :
		local_id = md5(file_name+str(start))
		self.local_dict.update({local_id:{}})
		work_split = []
		frag = (end - start)/self.config['threads']
		for i in range(self.config['threads']) :
			if i== 0 :
				work_split.append([start,start+frag])
			else :
				work_split.append([start+1,start+frag])
			start += frag
		work_split[-1][1] = end
		print work_split
		for i in range(self.config['threads']) :
			self.local_dict[local_id].update({'threads_completed':0,'url':url,'reporting_ip':reporting_ip,'file':file_name,'home_ip':self.config['home_ip'],str(i):{'start':work_split[i][0],'end':work_split[i][1],'data':[]}})
			threading.Thread(target=self.basic_downloader,args=(i,work_split[i][0],work_split[i][1],url,reporting_ip,file_name,local_id)).start()

	def basic_downloader(self,thread_num,START,END,url,IP,file_name,local_id) :
		content = ''
		req = urllib2.Request(url)
		req.headers["Range"]='bytes='+str(START)+'-'+str(END)
		f = urllib2.urlopen(req)
		while True :
			data = f.read()			
			if not data :
				break
			content += data
		self.local_dict[local_id]['threads_completed'] += 1
		self.local_dict[local_id][str(thread_num)]['data'] = [START,content]
		print str(self.local_dict[local_id]['threads_completed']*100/self.config['threads']) + '% done for ' + file_name + '  for job ' + local_id
		if self.local_dict[local_id]['threads_completed'] == self.config['threads'] :
			threading.Thread(target=self.local_data_assembly,args=(file_name,self.local_dict[local_id]['reporting_ip'],local_id)).start()
	
	def local_data_assembly(self,file_name,reporting_ip,local_id) :
		print "\nEnterered local assembler \n"
		content = ''.join( item[1] for item in sorted([ self.local_dict[local_id][str(i)]['data'] for i in range(self.config['threads']) ],key=itemgetter(0)))
		with open(local_id,'wb') as f :
			f.write(content)
		print '\nCompleted For' , local_id , '\n'
		start = self.local_dict[local_id]['0']['start']
		url = "http://" + reporting_ip + ":" + str(self.config['server_port']) + "/fetch_local_data?local_id=" + local_id + "&ip=" + self.config['home_ip'] + "&start=" + str(start) + "&file_name=" + file_name
		req = urllib2.urlopen(url)		
		self.local_dict.pop(local_id,None)		

	def fetch_local_data(self,local_id,ip,start,file_name) :
		url = 'http://' + ip + ':' + str(self.config['server_port']) + '/local_transfer?local_id=' + local_id
		f = urllib2.urlopen(url)
		content = ''
		while True :
			data = f.read()
			if not data :
				break
			content += data
		print '\nFetching url ' , url , '\n'
		print file_name
		self.global_dict[md5(file_name)]['data'].append([int(start),content])
		if len(self.global_dict[md5(file_name)]['data']) == len(self.global_dict[md5(file_name)]['active_peers']):
			threading.Thread(target=self.global_data_assembley,args=(file_name,)).start()

	def global_data_assembley(self,file_name) :
		print "\nEnterered global assembler \n"
		content = ''.join( item[1] for item in sorted(self.global_dict[md5(file_name)]['data'],key=itemgetter(0)))
		with open(file_name,'wb') as f :
			f.write(content)
		print "\nAssembled Successfully " + file_name + "\n"
		self.global_dict.pop(md5(file_name),None)
