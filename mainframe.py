import json
import os
import threading
import socket
import hashlib
import urllib2
import urllib
from operator import itemgetter
import os
import random
import time
import psutil
from Queue import Queue

class downloader :
	def __init__(self) :
		self.config = self.get_config()
		self.global_dict = {}
		self.local_dict = {}
		self.job_configs = {}
		self.master_job_configs = {}
		self.synced_job_configs = {}
		self.udp_sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		self.pool = ThreadPool(self.config['threads'])
		threading.Thread(target=self.logger,args=()).start()

	def get_config(self) :
		with open('config.json','r') as f :
			return json.loads(f.read())

	# def render_html_log(self) :
	# 	jobs = self.synced_job_configs.keys()
	# 		if len(jobs) > 0 :
	# 			print
	# 			print '##' * 50
	# 			print '\nAvailable Jobs :\n'
	# 			#print json.dumps(self.synced_job_configs,indent=4)
	# 			for job in jobs :
	# 				print '\t',job
	# 				print '\t\tInititaor\t',self.synced_job_configs[job]['reporting_ip']
	# 				print '\t\tFile Name\t',self.synced_job_configs[job]['file_name']
	# 				print '\t\tDownload Url\t',self.synced_job_configs[job]['url']					
	# 				print '\t\tPeers'
	# 				print '\t\t\tPeer\t\tPercent Done\tCPU Usage' 
	# 				tot_threas_completed = 0					
	# 				for peer in self.synced_job_configs[job]['peers'].keys() :
	# 					string = '\t\t\t'
	# 					string += peer
	# 					string += '\t'
	# 					string += str( float(self.synced_job_configs[job]['peers'][peer]['threads_completed']*100) / self.config['threads'] ) + '%'
	# 					string += '\t\t'
	# 					string += str(self.synced_job_configs[job]['peers'][peer]['cpu_percent'])
	# 					print string
	# 					tot_threas_completed += self.synced_job_configs[job]['peers'][peer]['threads_completed']
	# 				print '\t\tTotal Percentage Done\t'+ str( float(tot_threas_completed*100) / (len(self.synced_job_configs[job]['peers'].keys())*self.config['threads']) )
	# 			print
	# 			print '##' * 50
	# 			print

	def logger(self) :
		while True :
			time.sleep(self.config['log_interval'])
			jobs = self.synced_job_configs.keys()
			if len(jobs) > 0 :
				print
				print '##' * 50
				print '\nAvailable Jobs :\n'
				#print json.dumps(self.synced_job_configs,indent=4)
				for job in jobs :
					print '\t',job
					print '\t\tInititaor\t',self.synced_job_configs[job]['reporting_ip']
					print '\t\tFile Name\t',self.synced_job_configs[job]['file_name']
					print '\t\tDownload Url\t',self.synced_job_configs[job]['url']					
					print '\t\tPeers'
					print '\t\t\tPeer\t\tPercent Done\tCPU Usage' 
					tot_threas_completed = 0					
					for peer in self.synced_job_configs[job]['peers'].keys() :
						string = '\t\t\t'
						string += peer
						string += '\t'
						string += str( float(self.synced_job_configs[job]['peers'][peer]['threads_completed']*100) / self.config['threads'] ) + '%'
						string += '\t\t'
						string += str(self.synced_job_configs[job]['peers'][peer]['cpu_percent'])
						print string
						tot_threas_completed += self.synced_job_configs[job]['peers'][peer]['threads_completed']
					print '\t\tTotal Percentage Done\t'+ str( float(tot_threas_completed*100) / (len(self.synced_job_configs[job]['peers'].keys())*self.config['threads']) )
				print
				print '##' * 50
				print

	def start_background_sync_client(self) :
		threading.Thread(target=self.sync_proc__1,args=()).start()
		#threading.Thread(target=self.sync_proc__2,args=()).start()
		#threading.Thread(target=self.sync_proc__3,args=()).start()

	def sync_proc__1(self)  :
		while True :
			sync__interval = float(random.randrange(10*self.config['sync_interval_min'],10*self.config['sync_interval_max']))/10
			time.sleep(sync__interval)
			jobs_at_master = self.job_configs.keys()
			for job in jobs_at_master :
				for peer in self.global_dict[job]['active_peers'] :
					self.udp_sock.sendto('sync_proc__1' + job ,(peer,self.config['sync_server_port']))

	def sync_proc__2(self)  :
		while True :
			sync__interval = random.randrange(3,6)
			time.sleep(sync__interval)
			##proc
	
	def sync_proc__3(self)  :
		while True :
			sync__interval = random.randrange(3,6)
			time.sleep(sync__interval)
			##proc

	def sync_server(self) :
		sync_server = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		sync_server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
		sync_server.bind((self.config['server_ip'],self.config['sync_server_port']))

		while True :
			data, address = sync_server.recvfrom(1500)
			host = address[0]
			tag = data[:12]
			msg = data[12:]

			if tag == '_if__online_' :
				sync_server.sendto(self.md5(tag),address)

			elif tag == 'sync_proc__1' :	
				frag = self.job_configs[msg]
				frag.update({'sync_time':time.time()})	
				frag.update({'cpu_percent':psutil.cpu_percent()})
				for key,value in self.local_dict.iteritems() :
					if value['file'] == self.job_configs[msg]['file_name'] :
						frag.update({'threads_completed':value['threads_completed']})				
				self.udp_sock.sendto('rply_proc__1' + json.dumps(frag),(host,self.config['sync_server_port']))

			elif tag == 'sync_proc__2' : 
				pass

			elif tag == 'sync_proc__3' :
				pass

			elif tag == 'rply_proc__1' :
				frag = json.loads(msg)
				#print json.dumps(frag,indent=4)
				job = self.md5(frag['file_name'])
				if job not in self.synced_job_configs :
					self.synced_job_configs.update({job:{'peers':{}}})
				self.synced_job_configs[job].update({'reporting_ip':frag['reporting_ip']})
				self.synced_job_configs[job].update({'url':frag['url']})
				self.synced_job_configs[job].update({'file_name':frag['file_name']})
				if host not in self.synced_job_configs[job] :
					self.synced_job_configs[job]['peers'].update({host: {
															'threads_completed' : 	frag['threads_completed'],
															'cpu_percent' 		: 	frag['cpu_percent'],
															'start' 			: 	frag['start'],
															'end' 				: 	frag['end'],
															'size' 				:	frag['end'] - frag['start']
						}
					})
				#print json.dumps(self.synced_job_configs,indent=4)

			elif tag == 'rply_proc__2' :
				pass

			else :
				pass

	def md5(self,anything) :
		m = hashlib.md5()
		m.update(str(anything))
		return m.hexdigest()

	def start_background_sync_server(self) :
		threading.Thread(target=self.sync_server,args=()).start()

	def check_if_online(self) :
		check_client = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		temp = []
		active_peers = []
		for peer in self.config['friends'] :
			check_client.sendto('_if__online_',(peer,self.config['sync_server_port']))
			check_client.settimeout(0.2)
			try :
				temp.append(check_client.recvfrom(500))
			except socket.error as error :
				pass
		for item in temp :
			if item[0] == self.md5('_if__online_') :
				active_peers.append(item[1][0])
		return active_peers

	def distributor(self,url,filename) :
		url = urllib2.unquote(url)
		req = urllib2.urlopen(url)
		meta = req.info()
		try :
			size = int(meta["Content-Length"])
		except KeyError :
			pass
		active_peers = self.check_if_online()
		self.global_dict.update({ self.md5(filename): {'range':[],'data':[],'active_peers':active_peers}})	
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
		#print '\n##########    DOWNLOAD DISTRIBUTION   #################\n'
		for i in range(len(active_peers)) :
			self.global_dict[self.md5(filename)]['range'].append([int(workSplit[i][0]),int(workSplit[i][1])])
			req_url = "http://" + active_peers[i] + ":" + str(self.config['server_port']) + "/local_init?start=" + str(workSplit[i][0]) + "&end=" + str(workSplit[i][1]) + "&url=" + urllib.quote(url) + "&reporting_ip=" + self.config['home_ip'] + "&file_name=" + filename + "&self_ip=" + active_peers[i]
			urllib2.urlopen(req_url)
			#print str(i+1) + '.  ' + ' CLIENT - ' +  str(active_peers[i])  + '    START - ' + str(workSplit[i][0]) + '		END - ' + str(workSplit[i][1])
		#print '\n###################				 ###################\n'

	def local_init(self,start,end,url,file_name,reporting_ip) :
		local_id = self.md5(file_name+str(start))
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
		for i in range(self.config['threads']) :
			self.local_dict[local_id].update({'threads_completed':0,'url':url,'reporting_ip':reporting_ip,'file':file_name,'home_ip':self.config['home_ip'],str(i):{'start':work_split[i][0],'end':work_split[i][1],'data':[]}})
			self.pool.add_task(self.basic_downloader,i,work_split[i][0],work_split[i][1],url,reporting_ip,file_name,local_id)
			#threading.Thread(target=self.basic_downloader,args=(i,work_split[i][0],work_split[i][1],url,reporting_ip,file_name,local_id)).start()		
		self.job_configs.update({ self.md5(file_name) : 
														{   
															'reporting_ip'	: 	reporting_ip,
															'start'		: 	start,
															'end'		:	end,
															'file_name' :	file_name,
															'url'		:	url
														} 
													})
		
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
		#print str(self.local_dict[local_id]['threads_completed']*100/self.config['threads']) + '% done for ' + file_name + '  for job ' + local_id
		if self.local_dict[local_id]['threads_completed'] == self.config['threads'] :
			threading.Thread(target=self.local_data_assembly,args=(file_name,self.local_dict[local_id]['reporting_ip'],local_id)).start()
	
	def local_data_assembly(self,file_name,reporting_ip,local_id) :
		#print "\nEnterered local assembler \n"
		content = ''.join( item[1] for item in sorted([ self.local_dict[local_id][str(i)]['data'] for i in range(self.config['threads']) ],key=itemgetter(0)))
		with open(local_id,'wb') as f :
			f.write(content)
		#print '\nCompleted For' , local_id , '\n'
		start = self.local_dict[local_id]['0']['start']
		url = "http://" + reporting_ip + ":" + str(self.config['server_port']) + "/fetch_local_data?local_id=" + local_id + "&ip=" + self.config['home_ip'] + "&start=" + str(start) + "&file_name=" + file_name
		req = urllib2.urlopen(url)		

	def fetch_local_data(self,local_id,ip,start,file_name) :
		url = 'http://' + ip + ':' + str(self.config['server_port']) + '/local_transfer?local_id=' + local_id
		f = urllib2.urlopen(url)
		content = ''
		while True :
			data = f.read()
			if not data :
				break
			content += data
		#print '\nFetching url ' , url , '\n'		
		self.global_dict[self.md5(file_name)]['data'].append([int(start),content])
		if len(self.global_dict[self.md5(file_name)]['data']) == len(self.global_dict[self.md5(file_name)]['active_peers']):
			threading.Thread(target=self.global_data_assembley,args=(file_name,)).start()

	def global_data_assembley(self,file_name) :
		#print "\nEnterered global assembler \n"
		content = ''.join( item[1] for item in sorted(self.global_dict[self.md5(file_name)]['data'],key=itemgetter(0)))
		with open(file_name,'wb') as f :
			f.write(content)
		#print "\nAssembled Successfully " + file_name + "\n"
		#self.global_dict.pop(self.md5(file_name),None)

class Worker(threading.Thread) :
	def __init__(self, tasks) :
		threading.Thread.__init__(self)
		self.tasks = tasks
		self.daemon = True
		self.start()

	def run(self) :
		while True :
			func, args, kargs = self.tasks.get()
			try:
				func(*args, **kargs)
			except Exception, e:
				print e
			finally:
				self.tasks.task_done()

class ThreadPool :
	def __init__(self, num_threads) :
		self.tasks = Queue(num_threads)
		for _ in range(num_threads) :
			Worker(self.tasks)

	def add_task(self, func, *args, **kargs) :		
		self.tasks.put((func, args, kargs))

	def wait_completion(self) :
		self.tasks.join()
