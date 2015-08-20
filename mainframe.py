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
import requests

class downloader :
	def __init__(self) :
		self.config = self.get_config()
		self.global_dict = {}
		self.lock = threading.Lock()
		self.local_dict = {}
		self.job_configs = {}
		self.master_job_configs = {}
		self.synced_job_configs = {}
		self.udp_sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		self.pool = ThreadPool(self.config['thread_pool_size'])
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
			self.lock.acquire()
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
			self.lock.release()

	def start_background_sync_client(self) :
		threading.Thread(target=self.sync_proc_client,args=()).start()
		
	def sync_proc_client(self)  :
		while True :
			try :
				self.lock.acquire()
				sync__interval = float(random.randrange(10*self.config['sync_interval_min'],10*self.config['sync_interval_max']))/10
				time.sleep(sync__interval)
				jobs_at_master = self.job_configs.keys()
				for job in jobs_at_master :
					for peer in self.global_dict[job]['active_peers'] :
						self.udp_sock.sendto('sync_proc__1' + job ,(peer,self.config['sync_server_port']))
				del_obj = []
				for job in self.global_dict.keys() :				
					if self.global_dict[job]['status'] == 'done' :
						for i in range(len(self.global_dict[job]['active_peers'])) :
							self.udp_sock.sendto('dwnd__done__' + job,(self.global_dict[job]['active_peers'][i],self.config['sync_server_port']))
							del_obj.append(job)
				del_obj = [ self.global_dict.pop(job,None) for job in del_obj ]				
				self.lock.release()
			except Exception as error :
				pass

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

			elif tag == 'dwnd__done__' :
				self.lock.acquire()
				print 'dwnd__done__\n\n'
				del_keys = [ key for key in self.local_dict.keys() 	if self.local_dict[key]['main_job'] == msg ]
				del_keys = [ self.local_dict.pop(key,None) for key in del_keys ]
				if msg in self.synced_job_configs.keys() :
					self.synced_job_configs.pop(msg,None)				
				self.lock.release()


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
		self.global_dict.update({ self.md5(filename): {'url':url,'file_name':filename,'range':[],'data':[],'active_peers':active_peers,'status':'in progress'}})
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
		for i in range(len(active_peers)) :
			self.global_dict[self.md5(filename)]['range'].append([int(workSplit[i][0]),int(workSplit[i][1])])
			req_url = "http://" + active_peers[i] + ":" + str(self.config['server_port']) + "/local_init"			
			post_data = {
				'start'	 		: workSplit[i][0],
				'end' 			: workSplit[i][1],
				'url'	 		: url,
				'reporting_ip'	: self.config['home_ip'],
				'file_name'		: filename
			}
			req = requests.post(req_url,data=post_data)			

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
			self.pool.add_task(self.basic_downloader,i,local_id)	
		self.job_configs.update({ self.md5(file_name) : 
														{   
															'reporting_ip'	: 	reporting_ip,
															'start'		: 	start,
															'end'		:	end,
															'file_name' :	file_name,
															'url'		:	url
														} 
													})
		
	def basic_downloader(self,thread_num,local_id) :
		content = ''		
		req = urllib2.Request(self.local_dict[local_id]['url'])
		req.headers["Range"]='bytes='+str(self.local_dict[local_id][str(thread_num)]['start'])+'-'+str(self.local_dict[local_id][str(thread_num)]['end'])
		f = urllib2.urlopen(req)
		while True :
			data = f.read()
			if not data :
				break
			content += data
		self.local_dict[local_id]['threads_completed'] += 1
		self.local_dict[local_id]['main_job'] = self.md5(self.local_dict[local_id]['file'])
		self.local_dict[local_id][str(thread_num)]['data'] = [self.local_dict[local_id][str(thread_num)]['start'],content]		
		if self.local_dict[local_id]['threads_completed'] == self.config['threads'] :
			threading.Thread(target=self.local_data_assembly,args=(local_id,)).start()
	
	def local_data_assembly(self,local_id) :
		file_name = self.local_dict[local_id]['file']
		reporting_ip = self.local_dict[local_id]['reporting_ip']
		content = ''.join( item[1] for item in sorted([ self.local_dict[local_id][str(i)]['data'] for i in range(self.config['threads']) ],key=itemgetter(0)))
		with open(local_id,'wb') as f :
			f.write(content)
		req_url = "http://" + self.local_dict[local_id]['reporting_ip'] + ":" + str(self.config['server_port']) + "/fetch_local_data"
		post_data = {
			'start'	 		: self.local_dict[local_id]['0']['start'],
			'file_name'		: self.local_dict[local_id]['file'],
			'ip'			: self.config['home_ip'],
			'local_id'		: local_id
		}
		req = requests.post(req_url,data=post_data)		

	def fetch_local_data(self,local_id,ip,start,file_name) :
		req_url = 'http://' + ip + ':' + str(self.config['server_port']) + '/local_transfer'
		post_data = {
			'local_id' : local_id			
		}
		req = requests.post(req_url,data=post_data)
		content = req.content
		self.global_dict[self.md5(file_name)]['data'].append([int(start),content])		
		if len(self.global_dict[self.md5(file_name)]['data']) == len(self.global_dict[self.md5(file_name)]['active_peers']):
			threading.Thread(target=self.global_data_assembley,args=(file_name,)).start()

	def global_data_assembley(self,file_name) :				
		content = ''.join( item[1] for item in sorted(self.global_dict[self.md5(file_name)]['data'],key=itemgetter(0)))
		print '\n\nJob ' + self.md5(file_name) + ' done :D '
		print '\tFile :\t\t' + file_name
		print '\tDownload Url : \t' + self.global_dict[self.md5(file_name)]['url']
		print
		print
		with open(file_name,'wb') as f :
			f.write(content)		
		self.global_dict[self.md5(file_name)]['status'] = 'done'

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
