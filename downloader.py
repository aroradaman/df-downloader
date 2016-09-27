import time
import json
import hashlib
import urllib2
import threading
from operator import itemgetter
import os
from Queue import Queue

class dfDownloader :
	def __init__(self) :
		
		with open('config.json','r') as f :
			self.config = json.loads(f.read())
		
		self.manager = {}
		self.hasher = hashlib.md5()
		self.logQueue = Queue()

		organizerThread = threading.Thread(target=self.organizer,args=())
		organizerThread.start()

		loggerThread = threading.Thread(target=self.logger,args=())
		loggerThread.start()
	
	def md5(self, anything) :
		self.hasher.update(str(anything))
		return self.hasher.hexdigest()

	def logger(self) :
		while True :
			data = self.logQueue.get()
			print time.strftime("[%Y-%m-%d %H:%M]", time.localtime( time.time() ))  + str(data)

	def downloadInit(self, filename, url) :

		url = urllib2.unquote(url)
		print time.time()
		req = urllib2.urlopen(url)
		print time.time()
		meta = req.info()

		try :
			
			size = int(meta["Content-Length"])
		except KeyError :
			pass

		downloadId = self.md5(filename)
		self.logQueue.put(' Job ' + downloadId + ' started.')
		self.manager.update({ downloadId: {
								'url'		: url,
								'filename'	: filename,
								'start_time': time.time(),
								'worker'	: {},
								'size'		: size,
								'downloaded': 0
							}
						})
		
		workSplit = []
		frag = size/self.config['threads']
		start = 0

		for i in range(self.config['threads']) :	
			if i== 0 :
				workSplit.append([start,start+frag])
			else :
				workSplit.append([start+1,start+frag])
			start += frag
		workSplit[-1][1] = size

		for spliter in workSplit :
			workerId = self.md5( filename + str(spliter[0]) + str(time.time()))
			self.manager[downloadId]['worker'][workerId] = {
					'start'	:	spliter[0],
					'end'	:	spliter[1],
					'status':	False
			}
			baseDownloaderThread = threading.Thread(target=self.baseDownloader,args=(downloadId, workerId))
			baseDownloaderThread.start()	

	def assembler(self, downloadId) :
		assemblerData = []
		assemblerData = [ self.manager[downloadId]['worker'][workerId]['data'] for workerId in self.manager[downloadId]['worker'].keys() ]
		# print assemblerData
		assemblerData.sort(key=lambda x:x[0])

		with open(os.path.join(os.getcwd(),self.manager[downloadId]['filename']),'wb') as f :
			f.write(''.join( data[1] for data in assemblerData))

		self.logQueue.put('Job ' + downloadId + ' Completed')
		self.logQueue.put(json.dumps({
			'File Name' : self.manager[downloadId]['filename'],
			'Url'		: self.manager[downloadId]['url'],
			'Size'		: str((float(self.manager[downloadId]['size'])/1024)/1024) + ' MB',
			'Speed'		: str(((self.manager[downloadId]['size'] / (time.time() - self.manager[downloadId]['start_time']))/1024)/1024) + ' Mbps',
			'Time'		: time.time() - self.manager[downloadId]['start_time']

			},
		indent=4))
		downloadId = self.manager.pop(downloadId)

	def organizer(self) :
		while True :
			time.sleep(1.5)
			for downloadId in self.manager.keys() :
				completedJobs	  = 0
				for workerId in self.manager[downloadId]['worker'].keys() :
					if self.manager[downloadId]['worker'][workerId]['status'] :
						completedJobs += 1
						downloadCompleted = True
					else :
						downloadCompleted = False
					# print workerId,self.manager[downloadId]['worker'][workerId]['status'],completedJobs,downloadCompleted
				self.logQueue.put(' Job ' + downloadId + ' Pending- ' + str((float(self.manager[downloadId]['downloaded'])/self.manager[downloadId]['size'])*100) + '%')
				
				if completedJobs == self.config['threads'] :
					self.assembler(downloadId)
			

	def baseDownloader(self, downloadId, workerId) :

		content = ''
		baseDownloadUrl = self.manager[downloadId]['url']
		startByteRange  = self.manager[downloadId]['worker'][workerId]['start']
		endByteRange    = self.manager[downloadId]['worker'][workerId]['end']

		req = urllib2.Request(baseDownloadUrl)
		req.headers["Range"] = 'bytes='+str(startByteRange)+'-'+str(endByteRange)
		
		self.logQueue.put(' Job ' + workerId + ' Started')
		
		f = urllib2.urlopen(req)
		while True :
			data = f.read()
			self.manager[downloadId]['downloaded']	+= len(data)
			if not data :
				break
			content += data

		self.manager[downloadId]['worker'][workerId].update({'data' : [ startByteRange, content]})
		self.manager[downloadId]['worker'][workerId]['status'] = True
		
		self.logQueue.put(" Job " + workerId + ' Completed')
		

downloader = dfDownloader()
downloader.downloadInit('a.pkg','https://www.python.org/ftp/python/2.7.12/python-2.7.12-macosx10.6.pkg')
