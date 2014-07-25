import pickle
import sys
import re
import zlib

class DataStore:
	def __init__(self):
		self.buckets = 10
		self.bucketsize = 100
		self.file = open('binaryblob', "r+")

	def choosebucket(self, key):
		crc = zlib.crc32(key)
		return crc % self.buckets

	def getlocation(self, bucket):
		return self.bucketsize * bucket;

	def getbucket(self, bucket):
		location = self.getlocation(bucket)
		self.file.seek(location)
		header = self.file.read(100)
		if (len(header) < 100):
			return {}

		numbers = re.compile('\d+(?:\.\d+)?')
		length = numbers.findall(header)

		if (len(length) < 1):
			return {}

		length = int(length[0])

		self.file.seek(location + 100)
		binary = self.file.read(length)
		
		data = pickle.loads(binary)
		return data

	def writebucket(self, bucket, data):
		binary = pickle.dumps(data)
		size = sys.getsizeof(binary)

		location = self.getlocation(bucket)
		self.file.seek(location)
		self.file.write(str(size))

		self.file.seek(location + 100)
		self.file.write(binary)

		return True

	def read(self, key):
		data = self.getbucket(self.choosebucket(key))

		if (key not in data):
			return False;

		return data[key]

	def write(self, key, value):
		bucket = self.choosebucket(key)
		data = self.getbucket(bucket)
		data[key] = value
		self.writebucket(bucket, data)
		return True

store = DataStore();
print store.read("lex");