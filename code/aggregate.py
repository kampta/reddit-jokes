import json
import os, fnmatch

inpath = "../data/raw/reddit/"
outpath = "../data/output/"

count = 0
fw = open(outpath+'reddit.json', 'w')
for root, dirnames, filenames in os.walk(inpath):
	for filename in fnmatch.filter(filenames, '*.json'):
		js = json.loads(open(os.path.join(root, filename)).read())
		if len(js) > 0:
			for joke in js:
				fw.write("\n".join([joke['title'].encode('utf-8'), joke['selftext'].encode('utf-8')]) + "\n")
			count += len(js)
			print count
fw.close()
