#!/usr/bin/env python
import sys
import pysam

def read_input():
	#for line in file:
	#print sys.argv[1]

	f = pysam.view('-')
	for line in f:
		yield line
		
def main(separator = '\t'):
	data = read_input()
	
	for line in data:
		line2 = str(line).strip()
		##print line2
		cols = line2.split(separator)
		if abs(int(cols[8])) > 10000:
			print line2
						
if __name__ == "__main__":
	main()
