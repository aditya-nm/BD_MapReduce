#!/usr/bin/env python3
import sys 
current_key= None 
current_value= None

for line in sys.stdin: 
	key,value= line.split("\t")
	value= int(value)
	if key == current_key: 
		current_value= current_value+ value 
	else:
		if current_key != None:
			print(current_key+"\t"+str(current_value))
		current_key= key 
		current_value= value
if (current_key != None):	
	print(current_key+"\t"+str(current_value))
