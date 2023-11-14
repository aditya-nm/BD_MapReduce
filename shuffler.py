import sys
import os 

curr_path= sys.argv[1]
f = open(curr_path+"/Shuffles.txt", "r")

lines= [line.strip() for line in f.readlines()]

count=0
for line in sys.stdin:
	os.system("echo \""+line.strip()+"\" >> "+curr_path+"/Intermetiate_for_"+lines[count]+".txt")
	count+=1
