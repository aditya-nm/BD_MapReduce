from multiprocessing import Process, Pipe, Pool
import threading
import os
import time
import sys
#Imported all modules 


#A simple function we will use to identify processes
def info():
	print('module name:',__name__ ) 
	print('parent process: ',os.getppid())
	print('process id: ',os.getpid())

def display_nodes(pipe):
	time.sleep(5)
	info()
	command= "mkdir "+str(os.getpid())
	stream= os.popen(command)
	output=stream.read()
	print("Directory created, memory has been allocated to node.")
	return str(os.getpid())
	
#Boolean function to test if word is in ledger
def search_str1(file_path, word):
	file1= open(file_path,'r+')
	content = file1.read()
	file1.close()
	if (word in content):
		return True
	else: 
		return False
	
##################################################################################################
#Given below are the various functions that execute on the worker nodes as required
##################################################################################################
def worker_write_process(child_conn):
	time.sleep(13)
	filename= child_conn.recv()	
	LocalDFS_Filename=pwd+"/"+str(os.getpid())+"/"+ filename+str(os.getpid())+".txt"
	file1 = open(LocalDFS_Filename, "a")  # append mode
	print("\t \t File opened")
	child_conn.send("Ready to receive data")
	while(1):
		l=child_conn.recv()
		if(l=="EOF"):
			break;
		file1.write(l)
	time.sleep(1)
	child_conn.send("\t \t Closing local storage and returning to idle state")
	file1.close()
###################################################################################################
#EXTRA FUNCTIONS HERE 
###################################################################################################
def shuffling_thread(receiver_end, signal):
	j=n
	while True:
		message= signal.recv()
		if(message=="KILL"):
			break
		elif(message=="READ"):
			
			print("Executing shuffle here")
			active=True
			print("Going to try and receive message.. ")
			msg=receiver_end.recv()
			print(msg)
			#output1= msg.readline()
			LocalDFS_Filename=pwd+"/"+str(os.getpid())+"/"+ "Intermediate2"+str(os.getpid())+".txt"
			file2 = open(LocalDFS_Filename, "a") 
			while True:
				if(msg=="EOF"):
					j= j-1
					if(j==0):
						break
				else:
					file2.write(msg)
				msg=receiver_end.recv()
			file2.close()
			print("Stopped receiving messages.. ")
		else:
			pass
	
def launch_shuffle(param):
	
	child=param[0]
	child1=param[1]
	launching_shufflers_thread = threading.Thread(target=shuffling_thread, args=(child,child1))
	launching_shufflers_thread.start()
	

def kill_shuffle(param):
	thread= param[0]
	signal=param[1]
	signal.send("KILL")
	
def worker_read_process(child_conn):
	filename= child_conn.recv()	
	LocalDFS_Filename=pwd+"/"+str(os.getpid())+"/"+ filename+str(os.getpid())+".txt"
	file1 = open(LocalDFS_Filename, "r+")  # read mode
	print("\t \t File opened")
	child_conn.send("Ready to send data")
	iterline= file1.readline()
	while iterline: 
		child_conn.send(iterline)
		iterline= file1.readline()
	child_conn.send("EOF")
	file1.close()
	msg=child_conn.recv()
	if("All received"):
		pass
		output=""
		file2 = open(LocalDFS_Filename, "r+")  # read mode
		for line in file2:
			if not line.isspace():
				output=output+line
		file3 = open(LocalDFS_Filename, "w")  # write mode 
		file3.write(output)
		file2.close()
		file3.close()
	else:
		print("WARNING: Packets might have gone missing")
	child_conn.send("Closing read")
	
def worker_mapper_process(param):
	child_conn=param[0]
	write_to_thread=param[1]
	signal_thread=param[2]
	pipes=param[3]
	thread_writes=param[4]
	ProcessIDs=param[5]
	msg=child_conn.recv()
	output_filename=child_conn.recv()
	intput_filename=child_conn.recv()
	
	child_conn.send("Yes")
	abs_mapper= child_conn.recv()
	
	
	intput_filename=pwd+"/"+str(os.getpid())+"/"+ intput_filename+str(os.getpid())+".txt"
	command="cat "+intput_filename+"| python3 "+abs_mapper
	stream= os.popen(command)
	output= stream.readline()
	LocalDFS_Filename=pwd+"/"+str(os.getpid())+"/"+ "Intermediate1"+str(os.getpid())+".txt"
	file1 = open(LocalDFS_Filename, "a") 
	while output:
		file1.write(output)
		output=stream.readline()
	file1.close()
	
	child_conn.send("Mapper done")
	child_conn.send("Beginning shuffle")
	
	LocalDFS_Filename=pwd+"/"+str(os.getpid())+"/"+ "Intermediate1"+str(os.getpid())+".txt" 
	print("Shuffling now..")
	command="cat "+LocalDFS_Filename+"| python3 hasher.py "+str(n)+" > "+pwd+"/"+str(os.getpid())+"/Shuffles.txt"
	stream1= os.system(command)
	command="cat "+LocalDFS_Filename+"| python3 shuffler.py "+pwd+"/"+str(os.getpid())
	stream1= os.system(command)
	
	FromFile=pwd+"/"+str(os.getpid())+"/"+ "Intermetiate_for_"
	ToFile=pwd+"/"+str(os.getpid())+"/"+ "Intermetiate_from_"
	current= ProcessIDs.index(str(os.getpid()))
	
	for id_it in range(len(ProcessIDs)):
		command= "cat "+FromFile+str(id_it)+".txt >> "+pwd+"/"+ProcessIDs[id_it]+"/"+ "Intermetiate_from_"+str(current)+".txt"
		os.system(command)
	
	time.sleep(2)
	for i in range(len(ProcessIDs)):
		command="cat "+ToFile+str(i)+".txt >> "+pwd+"/"+str(os.getpid())+"/Intermediate2.txt"
		os.system(command)
	command="cat "+pwd+"/"+str(os.getpid())+"/Intermediate2"+".txt | sort > "+pwd+"/"+str(os.getpid())+"/Intermediate2"+str(os.getpid())+".txt" 
	os.system(command)
	child_conn.send("Reducer ready")
	abs_reducer= child_conn.recv()
	print(abs_reducer, "received")
	print("Reducer executes here")
	
	input_filename= pwd+"/"+str(os.getpid())+"/"+ "Intermediate2"+str(os.getpid())+".txt"
	command="cat "+input_filename+"| python3 "+abs_reducer
	stream= os.popen(command)
	output= stream.readline()
	LocalDFS_Filename=pwd+"/"+str(os.getpid())+"/"+ output_filename+str(os.getpid())+".txt"
	file3 = open(LocalDFS_Filename, "a") 
	while output:
		file3.write(output)
		output=stream.readline()
	file3.close()
	
	
	child_conn.send("Reducer done")



##################################################################################################
#Given below is the function that the client process spawned is redirected to 
##################################################################################################

def client(parent_connections,client_connec, client_connec_thread,newstdin,pipes):
	sys.stdin= newstdin
	#Menu driven program to do all of the tasks 
	#READ, WRITE, MAPREDUCE 
	time.sleep(10)
	print("This is the client server booting up")
	print(info())
	command= "mkdir client"
	stream= os.popen(command)
	output=stream.read()
	print("Directory created, memory has been allocated to node.")
	print("Enter which function you want to execute: ")
	print("1: Write")
	print("2: Read")
	print("3: MapReduce")
	print("4: Terminate")
	choice= input()
	while(choice != "4"):
		if(choice== "1"):
			client_connec.send("Write")
			#Now we will communicate with the write thread
			message= client_connec_thread.recv()
			print(message, "Received from write thread")
			print("Enter the raw filepath to be stored in DFS")
			filepath= input()
			#End of communication with the write thread 
			#Begin communicating with the workers_write_process
			print("Enter the name you want the DFS file to be stored as")
			filename= input()
			client_connec_thread.send(filename)
			filename=client_connec_thread.recv()
			for i in range(n):
				pipes[i].send(filename)
			for i in range(n):
				j=pipes[i].recv()
			file1=open(filepath, "r+")
			lines= len(file1.readlines())
			file1.close()
			file1 = open(filepath,"a")
			for i in range(lines%n):

    				file1.write("\n")

			file1.close()
			fileop= open(filepath, "r+")
			iterline= fileop.readline()
			while iterline: 
				for i in range(n):
					pipes[i].send(iterline)
					iterline= fileop.readline()
					if iterline:
						pass
					else:
						break
					
				if iterline:
					pass
				else:
					break
			print("EOF detected and terminating.. ")
			fileop.close()
			for i in range(n):
				pipes[i].send("EOF")
			#End of communication with the launch workers_thread
			#Back to communicating with the write_thread 
			for i in range(n):
				j=pipes[i].recv()
			client_connec_thread.send("Finished Write")
			print("Ack of Task completed received on all nodes")
			print("Your BD_DFS filename is :", filename)
			#Back to communicating with the main server thread 
			closing_ACK=client_connec.recv()
			if closing_ACK== "Write termination ACK":
				pass
			else:
				print("An error occured exiting the write process")
		elif(choice== "2"):
			client_connec.send("Read")
			#Now we will communicate with the read thread
			msg= client_connec_thread.recv()
			print(msg, "Received from read thread")
			print("Enter the BD_DFS name of the file that you want to read.")
			filename=input()
			client_connec_thread.send(filename)
			presence= client_connec_thread.recv()
			if (presence== "Success"):
				##We will communicate with workers within this section
				for i in range(n):
					pipes[i].send(filename)
					print("Sent")
				for i in range(n):
					j=pipes[i].recv()
				Local_Client_FS_Filename=pwd+"/"+"client"+"/"+ filename+".txt"
				file2 = open(Local_Client_FS_Filename, "a")  # append mode
				while(1):
					for i in range(n):
						l=pipes[i].recv()
						if(l=="EOF"):
							break
						if not l.isspace():
							file2.write(l)	
					if(l=="EOF"):
						break
				file2.close()
				for i in range(n):
					pipes[i].send("All received")
				for i in range(n):
					j=pipes[i].recv()
				#End of communication with workers
				client_connec_thread.send("Terminate")
			else:
				print("Error: File not found")
				client_connec_thread.send("Terminate")
			#Back to communicating with the main server thread 
			closing_ACK=client_connec.recv()
			if closing_ACK== "Read termination ACK":
				pass
			else:
				print("An error occured exiting the read process")
		elif(choice== "3"):
			client_connec.send("MapReduce")
			#Now we will communicate with the mapReduce thread
			msg=client_connec_thread.recv()
			print("Enter the DFS filename you want to perform the mapReduce on: ")
			filename= input()
			temp= filename
			
			client_connec_thread.send(filename)
			message=client_connec_thread.recv()
			
			
			if(message== "Success"):
				outputfilename= "Output"
				client_connec_thread.send(outputfilename)
				filename= client_connec_thread.recv()
				
				##Communicating with workers directly now
				print("NOw")
				for i in range(n):
					pipes[i].send("Are you ready")
					pipes[i].send(outputfilename)
					pipes[i].send(temp)
				print("Here")	
				for i in range(n):
					j=pipes[i].recv()
				print("There")	
				print("Enter the absolute path to the mapper file")
				abs_mapper=input()
				for i in range(n):
					pipes[i].send(abs_mapper)
					
				for i in range(n):
					j=pipes[i].recv()
					
				print("Mapper task Successfully completed")
				#Wait for shuffle 
				for i in range(n):
					j=pipes[i].recv()
					
				print("Shuffle is taking place")
				for i in range(n):
					j=pipes[i].recv()
					
				print("Shuffle has concluded")
				print("Enter the absolute path to the redcuer")
				abs_reducer=input()
				for i in range(n):
					pipes[i].send(abs_reducer)
					
				for i in range(n):
					j=pipes[i].recv()
				print("Map reduce successfully executed. You may find your results at:",outputfilename," BD_DFS file")
					
				#Back to communicating with the mapReduce thread 
			else:
				print("Your file does not exist in DFS")
			client_connec_thread.send("random message")
			msg= client_connec_thread.recv()
			#Back to communicating with the main server thread 
			closing_ACK=client_connec.recv()
			if closing_ACK== "mapReduce termination ACK":
				pass
			else:
				print("An error occured exiting the mapReduce process")
		else:
			print("Invalid choice")
		print("Enter which function you want to execute: ")
		print("1: Write")
		print("2: Read")
		print("3: MapReduce")
		print("4: Terminate")
		choice= input()
	print("Exiting the client mode and sending message to disband server.. ")
	client_connec.send("Terminate")

##################################################################################################
#The below section includes the functions that different threads of the server program redirect to
##################################################################################################
#Function assisting in write
def search_str(file_path, word):
	file1= open(file_path,'r+')
	content = file1.read()
	while (word in content):
		word=word+"1"
	file1.close()
	file2=open(file_path,'a')
	file2.write(word+ "\n")
	file2.close()
	return word
	
def write(n,pipes,flag):
	#To CLIENT: Write task has been scheduled
	server_connec_thread.send("Begin")
	file1 = open("ledger.txt", "a") 
	
	filename= server_connec_thread.recv()
	
	final_filename= filename
	server_connec_thread.send(final_filename)
	#Transfer communication until launch_workers process finishes communicating 
	
	#Wait for a signal from client that it has written everything 
	msg=server_connec_thread.recv()
	if (msg== "Finished Write"):
		pass
	else:
		print("PipeError")
	 # append mode
	file1.write(final_filename)
	file1.close()
	print("Exiting write thread.. ")
	#END

def mapReduce(n,parent_connections,flag):
	#To CLIENT: MapReduce task has been scheduled, send mapper file, reducer file, DFS directory path, output file name
	server_connec_thread.send("mapReduce scheduled")
	filename= server_connec_thread.recv()
	if (search_str1(r'ledger.txt', filename)):
		server_connec_thread.send("Success")
	else:
		server_connec_thread.send("Failiure")
	
	#Search if the filename exists in ledger
	filename= server_connec_thread.recv()
	print(filename)
	final_filename= search_str(r'ledger.txt', filename)
	server_connec_thread.send(final_filename)
	#from CLIENT: Receive clients and store 
	#Wait for ACK from all the ndoes that they are ready for mapreduce 
	#Tell it to start the MAP tasks
	#Wait for a signal that MAP has finished 
	#Tell it to start the SHUFFLE tasks
	#Wait for a signal that SHUFFLE has finished 
	#Tell it to start the REDUCE tasks
	#Wait for a signal that REDUCE has finished 
	#Wait for ACK from all nodes that they have finished the local writes of OUTPUT
	#Print that mapReduce has completed
	#Pass the DFS filename where the output is stored 
	msg=server_connec_thread.recv()
	server_connec_thread.send("Exiting mapReduce thread now")
	#END
	print("dummy")

def read(n,parent_connections,flag):
	#To CLIENT: Read task has been scheduled
	server_connec_thread.send("Begin")
	#from CLIENT: Which nodes do I read from? 
	filename= server_connec_thread.recv()
	#Lookup filename
	if (search_str1(r'ledger.txt', filename)):
		server_connec_thread.send("Success")
	else:
		server_connec_thread.send("Failiure")
	#Wait for ACK from all the ndoes that they have created the directory and opened files 
	#Send pipes to CLIENT and tell it to read from those 
	msg= server_connec_thread.recv()
	#Wait for ACK from all nodes that they have finished the local reads
	
	#Wait for a signal that it has read everything and encountered EOF
	#Print that read task has completed 
	#END
	print("Exiting read thread.. ")

def launch_workers(n,pipes,flag): 
	print("Initiating launch of worker nodes..")
	write_to_threads=[]
	read_in_thread=[]
	signal_threads=[]
	receive_signals=[]
	passing_param=[]
	param_mapReduce=[]
	for i in range(n):
		parent, child= Pipe()
		write_to_threads.append(parent)
		read_in_thread.append(child)
		parent1, child1= Pipe()
		signal_threads.append(parent1)
		receive_signals.append(child1)
		
		passing_param.append((child, child1)) 
		param_mapReduce.append((pipes[i],parent,parent1,pipes,write_to_threads))
	
	with Pool(processes=n) as pool:
		Process_IDs= pool.map(display_nodes, pipes)
		var= pool.map(launch_shuffle,passing_param)
		while True:
			control=intcom2.recv()
			if(control== "Idle"):
				pass
				
			elif (control== "Write"):
				var=pool.map(worker_write_process, pipes)
				control="Idle"
				
			elif (control== "MapReduce"):
				for i in range(len(param_mapReduce)):
					a,b,c,d,e= param_mapReduce[i]
					param_mapReduce[i]=(a,b,c,d,e,Process_IDs)
				var=pool.map(worker_mapper_process, param_mapReduce)
				control= "Idle"
				
			elif (control== "Read"):
				var=pool.map(worker_read_process, pipes)
				control= "Idle"
				
			elif (control== "Terminate"):
				var= pool.map(kill_shuffle,passing_param)
				break	
			else:
				server_connec.send("Inavlid data received ACK")
			if event.is_set():
				break

##################################################################################################
#Here we define the entry point main function. Executed only once when server2.py is called.	
##################################################################################################
if __name__ =="__main__":
	pwd= os.popen('pwd').read().strip()

	#Accept number of nodes in network 
	print("Welcome to Project of BD1_203_208_211_257..")
	print("This is the server process")
	print(info())
	time.sleep(5)
	print("Enter the number of nodes in the network")
	n= int(input())
	
	#Declare and initialize variable that triggers jobs
	control="Idle"
	flag= False
	
	#Create the pipes
	child_connections=[]
	parent_connections=[]
	for i in range(n):
		parent_conn, child_conn = Pipe()
		child_connections.append(child_conn)
		parent_connections.append(parent_conn)
		
	#This is the event that will trigger collapse of worker nodes 
	event= threading.Event()
	
	#Creating the client process and a pipe to interact with the user further
	server_connec, client_connec= Pipe()
	intcom1, intcom2= Pipe()
	server_connec_thread, client_connec_thread= Pipe()
	newstdin= os.fdopen(os.dup(sys.stdin.fileno()))
	
	# creating thread to manage workers
	launching_workers_thread = threading.Thread(target=launch_workers, args=(n,child_connections,flag))
	launching_workers_thread.start()
	time.sleep(3)
	
	Process(target=client, args=(parent_connections,client_connec,client_connec_thread,newstdin,parent_connections)).start()
	
	#Declare the different threads we may use without starting any of them. 
	write_thread = threading.Thread(target=write, args=(n,parent_connections,flag))
	mapReduce_thread = threading.Thread(target=mapReduce, args=(n,parent_connections,flag))
	read_thread = threading.Thread(target= read, args= (n, parent_connections,flag))
	control= server_connec.recv()
	
	
	while(control != "Terminate"):
		if(control == "Idle"):
			pass
		elif (control== "Write"):
			write_thread = threading.Thread(target=write, args=(n,parent_connections,flag))
			write_thread.start()
			intcom1.send("Write")
			write_thread.join()
			server_connec.send("Write termination ACK")
		elif (control== "MapReduce"):
			mapReduce_thread = threading.Thread(target=mapReduce, args=(n,parent_connections,flag))
			mapReduce_thread.start()
			intcom1.send("MapReduce")
			mapReduce_thread.join()
			
			server_connec.send("mapReduce termination ACK")
		elif (control== "Read"):
			read_thread = threading.Thread(target= read, args= (n, parent_connections,flag))
			read_thread.start()
			intcom1.send("Read")
			read_thread.join()
			server_connec.send("Read termination ACK")
		else:
			server_connec.send("Inavlid data received ACK")
		control=server_connec.recv()
	intcom1.send("Terminate")
	event.set()
	launching_workers_thread.join()
	print("Done!")
