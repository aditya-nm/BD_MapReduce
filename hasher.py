import sys
n= int(sys.argv[1])
for message in sys.stdin:
	full_msg=message.split()
	first=full_msg[0]
	print(int(ord(first[0]))%n)
