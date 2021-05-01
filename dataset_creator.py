import csv
import numpy as np
import random as r
import datetime

random_name_list = []
with open('random-list-of-names.csv','rb')as namesFile:
	f_reader = csv.reader(namesFile)
	for row in f_reader:
		random_name_list.append(row)

namesFile.close()

with open("healthclub-dataset.csv", 'wb') as file:
	wr = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
	p1 = 3001
	i = 3000
	stayHealthy = 2000
	#random prob
	prob = np.random.choice([0.65,0.7,0.8])
	l = 0
	q = 0

	#randomize probability
	delta = 200
	while i != 0:
		if l == 0:
			l = 15
			fname,lname,member_ID = random_name_list[q][0], random_name_list[q][1], random_name_list[q][2]
			days = r.sample(range(1,29),15)
		l -= 1
		i -= 1
		if q > 4000:
			q = 0
		else:
			q += 1


		#choose direction
		changeDirection = np.random.choice(np.arange(0,2), p=[prob,1-prob])

		#randomize delta
		delta = np.random.choice(np.arange(0,850))

		#set date
		month = r.randint(1,3)
		date = datetime.date(2018, month, days[l])


		#choose random increase/decrease from previous two numbers
		if changeDirection > 0:
			cals = r.randint(p1-r.randint(0,delta), p1)
			p1 = cals
			data = [member_ID,fname,lname,cals,date]
			wr.writerow(data)
		else:
			cals = p1+r.randint(0,delta)
			p1 = cals
			data = [member_ID,fname,lname,cals,date]
			wr.writerow(data)

		if cals < 2200:
			prob = np.random.choice([0.7,0.8,0.85])
		elif cals > 4000:
			prob = np.random.choice([0.2,0.4,0.45])
		else:
			prob = np.random.choice([0.55,0.6,0.7,0.8])







