#batch transformation

file = sc.textFile("healthclub-dataset.csv")
counts = file.flatMap(lambda line: [line.split(",")[1], str(line.split(",")[2]), line.split(",")[3]])\
.map(lambda fn,ln,cals: fn + " " + ln, cals)\
.reduceByKey(lambda v1,v2: int(v1)+int(v2))\
.sortBy(lambda x: x[1])\
.map(lambda k,v: "{0},{1}".format(k,v))

counts.saveAsTextFile("total_cals.csv")






file = sc.textFile("healthclub-dataset.csv")
counts = file.map(lambda line: (line.split(",")[2], line.split(",")[3]))\
.reduceByKey(lambda v1,v2: int(v1)+int(v2))\
.sortBy(lambda x: x[1])\
.map(lambda (k,v): "{0},{1}".format(k,v))

counts.saveAsTextFile("total_cals.csv")
