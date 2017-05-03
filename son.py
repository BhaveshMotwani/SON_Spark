from pyspark import SparkContext
from collections import defaultdict
from operator import add
from itertools import combinations
import sys
sc = SparkContext(appName="inf553")

ms=1

def main():
    global ms
    f=[]
    mj=[]
    d=[]
    a = sc.textFile(sys.argv[1],2)
    ms=sys.argv[2]
    out=sys.argv[3]

#PHASE 1

    h=a.mapPartitions(apriori).map(lambda x:(x,1)).distinct()
    p=list(h.collect())
    for x,y in p:
	mj.append(x)
    g=a.mapPartitions(lambda x:apriori1(x,mj))

#PHASE 2

    f+=g.collect()
    f=sc.parallelize(f)
    s=f.reduceByKey(add)
    s=list(s.collect())
    for i in s:
	if i[1]>=float(ms)*len(a.collect()):
		d.append(i)

#Writing Output to file

    file = open(out,'w')
    kit=','
    for x,y in d:
	p=[]
	for i in x:
		p.append(str(i))
	z=tuple(p)
    	f=kit.join(tuple(p))
	file.write("%s\n"%f)

def apriori(x):
    j=[]
    o=[]
    r=[]
    oi=[]
    m={}
    lines=0
    po={}
    a=list(x)
    for line in a:
    	j+=list(line.split(','))
	lines+=1
    k=set(j)
    jk=len(k)
    b=1
	
    while(len(k)!=0 & b<=jk):
    	o=[]
    	mi=[]
    	o+=combinations(k,b)
    	for i in o:
		if(b==1 or set(combinations(i,b-1)).issubset(oi)==True):
			count=0
			for me in a:
				if(set(i).issubset(set(me.split(',')))):
					count+=1
			if(count>=(float(ms)*lines)):
				po[i]=1
				oi+=[i]
				mi+=i
    	k=set(mi)
	b+=1	
    return po

def apriori1(q,w):
    mk=[]
    q=list(q)
    w=list(w)
    for i in w:
	count=0
	for j in q:
		if set(i).issubset(set(j.split(','))):	
			count+=1
        mk.append((i,count))
    return mk







if __name__ == '__main__':
    main()
