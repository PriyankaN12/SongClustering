import sys
from pyspark import SparkConf, SparkContext
from song import song
from cluster import cluster
from collections import defaultdict
from itertools import combinations


sc=SparkContext("local","Lappname")

def hash_func(x,i):
    return (223*int(x)+139*i)%(n[0]//4)

def sign_value(i,y):
    return min([hash_func(x, i) for x in y])

def min_hash(u):

    for shingle in u:
        shingle_val=[sign_value(i,shingle[1:]) for i in range(100)]
        yield shingle[0], ",".join(str(x) for x in shingle_val)


file1 = sc.textFile("global_shingles.txt",10)
n = file1.map(lambda x: len(x)).collect()
print (n,"n")

file2 = sc.textFile("RS_shingles.txt",100).map(lambda x: x.strip().split(","))
# song_movie = file2.collect()

obj_list = file2.mapPartitions(min_hash)
result=obj_list.collect()
# print("obj_list",result[:2])

# file_out=open("min_hashes.txt","w")
file_out=open("RS_hashes.txt","w")

for row in result:
    a,b=row
    file_out.write(str(a)+"~"+b+"\n")
file_out.close()


