import sys
from pyspark import SparkConf, SparkContext
from song import song
from cluster import cluster
from collections import defaultdict
from itertools import combinations

# G_B=10
G_K=3
sc=SparkContext("local","Lappname")
file1 = sc.textFile("global_shingles.txt")
n = file1.map(lambda x: len(x)).collect()
min_hash_dict = {}
song_dict = defaultdict(list)
sim_songs = defaultdict(list)

def hashes(x,i):
        x = int(x)
        i = int(i)
        val = ((5*x) + (13*i))%n[0]
        return val

def min_hash(u):
    l = []
    for shingle in u:
        shingle_val = []
        for i in range(0,1000):
            min_val = float('inf')
            for x in shingle[1:]:
                min_val = min(min_val, hashes(x,i))
            shingle_val.append(min_val)
        song = [shingle[0]]
        song.extend(shingle_val)
        l.append(song)
    return l

def lsh(l):
	combs = []
	rev_list = []
	l = list(l)
	d = defaultdict(list)
	ret_val = []
	for i in l[0][1]:
		rev = []
		key = ",".join(str(v) for v in i[1:])
		d[key].append(i[0])
	for i in d:
		pairs = list(combinations(d[i],2))
		ret_val.append((i,pairs))
	return ret_val

def create_bands(l):
	song_band = []
	song = l[0]
	l = l[1:]
	d = defaultdict(list)
	for i in range(len(l)):
		b_num = int(i/2)
		if(song not in d[b_num]):
			d[b_num].append(song)
		d[b_num].append(l[i])
	ret_val = []
	for i in d:
		ret_val.append((i,d[i]))
	return ret_val

def jaccard(a,b):
	inter = set(a).intersection(set(b))
	inter_size = len(inter)
	union = set(a).union(set(b))
	union_size = len(union)
	return float(inter_size)/union_size

def similar_fn(l):
	for j in l:
		for i in j[1]:
			if(len(i)>1 and i[0] in min_hash_dict and i[1] in min_hash_dict):
				val = jaccard(min_hash_dict[i[0]],min_hash_dict[i[1]])
				song_dict[i[0]].append((val,i[1]))
				song_dict[i[1]].append((val,i[0]))

	for i in song_dict:
		lo = list(set(song_dict[i]))
		for j in lo:
			sim_songs[i].append(j[1])

file2 = sc.textFile("small1.txt").map(lambda x: x.split(","))
song_movie = file2.collect()
obje_file = file2.map(lambda x: (x[0], x[1:])).map(lambda x: song(x[0],x[1],n)).collect()

for i in song_movie:
	print("song",i[0])
	min_hash_dict[i[0]] = i[1:]

obj_list = file2.mapPartitions(min_hash)


bands = obj_list.map(create_bands)
band_rdd = bands.flatMap(lambda x: [i for i in x]).groupByKey().map(lambda x: (x[0],list(x[1])))
part_band = band_rdd.partitionBy(500,lambda x:x)

rev = part_band.mapPartitions(lsh)

similar_fn(rev.collect())

for i in sim_songs:
	for j in list(obje_file):
		if(j.id == i):
			j.assign_similar_songs(sim_songs[i])


def find_cluster(x):
    d={}
    x=list(x)
    if len(x)>0:
        for i in x:
            # i.printer()
            if i.id not in cid:
                js=-1
                c=None
                for (k,j) in enumerate(cen):
                    y=jaccard(j.centroid.shingle_list,i.shingle_list)
                    if y>js:
                        js=y
                        c=j.centroid.id
                if c in d:
                    d[c].append(i)
                else:
                    d[c]=[i]

    rt=[]
    for i in d:
        rt.append((i,d[i]))
    return rt


songs=sc.parallelize(obje_file,8)
cent=songs.takeSample(False,G_K,2)
print len(songs.collect())
cen=[]
cid=set()
for i in cent:
    cen.append(cluster(i))
    cid.add(i.id)

# print cid
cl_as=songs.mapPartitions(lambda x:find_cluster(x)).reduceByKey(lambda x,y:x+y).collectAsMap()
# print cl_as
for i in cen:
    if i.centroid.id in cl_as:
        i.members.extend(cl_as[i.centroid.id])

# print "*"*50
# for i in cen:
#     i.printer()
