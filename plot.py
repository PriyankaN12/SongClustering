import matplotlib.pyplot as plt
import sys
from pyspark import SparkConf, SparkContext
sc=SparkContext("local","Lappname")

# from song import song
# from cluster import cluster

# x = np.random.randn(10)
# y = np.random.randn(10)
# Cluster = np.array([0, 1, 1, 1, 3, 2, 2, 3, 0, 2])    # Labels of cluster 0 to 3
# centers = np.random.randn(4, 2)
#
# fig = plt.figure()
# ax = fig.add_subplot(111)
# scatter = ax.scatter(x,y,c=Cluster,s=50)
# for i,j in centers:
#     ax.scatter(i,j,s=50,c='red',marker='+')
# ax.set_xlabel('x')
# ax.set_ylabel('y')
# plt.colorbar(scatter)
#
# fig.show()
filename="Large"
wr=open("plot.txt","w")
file1 = sc.textFile(filename+"_data.txt").flatMap(lambda x:x.split("\n")).map(lambda x:x.split(",")).map(lambda x:(x[0],x[1:]))
# file2=sc.textFile(filename+"_hashes.txt").flatMap(lambda x:x.split("\n")).map(lambda x:x.split("~")).map(lambda x:(x[0],x[1]))
file3=sc.textFile(filename+"_kmeans.txt").flatMap(lambda x:x.split("\n")).map(lambda x:x.split(":")).map(lambda x:(x[0],x[1]))
file3=file3.flatMapValues(lambda x:x.split(",")).map(lambda x:(x[1],x[0]))
# j=file1.join(file2)
j1=file1.join(file3)

j2=j1.collect()
# print j2
for i in j2:
    s=i[0]+":"
    s+=",".join(i[1][0])
    # print i[1][0]
    # s+=":"+i[1][0][1]
    s+=":"+i[1][1]+"\n"
    wr.write(s)
wr.close();
