import matplotlib as mpl
mpl.use('TkAgg')
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

pt=open("plot.txt","r").readlines()
lb=[]
n=[]
dl={}
m=0
clb=[]
grp=[]
dlc={}
vect = TfidfVectorizer()
for i in pt:
    x=i.split(":")
    yy=x[1].split(",")
    lbl=yy[1]
    if lbl in dl:
        lb.append(lbl)
    else:
        dl[lbl]=m*10
        m+=1
        lb.append(lbl)
    ny=yy[3]
    n.append(ny)
    dd=x[2].strip()
    if dd in dlc:
        clb.append(dd)
    else:
        dlc[dd]=m*10
        clb.append(dd)

vc=vect.fit_transform(n).toarray()

pca = PCA(n_components=2)
red=pca.fit_transform(vc)
# print red
xa=[]
ya=[]
for i in red:
    xa.append(i[0])
    ya.append(i[1])

xa=np.array(xa)
ya=np.array(ya)
colors = ("red", "blue","green","aqua","brown","indigo","gold","lavender","lime","magenta","maroon","navy","orange","plum","salmon","wheat")
colors+=("yellow","pink","khaki","coral","azure","grey","teal","yellowgreen")

cd={}
if 'Rock' in dl and 'Hip-Hop' in dl:
    dl['Hip-Hop']+=dl['Rock']
    del dl['Rock']
if 'Other' in dl and 'Hip-Hop' in dl:
    dl['Hip-Hop']+=dl['Other']
    del dl['Other']
if 'Metal' in dl and 'Pop' in dl:
    dl['Pop']+=dl['Metal']
    del dl['Metal']
print len(dl.keys())
for i,v in enumerate(dl.keys()):
    cd[v]=colors[i]
print cd
for i,v in enumerate(lb):
    if v=='Rock' or v=='Other':
        lb[i]='Hip-Hop'
    if v=='Metal':
        lb[i]='Pop'

pll=np.array(lb)

print pll
fig = plt.figure()
ax = fig.add_subplot(111)
for g in np.unique(pll):
    ix = np.where(pll == g)
    ax.scatter(xa[ix], ya[ix], c = cd[g], label = g, s = 20)
# ax.scatter(xa, ya, alpha=1.0, c=lb, edgecolors='none', s=20)
plt.title('Matplot scatter plot')
plt.legend(loc=2)
plt.show()

# cdc={}
# for i,v in enumerate(dlc.keys()):
#     cdc[v]=colors[i]
# pll1=np.array(clb)
# print cdc
# fig1 = plt.figure()
# ax1 = fig1.add_subplot(111)
# for g in np.unique(pll1):
#     ix = np.where(pll1 == g)
#     ax1.scatter(xa[ix], ya[ix], c = cdc[g], label = g, s = 20)
#
# plt.title('Matplot scatter plot')
# plt.legend(loc=2)
# plt.show()
