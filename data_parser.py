# -*- coding: utf-8 -*-
"""
Created on Sun Apr 14 13:54:23 2019

@author: mhari
"""


import pandas as pd
import string

new_f=open("RS_data.txt","w")

file_contents=pd.read_csv('D:\EduDataMining-553\Project\songdata\lyrics.csv',sep=',')
file_contents=file_contents.values
allowed_chars=string.ascii_lowercase+string.digits+"  "
line_count=0
for i in range(0,362237,220):
    new_rec=list(file_contents[i])
    if type(new_rec[5]) is str:
        string_lyric=new_rec[5].lower()
        for k in string.punctuation+string.whitespace:
            string_lyric=string_lyric.replace(k," ")
        string_lyric="".join([c for c in string_lyric if c in allowed_chars ])
        song_lyric=" ".join(string_lyric.split())
        if song_lyric.strip()!='':
            new_f.write(str(line_count)+","+str(new_rec[2])+","+new_rec[3]+","+new_rec[4]+","+song_lyric+"\n")
            line_count+=1
    
new_f.close()

