import os
import csv



bowl_cluster=dict()
bat_cluster=dict()
fp=open('cluster_bowler.csv','r')
row=fp.read().split('\n')
row=[x.split(',') for x in row]
row.pop()
for x in row:
        bowl_cluster[x[2]]=[x[0],x[3],x[4]]
fp=open('cluster_batsmen.csv','r')
row=fp.read().split('\n')
row=[x.split(',') for x in row]
row.pop()
for x in row:
        bat_cluster[x[2]]=[x[0],x[3],x[4]]        
#print(bat_cluster,bowl_cluster)
dir1='output_csv'
files = os.listdir(dir1)
lis=[]
for file in files:
        
        f=open(dir1+'/'+file,'r')
        row=f.read().split('\n')
        row=[x.split(',') for x in row]
        row.pop()
        l=[0,0,0,0,0,0,0,0,0,0,0]
        i=0
        batsmans=set()
        leng=1
        for x in row:
                #c1=cluster_batsman[x[3]]
                ball=float(x[1])
                if int(ball)==i:
                        l[0]=bowl_cluster[x[4]][0]
                        l[1]=bowl_cluster[x[4]][1]
                        l[2]=bowl_cluster[x[4]][2]
                        batsmans.add(x[3])
                        l[9]+=int(x[6])
                        if(len(x)>9):
                                l[10]=l[10]+1
                else:
                       
                        bat1=batsmans.pop()
                        l[3]=bat_cluster[bat1][0]
                        l[4]=bat_cluster[bat1][1]
                        l[5]=bat_cluster[bat1][2]
                        try:
                                bat2=batsmans.pop()
                                l[6]=bat_cluster[bat2][0]
                                l[7]=bat_cluster[bat2][1]
                                l[8]=bat_cluster[bat2][2]
                        except KeyError:
                                l[6]=bat_cluster[bat1][0]
                                l[7]=bat_cluster[bat1][1]
                                l[8]=bat_cluster[bat1][2]
                        
                       
                        lis.append(l)
                        
                        l=[0,0,0,0,0,0,0,0,0,0,0]
                        batsmans=set()
                        i+=1
                        ball=float(x[1])
                        if int(ball)==0:
                                i=0
                        if int(ball)==i :
                                l[0]=bowl_cluster[x[4]][0]
                                l[1]=bowl_cluster[x[4]][1]
                                l[2]=bowl_cluster[x[4]][2]      
                                batsmans.add(x[3])
                                l[9]+=int(x[6])
                                if(len(x)>9):
                                        l[10]=l[10]+1
                        if(len(row)==leng):
                          bat1=batsmans.pop()
                          l[3]=bat_cluster[bat1][0]
                          l[4]=bat_cluster[bat1][1]
                          l[5]=bat_cluster[bat1][2]
                          try:
                                bat2=batsmans.pop()
                                l[6]=bat_cluster[bat2][0]
                                l[7]=bat_cluster[bat2][1]
                                l[8]=bat_cluster[bat2][2]
                          except KeyError:
                                l[6]=bat_cluster[bat1][0]
                                l[7]=bat_cluster[bat1][1]
                                l[8]=bat_cluster[bat1][2]
                                lis.append(l)
                leng+=1        
fp=open('details.csv','w+')
writer=csv.writer(fp)                                           
for x in lis:
        writer.writerow(x)
