from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark import SparkConf, SparkContext
from numpy import array
import sys
conf=SparkConf().setMaster("local")
sc=SparkContext(conf=conf)

def createLabeledPointsRuns(fields):
        
        bowl_c=int(fields[0])
        bowl_wic=int(fields[1])
        bowl_eco=float(fields[2])
        bat1_c=int(fields[3])
        bat1_avg=float(fields[4])
        bat1_sr=float(fields[5])
        bat2_c=int(fields[6])
        bat2_avg=float(fields[7])
        bat2_sr=float(fields[8])
        runs=int(fields[9])
        return LabeledPoint(runs, array([bowl_c, bowl_wic, bowl_eco,bat1_c,bat1_avg,bat1_sr,bat2_c,bat2_avg,bat2_sr]))
        
def createLabeledPointsWickets(fields):
        
        bowl_c=int(fields[0])
        bowl_wic=int(fields[1])
        bowl_eco=float(fields[2])
        bat1_c=int(fields[3])
        bat1_avg=float(fields[4])
        bat1_sr=float(fields[5])
        bat2_c=int(fields[6])
        bat2_avg=float(fields[7])
        bat2_sr=float(fields[8])
        wickets=int(fields[10])
        return LabeledPoint(wickets, array([bowl_c, bowl_wic, bowl_eco,bat1_c,bat1_avg,bat1_sr,bat2_c,bat2_avg,bat2_sr]))
rawData=sc.textFile('details.csv')


csvData=rawData.map(lambda x:x.split(","))
trainingData1=csvData.map(createLabeledPointsRuns)
model1= DecisionTree.trainRegressor(trainingData1, categoricalFeaturesInfo={},
                                        impurity='variance', maxDepth=14, maxBins=30)
trainingData2=csvData.map(createLabeledPointsWickets)
model2= DecisionTree.trainRegressor(trainingData2, categoricalFeaturesInfo={},
                                        impurity='variance', maxDepth=14, maxBins=30)
                                        
                                        
                                        
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

score_list=[]
lis=[sys.argv[1],sys.argv[2]]     
teams=[0,0]
for k in range(0,2):
        f1=open(lis[k],'r')
        data=f1.readlines()
        teams[k]=data[0].strip('\r\n')
        batsman=data[1].strip('\r\n').split(',')
        bowler=data[2].strip('\r\n').split(',')
        bat_A=batsman.pop(0)
        bat_B=batsman.pop(0)
        score=0
        index_error=0
        change=0
        for i in range(1,21):
                bowl_X=bowler.pop(0)
                testing_runs=[array([bowl_cluster[bowl_X][0],bowl_cluster[bowl_X][1],bowl_cluster[bowl_X][2],
                                bat_cluster[bat_A][0],bat_cluster[bat_A][1],bat_cluster[bat_A][2],
                                bat_cluster[bat_B][0],bat_cluster[bat_B][1],bat_cluster[bat_B][2] ])]
                testData=sc.parallelize(testing_runs)
                predictions=model1.predict(testData)
                results=predictions.collect()
                for result in results:
                        print("the Runs are:",result)
                        score+=round(result)
                testing_wick=[array([bowl_cluster[bowl_X][0],bowl_cluster[bowl_X][1],bowl_cluster[bowl_X][2],
                                bat_cluster[bat_A][0],bat_cluster[bat_A][1],bat_cluster[bat_A][2],
                                bat_cluster[bat_B][0],bat_cluster[bat_B][1],bat_cluster[bat_B][2] ])] 
                print(testing_wick)
                print(results)
                testData1=sc.parallelize(testing_wick)
                prediction=model2.predict(testData1)
                results=prediction.collect()
                print("the wickets are:",results[0])
                if results[0]>0.35:
                                if change==0:
                                     try:
                                         bat_A=batsman.pop(0)
                                     except IndexError:
                                         index_error=1
                                         break
                                     finally:
                                         change=1
                                      
                                else:
                                     try:
                                         bat_B=batsman.pop(0)
                                     except IndexError:
                                         index_error=1
                                         break
                                     finally:
                                         change=1
                if k==1:
                       if score > score_list[0]:
                                 break                         
        print 'SCORE = ',score
        score_list.append(score)
        if index_error==1:
                print 'ALL OUT'    
print("Predicted score of %s : %d"%(teams[0],score_list[0]))
print("Predicted score of %s : %d"%(teams[1],score_list[1]))                                                                          


path='output_csv/'+sys.argv[3]
#print(path)

f=open(path,'r')
data=f.read().split('\n')
data=[x.split(',') for x in data]
data.pop()

score=[0,0]
for x in data:
        if x[0]=='1st innings':
                score[0]+=int(x[6])  
                team1=x[2]      
        else:
                score[1]+=int(x[6])
                team2=x[2]
                
print("Actual score of %s : %d"%(team1,score[0]))
print("Actual score of %s : %d"%(team2,score[1]))        
