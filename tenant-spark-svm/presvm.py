#!/usr/bin/python

from __future__ import division 
import datetime
import time
import pandas as pd
from sklearn import preprocessing

import numpy as np
from sklearn import svm
from sklearn.externals import joblib
from sklearn.cross_validation import train_test_split
 
#####连sample istrain=1，训练集， istrain=-1全集预测 istrain=0 一条预测
def predata_sql(tenant,istrain):
    start=time.time()
            
    factors=['id','sample_type','create_time','update_time','mylevel','managerlevel','throw_reason','custype','province','type','terminals',
                         'callcount','visitcount','traincount','cr_lm_count','vr_lm_count']
    tenant = tenant.loc[:,factors]
    now = datetime.datetime.now()
    tenant['update_days']=now-tenant['update_time']
    tenant['update_days']=[i.days for i in tenant['update_days']]     
    tenant['create_days'] = now-tenant['create_time'] 
    tenant['create_days'] = [i.days for i in tenant['create_days']]   

    tenant_with_dummy=tenant
    dummy_factors=['mylevel','managerlevel','throw_reason','custype','province','type']
    for i in dummy_factors:
        dummies=pd.get_dummies(tenant[i],prefix=i)
        tenant_with_dummy=tenant_with_dummy.join(dummies)
        
    xy=tenant_with_dummy
    indnxt=xy.columns[(len(dummy_factors)+4):len(xy.columns)]

    X=xy.ix[:,indnxt]
    y=xy.loc[:,'sample_type']
    ##dummy pattern
    dummies= X.columns
    print ('time',time.time()-start)
    
    if istrain==1:           
        ## 预处理
        X=X.fillna(0) 
        scaler = preprocessing.StandardScaler().fit(X)
        X_scaled = scaler.transform(X)
        return X_scaled,y,scaler,dummies
    elif istrain==-1:
        sample_type=tenant.ix[:,'sample_type']
        cusid=tenant.ix[:,'id']
        return X,sample_type,cusid
    else:
        sample_type=tenant.ix[0,'sample_type']      
        return X,sample_type

def tenant_svm_train(X_scaled,y):
    ##切分数据集67%训练，33%测试  
    x_train, x_test, y_train, y_test = train_test_split(X_scaled, y, test_size = 0.33,random_state=0) 

    #模型
    start_time=time.time()
    clf = svm.SVC(kernel='linear',C=1,probability=True)

    clf.fit(x_train,y_train)

    y_pred_train=clf.predict(x_train)
    y_pred_test=clf.predict(x_test)

    clf.decision_function(x_train)
    y_proba=clf.predict_proba(X_scaled)[:,1]
    y_proba_train=clf.predict_proba(x_train)[:,1]
    y_proba_test=clf.predict_proba(x_test)[:,1]

    print("time spent:", time.time() - start_time)  
    print clf.coef_
    print "train score="
    print clf.score(x_train,y_train)
    print "test score="
    print clf.score(x_test, y_test)  

    #保存模型
    #lr是一个LogisticRegression模型
    #joblib.dump(clf, 'tenant_svm.model')
    output=pd.DataFrame(y_proba,columns=['proba'])
    return clf,output     


def tenant_svm(clf,tenant_data,scaler,dummies,types):  
    data=tenant_data.ix[:,dummies].fillna(0)
    if types==-1:     ##type=-1, data为矩阵
        scaled_data=scaler.transform(data) 
        result = clf.predict_proba(scaled_data)[:,1]
    else:
        try:
            scaled_data=scaler.transform(data) 
            result = clf.predict_proba(scaled_data)[0,1]
        except:
            result=50
    return result   

def toscore(result,sample_type):
    if sample_type==1:
        result=99
    elif sample_type==0:
        result=0
    elif result==50:
        result=result    
    else:
        result = result + np.random.uniform(0.05,0.1) #随机变量
        if result<0.1:
            result=0.1
        elif result>0.95:
            result=95
        else:
            result = int(result*100)
    print result   
    ctime=datetime.datetime.now()
    return result,ctime 

def toscore2(prob):
    result = preprocessing.minmax_scale(prob, feature_range=(10, 95), axis=0, copy=True)
    result = np.array(result,dtype=np.int)   
    ctime=datetime.datetime.now()
    return result,ctime 

