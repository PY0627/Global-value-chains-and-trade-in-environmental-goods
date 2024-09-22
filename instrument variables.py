#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[17]:


df1=pd.read_csv(R"G:\20240829\Fixed broadbank subscriptions per 100 people\API_IT.NET.BBND.P2_DS2_en_csv_v2_3405538.csv")
df2=pd.read_csv(r"G:\20240829\fixed telephone subscriptions\API_IT.MLT.MAIN_DS2_en_csv_v2_3448939.csv")
df3=pd.read_csv(r"G:\20240829\individuals using internet % of population\API_IT.NET.USER.ZS_DS2_en_csv_v2_3409961.csv")
df4=pd.read_csv(r"G:\20240829\mobile cellular subscriptions per 100 people\API_IT.CEL.SETS.P2_DS2_en_csv_v2_3418710.csv")


# In[18]:


df=pd.read_csv(r"G:\20240829\gvc_man.csv")


# In[19]:


ddf=df1['Country Code']
a=df1.columns
aaa=pd.DataFrame()
for i in a[4:]:
    res=df1[str(i)]
    res=pd.concat([ddf,res],axis=1)
    res['year']=str(i)
    res.columns=["Country Code","Fixed broadbank subscriptions per 100 people_o","year"]
    aaa=pd.concat([aaa,res],axis=0)
aaa=aaa[["Country Code","year","Fixed broadbank subscriptions per 100 people_o"]]
aaa['year']=aaa['year'].astype(int)
aaa['year']=aaa['year']-13
df=pd.merge(df,aaa,left_on=['exp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']
aaa.columns=["Country Code","year","Fixed broadbank subscriptions per 100 people_d"]
df=pd.merge(df,aaa,left_on=['imp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']


# In[21]:


ddf=df2['Country Code']
a=df2.columns
aaa=pd.DataFrame()
for i in a[4:]:
    res=df2[str(i)]
    res=pd.concat([ddf,res],axis=1)
    res['year']=str(i)
    res.columns=["Country Code","fixed telephone subscriptions_o","year"]
    aaa=pd.concat([aaa,res],axis=0)
aaa=aaa[["Country Code","year","fixed telephone subscriptions_o"]]
aaa['year']=aaa['year'].astype(int)
aaa['year']=aaa['year']-13
df=pd.merge(df,aaa,left_on=['exp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']
aaa.columns=["Country Code","year","fixed telephone subscriptions_d"]
df=pd.merge(df,aaa,left_on=['imp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']


# In[23]:


ddf=df3['Country Code']
a=df3.columns
aaa=pd.DataFrame()
for i in a[4:]:
    res=df3[str(i)]
    res=pd.concat([ddf,res],axis=1)
    res['year']=str(i)
    res.columns=["Country Code","individuals using internet % of population_o","year"]
    aaa=pd.concat([aaa,res],axis=0)
aaa=aaa[["Country Code","year","individuals using internet % of population_o"]]
aaa['year']=aaa['year'].astype(int)
aaa['year']=aaa['year']-13
df=pd.merge(df,aaa,left_on=['exp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']
aaa.columns=["Country Code","year","individuals using internet % of population_d"]
df=pd.merge(df,aaa,left_on=['imp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']


# In[25]:


ddf=df4['Country Code']
a=df4.columns
aaa=pd.DataFrame()
for i in a[4:]:
    res=df4[str(i)]
    res=pd.concat([ddf,res],axis=1)
    res['year']=str(i)
    res.columns=["Country Code","mobile cellular subscriptions per 100 people_o","year"]
    aaa=pd.concat([aaa,res],axis=0)
aaa=aaa[["Country Code","year","mobile cellular subscriptions per 100 people_o"]]
aaa['year']=aaa['year'].astype(int)
aaa['year']=aaa['year']-13
df=pd.merge(df,aaa,left_on=['exp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']
aaa.columns=["Country Code","year","mobile cellular subscriptions per 100 people_d"]
df=pd.merge(df,aaa,left_on=['imp',"t"],right_on=["Country Code","year"],how="left")
del df['Country Code']
del df['year']


# In[27]:


df.to_csv(r"G:\20240829\gvc_man_工具变量1.csv",index=None)


# In[70]:


df=pd.read_csv(r"G:\20240829\gvc_man_工具变量1.csv")


# In[28]:


adb_countries_regions=pd.read_csv(R"G:\20240829\adb_countries_regions.csv")


# In[36]:


adb_countries_regions.columns


# In[37]:


adb_countries_regions=adb_countries_regions[['Country Abbreviation', 'Geographic Region']]


# In[38]:


adb_countries_regions.columns=['country', 'Geographic Region']


# In[32]:


DIG_ADB=pd.read_excel(r"G:\20240829\DIG_ADB.xlsx")


# In[39]:


DIG_ADB=pd.merge(DIG_ADB,adb_countries_regions,on="country",how="left")


# In[60]:


# 计算每行的diga_region
def calculate_digb_region(row, df):
#     print(row['yeart'])
#     print(df['yeart'])
    # 筛选出与当前行相同的yeart和Geographic Region, 但sect不同的行
    filtered_df = df[(df['yeart'] == row['yeart']) & 
                     (df['Geographic Region'] == row['Geographic Region']) & 
                     (df['sect'] == row['sect']) &
                     (df['country'] != row['country'])] 
                        
    # 计算diga_dom的平均值
    return filtered_df['digb_dom'].mean()

# 应用函数来计算diga_region
DIG_ADB['digb_region'] = DIG_ADB.apply(lambda row: calculate_digb_region(row, DIG_ADB), axis=1)


# In[62]:


DIG_ADB.to_csv(r"G:\20240829\digb_region.csv",index=None)


# In[56]:


DIG_ADB.columns


# In[63]:


DIG_ADB


# In[65]:


import re
# 提取sect列中的数字
DIG_ADB['sect'] = DIG_ADB['sect'].apply(lambda x: ''.join(re.findall(r'\d+', x)))


# In[67]:


DIG_ADB_=DIG_ADB[["yeart","country","sect","digb_region"]]


# In[72]:


DIG_ADB_.columns=["t","exp","sect","digb_region_o"]


# In[76]:


DIG_ADB_['sect']=DIG_ADB_['sect'].astype(int)


# In[77]:


df=pd.merge(df,DIG_ADB_,on=["t","exp","sect"],how="left")


# In[79]:


DIG_ADB_.columns=["t","imp","sect","digb_region_d"]
df=pd.merge(df,DIG_ADB_,on=["t","imp","sect"],how="left")


# In[81]:


df.to_csv(r"G:\20240829\gvc_man_工具变量12.csv",index=None)


# In[13]:


df=pd.read_csv(r"G:\20240829\gvc_man_工具变量12.csv")
DIG_ADB=pd.read_excel(r"G:\20240829\DIG_ADB.xlsx")


# In[15]:


import re
# 提取sect列中的数字
DIG_ADB['sect'] = DIG_ADB['sect'].apply(lambda x: ''.join(re.findall(r'\d+', x)))
DIG_ADB['sect']=DIG_ADB['sect'].astype(int)


# In[16]:


DIG_ADB=DIG_ADB[['yeart', 'country', 'sect',  'diga_dom', 'diga_for', 
       'digb_dom', 'digb_for']]
DIG_ADB.columns=["t","exp","sect", 'diga_dom_o', 'diga_for_o',  'digb_dom_o', 'digb_for_o']


# In[17]:


DIG_ADB.columns


# In[18]:


df=pd.merge(df,DIG_ADB,on=["exp","t","sect"],how="left")


# In[20]:


DIG_ADB.columns=["t","imp","sect", 'diga_dom_d', 'diga_for_d',  'digb_dom_d', 'digb_for_d']
df=pd.merge(df,DIG_ADB,on=["imp","t","sect"],how="left")


# In[22]:


df.to_csv(r"G:\20240829\gvc_man_工具变量_0902.csv",index=None)


# In[23]:


df


# In[ ]:




