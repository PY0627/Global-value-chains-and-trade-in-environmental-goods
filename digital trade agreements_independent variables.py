#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
import os


# In[7]:


import pandas as pd

# specify file path
file_path = r'E:\Gravity_csv_V202211\Gravity_V202211.csv'

DIGIT_OECD=pd.read_excel(r"E:\DIGIT_OECD.xlsx")

# split industry ，and delete Subcode column
split_columns = DIGIT_OECD['industry'].str.split('_', expand=True)
split_columns.columns = ['Country', 'Code', 'Subcode']
DIGIT_OECD = pd.concat([DIGIT_OECD.drop(columns=['industry']), split_columns], axis=1).drop(columns=['Subcode'])

# Read a CSV file and retain specific columns
columns_to_keep = ['year', 'country_id_o', 'country_id_d', 'distcap', 'contig', 'comlang_ethno', 'comrelig', 'pop_o', 'pop_d', 'gdp_o', 'gdp_d', 'fta_wto_raw']
df = pd.read_csv(file_path, usecols=columns_to_keep)
# Filter the 'year' column to include values between 1995 and 2020
df_filtered = df[(df['year'] >= 1995) & (df['year'] <= 2020)]
del df
df_filtered=df_filtered[df_filtered['country_id_o'].isin(DIGIT_OECD['Country']) | df_filtered['country_id_d'].isin(DIGIT_OECD['Country'])]


# In[5]:


# add the suffix '_o' to the column names of DIGIT_OECD, except for the columns year, Country, and Code
digit_oecd_o = DIGIT_OECD.rename(columns=lambda x: x + '_o' if x not in ['year', 'Country', 'Code'] else x)

# Match df's country_id_o and year with DIGIT_OECD's Country and year
df_filtered = pd.merge(df_filtered, digit_oecd_o, left_on=['country_id_o', 'year'], right_on=['Country', 'year'], how='left')

# Delete unnecessary columns
df_filtered.drop(columns=['Country'], inplace=True)

# Add the suffix '_d' to the column names of DIGIT_OECD, except for the columns year, Country, and Code
digit_oecd_d = DIGIT_OECD.rename(columns=lambda x: x + '_d' if x not in ['year', 'Country', 'Code'] else x)

# Match df's country_id_d and year with DIGIT_OECD's Country and year
df_filtered = pd.merge(df_filtered, digit_oecd_d, left_on=['country_id_d', 'year'], right_on=['Country', 'year'], how='left')

# delete unnecessary columns
df_filtered.drop(columns=['Country'], inplace=True)


# In[59]:


import pandas as pd
import os

# Specify file path
file_path = r'E:\Gravity_csv_V202211\Gravity_V202211.csv'
output_dir = r'F:\工作\digit'

# 
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


DIGIT_OECD = pd.read_excel(r"E:\DIGIT_OECD.xlsx")
# split industry columns
split_columns = DIGIT_OECD['industry'].str.split('_', expand=True)
split_columns.columns = ['Country', 'Code', 'Subcode']
DIGIT_OECD = pd.concat([DIGIT_OECD.drop(columns=['industry']), split_columns], axis=1).drop(columns=['Subcode'])

# Filter the year column to include values between 1995 and 2020, and read and process the CSV file in chunks.
columns_to_keep = ['year', 'country_id_o', 'country_id_d', 'distcap', 'contig', 'comlang_ethno', 'comrelig', 'pop_o', 'pop_d', 'gdp_o', 'gdp_d', 'fta_wto_raw']

chunk_size = 2000

for chunk in pd.read_csv(file_path, usecols=columns_to_keep, chunksize=chunk_size):
    # Filter the year column to include values between 1995 and 2020
    chunk_filtered = chunk[(chunk['year'] >= 1995) & (chunk['year'] <= 2020)]
    
    # Filter rows where country_id_o and country_id_d are in the Country column of DIGIT_OECD
    chunk_filtered = chunk_filtered[chunk_filtered['country_id_o'].isin(DIGIT_OECD['Country']) | chunk_filtered['country_id_d'].isin(DIGIT_OECD['Country'])]
    
    # Add the suffix '_o' to the column names of DIGIT_OECD, except for the columns year, Country, and Code
    digit_oecd_o = DIGIT_OECD.rename(columns=lambda x: x + '_o' if x not in ['year', 'Country', 'Code'] else x)
    
    # Match df's country_id_o and year with DIGIT_OECD's Country and year
    chunk_filtered = pd.merge(chunk_filtered, digit_oecd_o, left_on=['country_id_o', 'year'], right_on=['Country', 'year'], how='left')
    
    # delete unnecessary columns
    chunk_filtered.drop(columns=['Country'], inplace=True)
    
    # Add the suffix '_d' to the column names of DIGIT_OECD, except for the columns year, Country, and Code
    digit_oecd_d = DIGIT_OECD.rename(columns=lambda x: x + '_d' if x not in ['year', 'Country', 'Code'] else x)
    
    # Match df's country_id_d and year with DIGIT_OECD's Country and year
    chunk_filtered = pd.merge(chunk_filtered, digit_oecd_d, left_on=['country_id_d', 'year'], right_on=['Country', 'year'], how='left')
    
    # Remove unnecessary columns
    chunk_filtered.drop(columns=['Country'], inplace=True)
    
    # Get the index of the last row in the current chunk
    last_index = chunk_filtered.index[-1]
    
    # Save the processed chunk to a file
    output_file = os.path.join(output_dir, f'DIGIT_{last_index}.csv')
    chunk_filtered.to_csv(output_file, index=False)

print("results have been saved to", output_dir)


# In[20]:


gvc=pd.read_excel(r"F:\工作\OECD-GVC95-20(Koopman).xlsx")

# Split the IND column by "_" and handle missing values.
split_columns = gvc['IND'].str.split('_', expand=True)
split_columns.columns = ['Code', 'Subcode']

# If the Code column contains missing values, move the value from the Subcode column to the Code column.
split_columns['Code'] = split_columns.apply(lambda row: row['Subcode'] if row['Code'] == '' else row['Code'], axis=1)
split_columns['Subcode'] = split_columns.apply(lambda row: '' if row['Code'] == row['Subcode'] else row['Subcode'], axis=1)

# Delete the IND column and add new Code and Subcode columns.
gvc = gvc.drop(columns=['IND']).join(split_columns)
gvc = gvc.drop(columns=['Subcode'])


# In[22]:


digit=pd.read_excel(r"F:\DIGIT_OECD.xlsx")


split_columns = digit['industry'].str.split('_', expand=True)
split_columns.columns = ['Country', 'Code', 'Subcode']
digit = pd.concat([digit.drop(columns=['industry']), split_columns], axis=1).drop(columns=['Subcode'])


# In[24]:

gvc_filtered = pd.merge(gvc, digit, left_on=['COU', 'Time','Code'], right_on=['Country', 'year','Code'], how='left') 

gvc_filtered.drop(columns=['Country','year'], inplace=True)


# In[11]:


num=pd.read_excel(r"F:\data.xlsx")

expanded_data = []

for index, row in num.iterrows():
    for year in range(row['年份 '], 2021):
        new_row = row.copy()
        new_row['年份 '] = year
        expanded_data.append(new_row)


expanded_df = pd.DataFrame(expanded_data)


# In[32]:


gvc_filtered_num = pd.merge(gvc_filtered, expanded_df, left_on=['COU', 'Time'], right_on=['country1', 'year '], how='left')

gvc_filtered_num.drop(columns=['country1', 'year'], inplace=True)


# In[44]:


gra=pd.read_csv(r"F:\Gravity_csv_V202211\Gravity_V202211.csv",usecols=columns_to_keep)


# In[50]:


gra_filtered = gra[(gra['year'] >= 1995) & (gra['year'] <= 2020)]
gra_filtered = gra_filtered[gra_filtered['country_id_o'].isin(gvc_filtered['COU']) & gra_filtered['country_id_d'].isin(gvc_filtered['COU'])] 


# In[52]:


gra_filtered.to_csv(r"F:\工作\Gravity_csv_V202211_filtered.csv",index=None)


# In[72]:


import pandas as pd
import os


file_path = r'F:\Gravity_csv_V202211_filtered.csv'
output_dir = r'F:\工作\digit'


if not os.path.exists(output_dir):
    os.makedirs(output_dir)


columns_to_keep = ['year', 'country_id_o', 'country_id_d', 'distcap', 'contig', 'comlang_ethno', 'comrelig', 'pop_o', 'pop_d', 'gdp_o', 'gdp_d', 'fta_wto_raw']

chunk_size = 100
no=0
for chunk in pd.read_csv(file_path,  chunksize=chunk_size):

#     chunk_filtered = chunk[(chunk['year'] >= 1995) & (chunk['year'] <= 2020)]
    

#     chunk_filtered = chunk_filtered[chunk_filtered['country_id_o'].isin(gvc_filtered['COU']) & chunk_filtered['country_id_d'].isin(gvc_filtered['COU'])]
    

    digit_oecd_o = gvc_filtered.rename(columns=lambda x: x + '_o' if x not in ['COU', 'Time'] else x)
  
    chunk_filtered = pd.merge(chunk, digit_oecd_o, left_on=['country_id_o', 'year'], right_on=['COU', 'Time'], how='left')
    

    chunk_filtered.drop(columns=['COU','Time'], inplace=True)
    
   
    digit_oecd_d = gvc_filtered.rename(columns=lambda x: x + '_d' if x not in ['COU', 'Time'] else x)
    

    chunk_filtered = pd.merge(chunk_filtered, digit_oecd_d, left_on=['country_id_d', 'year'], right_on=['COU', 'Time'], how='left')
    

    chunk_filtered.drop(columns=['COU','Time'], inplace=True)
    
 
    last_index = chunk_filtered.index[-1]
    

    output_file = os.path.join(output_dir, f'Gravity_{no}.csv')
    chunk_filtered.to_csv(output_file, index=False)
    no=no+1
print("save results", output_dir)


# In[63]:


chunk_filtered


# In[12]:


oecd_new=pd.read_excel(r"E:\ISIC_OECD.xlsx")
oecd_new


# In[4]:



input_folder = r'F:\digit'
output_folder = r'F:\output'


file_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]


code_mapping = dict(zip(oecd_new['Old code'], oecd_new['Code']))


total_df = None
batch_size = 5  


for file in file_list:
    file_path = os.path.join(input_folder, file)
    

    chunk_filtered = pd.read_csv(file_path)
    
  
    chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_o', 'country_id_d', 'year'], right_on=['country1', 'country2', 'year '], how='left')
    chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_d', 'country_id_o', 'year'], right_on=['country1', 'country2', 'year '], how='left', suffixes=('', '_r'))
    
  
    for col in expanded_df.columns:
        if col not in ['country1', 'country2', 'year ']:
            chunk_filtered[col] = chunk_filtered[col].combine_first(chunk_filtered[col + '_r'])
            chunk_filtered.drop(columns=[col + '_r'], inplace=True)
    

    chunk_filtered.drop(columns=['country1', 'country2', 'year','country1_r', 'country2_r', 'year_r'], inplace=True)
  
    cols_to_fill = [
        'digita_for_d', 'digitb_d', 'digitb_dom_d', 'digitb_for_d',
        'digital_provisions', 'data_related provision', 'trade_facilitation_provisions', 'information_security_provisions',
        'facilitation_provision', 'digital_dep', 'digital_provisions', 'digital_chapters'
    ]
    chunk_filtered[cols_to_fill] = chunk_filtered[cols_to_fill].fillna(0)
    
    chunk_filtered['Code_o'] = chunk_filtered['Code_o'].map(code_mapping).fillna(chunk_filtered['Code_o'])
    chunk_filtered['Code_d'] = chunk_filtered['Code_d'].map(code_mapping).fillna(chunk_filtered['Code_d'])

    break
 
    output_file_path = os.path.join(output_folder, f'out_{file}')
    chunk_filtered.to_csv(output_file_path, index=False)

print("end。")


# In[5]:


chunk_filtered


# In[13]:


import os
import pandas as pd



code_mapping = dict(zip(oecd_new['Old code'], oecd_new['Code']))


input_folder = r'F:\工作\digit'
output_file = r'F:\工作\combined_output.csv'


file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]


total_df = None
batch_size = 5  


for i in range(0, len(file_list), batch_size):

    batch_files = file_list[i:i+batch_size]
    batch_dfs = []
    
    for file in batch_files:
        try:
            chunk_filtered = pd.read_csv(file)
            
           
            chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_o', 'country_id_d', 'year'], right_on=['country1', 'country2', 'year'], how='left')
            chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_d', 'country_id_o', 'year'], right_on=['country1', 'country2', 'year '], how='left', suffixes=('', '_r'))
            
          
            for col in expanded_df.columns:
                if col not in ['country1', 'country2', 'year ']:
                    chunk_filtered[col] = chunk_filtered[col].combine_first(chunk_filtered[col + '_r'])
                    chunk_filtered.drop(columns=[col + '_r'], inplace=True)
            
            # 删除多余的列
            chunk_filtered.drop(columns=['country1', 'country2', 'year ', 'country1_r', 'country2_r', 'year_r'], inplace=True)
            
            # 填充指定列中的空值为0
            cols_to_fill = [
                'digita_for_d', 'digitb_d', 'digitb_dom_d', 'digitb_for_d',
        'digital_provisions', 'data_related provision', 'trade_facilitation_provisions', 'information_security_provisions',
        'facilitation_provision', 'digital_dep', 'digital_provisions', 'digital_chapters'
            ]
            chunk_filtered[cols_to_fill] = chunk_filtered[cols_to_fill].fillna(0)
            
           
            chunk_filtered['Code_o'] = chunk_filtered['Code_o'].map(code_mapping).fillna(chunk_filtered['Code_o'])
            chunk_filtered['Code_d'] = chunk_filtered['Code_d'].map(code_mapping).fillna(chunk_filtered['Code_d'])
            
            batch_dfs.append(chunk_filtered)
        
        except Exception as e:
            print(f"Error processing {file}: {e}")
    

    if batch_dfs:
        batch_df = pd.concat(batch_dfs, axis=1)
        
        if total_df is None:
            total_df = batch_df
        else:
            total_df = pd.concat([total_df, batch_df], axis=1)
    
   
    if total_df is not None:
        if i == 0:
            total_df.to_csv(output_file, index=False, mode='w', header=True)
        else:
            total_df.to_csv(output_file, index=False, mode='a', header=False)
        
        total_df = None  

print("end。")


# In[8]:


chunk_filtered.columns


# In[14]:


batch_df


# In[ ]:




