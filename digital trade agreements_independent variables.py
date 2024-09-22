#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
import os


# In[7]:


import pandas as pd

# 指定文件路径
file_path = r'E:\闲鱼\第二部分\Gravity_csv_V202211\Gravity_V202211.csv'

DIGIT_OECD=pd.read_excel(r"E:\闲鱼\DIGIT_OECD.xlsx")
# 分列 industry 列，并删除 Subcode 列
split_columns = DIGIT_OECD['industry'].str.split('_', expand=True)
split_columns.columns = ['Country', 'Code', 'Subcode']
DIGIT_OECD = pd.concat([DIGIT_OECD.drop(columns=['industry']), split_columns], axis=1).drop(columns=['Subcode'])

# 读取CSV文件并保留特定列
columns_to_keep = ['year', 'country_id_o', 'country_id_d', 'distcap', 'contig', 'comlang_ethno', 'comrelig', 'pop_o', 'pop_d', 'gdp_o', 'gdp_d', 'fta_wto_raw']
df = pd.read_csv(file_path, usecols=columns_to_keep)
# 筛选 year 列，使其在 1995-2020 之间
df_filtered = df[(df['year'] >= 1995) & (df['year'] <= 2020)]
del df
df_filtered=df_filtered[df_filtered['country_id_o'].isin(DIGIT_OECD['Country']) | df_filtered['country_id_d'].isin(DIGIT_OECD['Country'])]


# In[5]:


# 添加后缀 "_o" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
digit_oecd_o = DIGIT_OECD.rename(columns=lambda x: x + '_o' if x not in ['year', 'Country', 'Code'] else x)

# 通过 df 的 country_id_o 和 year 与 DIGIT_OECD 的 Country 和 year 进行匹配
df_filtered = pd.merge(df_filtered, digit_oecd_o, left_on=['country_id_o', 'year'], right_on=['Country', 'year'], how='left')

# 删除不需要的列
df_filtered.drop(columns=['Country'], inplace=True)

# 添加后缀 "_d" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
digit_oecd_d = DIGIT_OECD.rename(columns=lambda x: x + '_d' if x not in ['year', 'Country', 'Code'] else x)

# 通过 df 的 country_id_d 和 year 与 DIGIT_OECD 的 Country 和 year 进行匹配
df_filtered = pd.merge(df_filtered, digit_oecd_d, left_on=['country_id_d', 'year'], right_on=['Country', 'year'], how='left')

# 删除不需要的列
df_filtered.drop(columns=['Country'], inplace=True)


# In[59]:


import pandas as pd
import os

# 指定文件路径
file_path = r'E:\闲鱼\第二部分\Gravity_csv_V202211\Gravity_V202211.csv'
output_dir = r'F:\工作\digit'

# 创建输出目录（如果不存在）
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 读取 DIGIT_OECD 表格
DIGIT_OECD = pd.read_excel(r"E:\闲鱼\DIGIT_OECD.xlsx")
# 分列 industry 列，并删除 Subcode 列
split_columns = DIGIT_OECD['industry'].str.split('_', expand=True)
split_columns.columns = ['Country', 'Code', 'Subcode']
DIGIT_OECD = pd.concat([DIGIT_OECD.drop(columns=['industry']), split_columns], axis=1).drop(columns=['Subcode'])

# 筛选 year 列，使其在 1995-2020 之间，并分块读取和处理 CSV 文件
columns_to_keep = ['year', 'country_id_o', 'country_id_d', 'distcap', 'contig', 'comlang_ethno', 'comrelig', 'pop_o', 'pop_d', 'gdp_o', 'gdp_d', 'fta_wto_raw']

chunk_size = 2000

for chunk in pd.read_csv(file_path, usecols=columns_to_keep, chunksize=chunk_size):
    # 筛选 year 列，使其在 1995-2020 之间
    chunk_filtered = chunk[(chunk['year'] >= 1995) & (chunk['year'] <= 2020)]
    
    # 筛选 country_id_o 和 country_id_d 在 DIGIT_OECD 的 Country 列中的行
    chunk_filtered = chunk_filtered[chunk_filtered['country_id_o'].isin(DIGIT_OECD['Country']) | chunk_filtered['country_id_d'].isin(DIGIT_OECD['Country'])]
    
    # 添加后缀 "_o" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    digit_oecd_o = DIGIT_OECD.rename(columns=lambda x: x + '_o' if x not in ['year', 'Country', 'Code'] else x)
    
    # 通过 df 的 country_id_o 和 year 与 DIGIT_OECD 的 Country 和 year 进行匹配
    chunk_filtered = pd.merge(chunk_filtered, digit_oecd_o, left_on=['country_id_o', 'year'], right_on=['Country', 'year'], how='left')
    
    # 删除不需要的列
    chunk_filtered.drop(columns=['Country'], inplace=True)
    
    # 添加后缀 "_d" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    digit_oecd_d = DIGIT_OECD.rename(columns=lambda x: x + '_d' if x not in ['year', 'Country', 'Code'] else x)
    
    # 通过 df 的 country_id_d 和 year 与 DIGIT_OECD 的 Country 和 year 进行匹配
    chunk_filtered = pd.merge(chunk_filtered, digit_oecd_d, left_on=['country_id_d', 'year'], right_on=['Country', 'year'], how='left')
    
    # 删除不需要的列
    chunk_filtered.drop(columns=['Country'], inplace=True)
    
    # 获取当前块中最后一行的索引数
    last_index = chunk_filtered.index[-1]
    
    # 保存处理后的块到文件
    output_file = os.path.join(output_dir, f'DIGIT_{last_index}.csv')
    chunk_filtered.to_csv(output_file, index=False)

print("处理完成，结果已保存到", output_dir)


# In[20]:


gvc=pd.read_excel(r"F:\工作\OECD-GVC数据95-20(Koopman).xlsx")

# 将 IND 列按 "_" 分列，并处理空值情况
split_columns = gvc['IND'].str.split('_', expand=True)
split_columns.columns = ['Code', 'Subcode']

# 如果 Code 列为空值，将 Subcode 的值移动到 Code 列
split_columns['Code'] = split_columns.apply(lambda row: row['Subcode'] if row['Code'] == '' else row['Code'], axis=1)
split_columns['Subcode'] = split_columns.apply(lambda row: '' if row['Code'] == row['Subcode'] else row['Subcode'], axis=1)

# 删除 IND 列，并添加新的 Code 和 Subcode 列
gvc = gvc.drop(columns=['IND']).join(split_columns)
gvc = gvc.drop(columns=['Subcode'])


# In[22]:


digit=pd.read_excel(r"F:\工作\DIGIT_OECD.xlsx")

# 分列 industry 列，并删除 Subcode 列
split_columns = digit['industry'].str.split('_', expand=True)
split_columns.columns = ['Country', 'Code', 'Subcode']
digit = pd.concat([digit.drop(columns=['industry']), split_columns], axis=1).drop(columns=['Subcode'])


# In[24]:


gvc_filtered = pd.merge(gvc, digit, left_on=['COU', 'Time','Code'], right_on=['Country', 'year','Code'], how='left') 
# 删除不需要的列
gvc_filtered.drop(columns=['Country','year'], inplace=True)


# In[11]:


num=pd.read_excel(r"F:\工作\贸易协定数字条款_删除.xlsx")
# 生成所有年份从原有年份到2020年的数据
expanded_data = []

for index, row in num.iterrows():
    for year in range(row['年份 '], 2021):
        new_row = row.copy()
        new_row['年份 '] = year
        expanded_data.append(new_row)

# 创建新的数据框
expanded_df = pd.DataFrame(expanded_data)


# In[32]:


gvc_filtered_num = pd.merge(gvc_filtered, expanded_df, left_on=['COU', 'Time'], right_on=['国家1', '年份 '], how='left')
# 删除不需要的列
gvc_filtered_num.drop(columns=['国家1', '年份 '], inplace=True)


# In[44]:


gra=pd.read_csv(r"F:\工作\Gravity_csv_V202211\Gravity_V202211.csv",usecols=columns_to_keep)


# In[50]:


gra_filtered = gra[(gra['year'] >= 1995) & (gra['year'] <= 2020)]
gra_filtered = gra_filtered[gra_filtered['country_id_o'].isin(gvc_filtered['COU']) & gra_filtered['country_id_d'].isin(gvc_filtered['COU'])] 


# In[52]:


gra_filtered.to_csv(r"F:\工作\Gravity_csv_V202211_filtered.csv",index=None)


# In[72]:


import pandas as pd
import os

# 指定文件路径
file_path = r'F:\工作\Gravity_csv_V202211_filtered.csv'
output_dir = r'F:\工作\digit'

# 创建输出目录（如果不存在）
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 筛选 year 列，使其在 1995-2020 之间，并分块读取和处理 CSV 文件
columns_to_keep = ['year', 'country_id_o', 'country_id_d', 'distcap', 'contig', 'comlang_ethno', 'comrelig', 'pop_o', 'pop_d', 'gdp_o', 'gdp_d', 'fta_wto_raw']

chunk_size = 100
no=0
for chunk in pd.read_csv(file_path,  chunksize=chunk_size):
#     # 筛选 year 列，使其在 1995-2020 之间
#     chunk_filtered = chunk[(chunk['year'] >= 1995) & (chunk['year'] <= 2020)]
    
#     # 筛选 country_id_o 和 country_id_d 在 DIGIT_OECD 的 Country 列中的行
#     chunk_filtered = chunk_filtered[chunk_filtered['country_id_o'].isin(gvc_filtered['COU']) & chunk_filtered['country_id_d'].isin(gvc_filtered['COU'])]
    
    # 添加后缀 "_o" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    digit_oecd_o = gvc_filtered.rename(columns=lambda x: x + '_o' if x not in ['COU', 'Time'] else x)
    
    # 通过 df 的 country_id_o 和 year 与 DIGIT_OECD 的 Country 和 year 进行匹配
    chunk_filtered = pd.merge(chunk, digit_oecd_o, left_on=['country_id_o', 'year'], right_on=['COU', 'Time'], how='left')
    
    # 删除不需要的列
    chunk_filtered.drop(columns=['COU','Time'], inplace=True)
    
    # 添加后缀 "_d" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    digit_oecd_d = gvc_filtered.rename(columns=lambda x: x + '_d' if x not in ['COU', 'Time'] else x)
    
    # 通过 df 的 country_id_d 和 year 与 DIGIT_OECD 的 Country 和 year 进行匹配
    chunk_filtered = pd.merge(chunk_filtered, digit_oecd_d, left_on=['country_id_d', 'year'], right_on=['COU', 'Time'], how='left')
    
    # 删除不需要的列
    chunk_filtered.drop(columns=['COU','Time'], inplace=True)
    
    # 获取当前块中最后一行的索引数
    last_index = chunk_filtered.index[-1]
    
    # 保存处理后的块到文件
    output_file = os.path.join(output_dir, f'Gravity_{no}.csv')
    chunk_filtered.to_csv(output_file, index=False)
    no=no+1
print("处理完成，结果已保存到", output_dir)


# In[63]:


chunk_filtered


# In[12]:


oecd_new=pd.read_excel(r"E:\闲鱼\ISIC_OECD.xlsx")
oecd_new


# In[4]:


# 指定文件路径
input_folder = r'F:\工作\digit'
output_folder = r'F:\工作\output'

# 获取文件列表
file_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 创建从 Old code 到 Code 的映射
code_mapping = dict(zip(oecd_new['Old code'], oecd_new['Code']))

# 初始化总的 DataFrame
total_df = None
batch_size = 5  # 每次处理的文件数

# 处理每个文件
for file in file_list:
    file_path = os.path.join(input_folder, file)
    
    # 读取 chunk_filtered 文件
    chunk_filtered = pd.read_csv(file_path)
    
    # 筛选出需要 merge 的数据
    chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_o', 'country_id_d', 'year'], right_on=['国家1', '国家2', '年份 '], how='left')
    chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_d', 'country_id_o', 'year'], right_on=['国家1', '国家2', '年份 '], how='left', suffixes=('', '_r'))
    
    # 将 '_r' 后缀的列合并到主表中，并删除 '_r' 后缀的列
    for col in expanded_df.columns:
        if col not in ['国家1', '国家2', '年份 ']:
            chunk_filtered[col] = chunk_filtered[col].combine_first(chunk_filtered[col + '_r'])
            chunk_filtered.drop(columns=[col + '_r'], inplace=True)
    
    # 删除多余的列
    chunk_filtered.drop(columns=['国家1', '国家2', '年份 ','国家1_r', '国家2_r', '年份 _r'], inplace=True)
    # 填充指定列中的空值为0
    cols_to_fill = [
        'digita_for_d', 'digitb_d', 'digitb_dom_d', 'digitb_for_d',
        '区域贸易协定电子商务和数据条款', '数据相关条款', '贸易促进条款', '信息安全条款',
        '组织运行条款', '数字贸易条款总深度指数', '是否有条款', '是否有章节'
    ]
    chunk_filtered[cols_to_fill] = chunk_filtered[cols_to_fill].fillna(0)
    # 替换 Code_o 列中的数值
    chunk_filtered['Code_o'] = chunk_filtered['Code_o'].map(code_mapping).fillna(chunk_filtered['Code_o'])
    chunk_filtered['Code_d'] = chunk_filtered['Code_d'].map(code_mapping).fillna(chunk_filtered['Code_d'])

    break
    # 保存处理后的文件
    output_file_path = os.path.join(output_folder, f'out_{file}')
    chunk_filtered.to_csv(output_file_path, index=False)

print("处理完成，文件已保存到输出路径。")


# In[5]:


chunk_filtered


# In[13]:


import os
import pandas as pd


# 创建从 Old code 到 Code 的映射
code_mapping = dict(zip(oecd_new['Old code'], oecd_new['Code']))

# 文件路径
input_folder = r'F:\工作\digit'
output_file = r'F:\工作\combined_output.csv'

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 初始化总的 DataFrame
total_df = None
batch_size = 5  # 每次处理的文件数

# 逐步处理文件
for i in range(0, len(file_list), batch_size):
    # 获取当前批次的文件列表
    batch_files = file_list[i:i+batch_size]
    batch_dfs = []
    
    for file in batch_files:
        try:
            chunk_filtered = pd.read_csv(file)
            
            # 筛选出需要 merge 的数据
            chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_o', 'country_id_d', 'year'], right_on=['国家1', '国家2', '年份 '], how='left')
            chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['country_id_d', 'country_id_o', 'year'], right_on=['国家1', '国家2', '年份 '], how='left', suffixes=('', '_r'))
            
            # 将 '_r' 后缀的列合并到主表中，并删除 '_r' 后缀的列
            for col in expanded_df.columns:
                if col not in ['国家1', '国家2', '年份 ']:
                    chunk_filtered[col] = chunk_filtered[col].combine_first(chunk_filtered[col + '_r'])
                    chunk_filtered.drop(columns=[col + '_r'], inplace=True)
            
            # 删除多余的列
            chunk_filtered.drop(columns=['国家1', '国家2', '年份 ', '国家1_r', '国家2_r', '年份 _r'], inplace=True)
            
            # 填充指定列中的空值为0
            cols_to_fill = [
                'digita_for_d', 'digitb_d', 'digitb_dom_d', 'digitb_for_d',
                '区域贸易协定电子商务和数据条款', '数据相关条款', '贸易促进条款', '信息安全条款',
                '组织运行条款', '数字贸易条款总深度指数', '是否有条款', '是否有章节'
            ]
            chunk_filtered[cols_to_fill] = chunk_filtered[cols_to_fill].fillna(0)
            
            # 替换 Code_o 列中的数值
            chunk_filtered['Code_o'] = chunk_filtered['Code_o'].map(code_mapping).fillna(chunk_filtered['Code_o'])
            chunk_filtered['Code_d'] = chunk_filtered['Code_d'].map(code_mapping).fillna(chunk_filtered['Code_d'])
            
            batch_dfs.append(chunk_filtered)
        
        except Exception as e:
            print(f"Error processing {file}: {e}")
    
    # 横向拼接当前批次的数据
    if batch_dfs:
        batch_df = pd.concat(batch_dfs, axis=1)
        
        if total_df is None:
            total_df = batch_df
        else:
            total_df = pd.concat([total_df, batch_df], axis=1)
    
    # 保存当前处理结果到输出文件，防止内存占用过多
    if total_df is not None:
        if i == 0:
            total_df.to_csv(output_file, index=False, mode='w', header=True)
        else:
            total_df.to_csv(output_file, index=False, mode='a', header=False)
        
        total_df = None  # 清空 total_df 以释放内存

print("处理完成，文件已保存到输出路径。")


# In[8]:


chunk_filtered.columns


# In[14]:


batch_df


# In[ ]:




