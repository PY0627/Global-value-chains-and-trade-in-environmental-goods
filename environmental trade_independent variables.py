#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os


# In[11]:


import os
import pandas as pd

# 定义文件路径
folder_path = r'E:\闲鱼\HSCODE分别整合'

# 获取文件列表
file_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# 初始化一个空的数据框
combined_df = pd.DataFrame()

# 读取每个文件，替换第一列为文件名（去除后缀），并拼接到一起
for file in file_list:
    df = pd.read_csv(file)  # 根据实际文件格式调整读取方式
    file_name = os.path.splitext(os.path.basename(file))[0]  # 获取文件名，不包括后缀
    df.iloc[:, 0] = file_name  # 将第一列替换为文件名（无后缀）
    combined_df = pd.concat([combined_df, df], ignore_index=True)
combined_df.columns=['Name',"HSCODE"]
# 打印合并后的数据框
print(combined_df)


# In[14]:


# 定义文件路径
folder_path = r'E:\闲鱼\BACI_HS92_V202401b'

# 获取文件列表
file_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# 初始化一个空的数据框
final_df = pd.DataFrame()

# 遍历每个文件并筛选
for file in file_list:
    df = pd.read_csv(file)  # 根据实际文件格式调整读取方式
    
    # 筛选第四列包含combined_df第二列代码的行
    filtered_df = df[df.iloc[:, 3].isin(combined_df['HSCODE'])]
    
    # 如果有筛选出的行，将combined_df的第一列添加到最后一列
    if not filtered_df.empty:
        filtered_df['Source'] = combined_df.set_index('HSCODE').loc[filtered_df.iloc[:, 3], 'Name'].values
    
    # 拼接到最终的数据框
    final_df = pd.concat([final_df, filtered_df], ignore_index=True)

# 打印最终合并后的数据框
print(final_df)


# In[17]:


# 定义存储路径
output_folder = r'E:\闲鱼\HSCODE整合后'

# 如果输出文件夹不存在，创建它
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 按照 Source 列分组并保存为不同的 CSV 文件
for source, group_df in final_df.groupby('Source'):
    # 删除 Source 列
    group_df = group_df.drop(columns=['Source'])
    
    # 创建文件名，去掉特殊字符以确保文件名有效
    file_name = ''.join(e for e in source if e.isalnum() or e.isspace()).replace(' ', '_') + '.csv'
    file_path = os.path.join(output_folder, file_name)
    
    # 保存为 CSV 文件
    group_df.to_csv(file_path, index=False)

print("文件已保存到指定目录。")


# In[18]:


HS_ISIC=pd.read_csv(r"E:\闲鱼\HS and ISIC conversion table@3.csv")


# In[20]:


# 定义文件夹路径
input_folder = r'E:\闲鱼\HSCODE整合后'
output_folder = r'E:\闲鱼\HSCODE行业'

# 如果输出文件夹不存在，创建它
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 处理每个文件
for file in file_list:
    # 读取文件
    HS_df = pd.read_csv(file)
    
    # 匹配第四列并添加 ISIC 数据
    HS_df['ISIC Rev. 4'] = HS_df.iloc[:, 3].map(HS_ISIC.set_index('HS6')['ISIC Rev. 4'])
    
    # 生成输出文件名
    base_name = os.path.splitext(os.path.basename(file))[0]
    output_file = os.path.join(output_folder, base_name + '_ISIC.csv')
    
    # 保存处理后的文件
    HS_df.to_csv(output_file, index=False)

print("处理完成，文件已保存到指定目录。")


# In[10]:


ISIC_OECD=pd.read_excel(r"E:\闲鱼\OECD·ADB·WIOD国家行业分类表.xlsx",sheet_name="OECD_行业")


# In[11]:


# 将 ISIC Rev.4 列按“,”分割，并展开为多行
ISIC_OECD_expanded = ISIC_OECD.assign(**{
    'ISIC Rev.4': ISIC_OECD['ISIC Rev.4'].str.split(', ')
}).explode('ISIC Rev.4')

print(ISIC_OECD_expanded)


# In[12]:


# 定义文件夹路径
input_folder = r'E:\闲鱼\HSCODE行业'
output_folder = r'E:\闲鱼\HSCODE_OECD'

# 如果输出文件夹不存在，创建它
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 处理每个文件
for file in file_list:
    # 读取文件
    ISIC_df = pd.read_csv(file)
    
    # 使得 ISIC_df 的第七列数据仅保留前2个字符
    ISIC_df.iloc[:, 6] = ISIC_df.iloc[:, 6].astype(str).str[:2]
    
    # 匹配 ISIC_df 第七列的数据，若 ISIC_OECD_expanded 的第三列包含 ISIC_df 第七列的数据
    ISIC_df['Code'] = ISIC_df.iloc[:, 6].map(ISIC_OECD_expanded.set_index('ISIC Rev.4')['NEW Code'])
    
    # 生成输出文件名
    base_name = os.path.splitext(os.path.basename(file))[0]
    output_file = os.path.join(output_folder, base_name + '_OECD.csv')
    
    # 保存处理后的文件
    ISIC_df.to_csv(output_file, index=False)

print("处理完成，文件已保存到指定目录。")


# In[45]:


import os
import pandas as pd

# 定义输入文件夹路径
input_folder = r'E:\闲鱼\HSCODE_OECD'

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 初始化一个空的 DataFrame 用于存储统计结果
summary_df = pd.DataFrame(columns=['Filename', 'HS_Code', 'Count'])

# 处理每个文件
for file in file_list:
    # 读取文件
    ISIC_df = pd.read_csv(file)
    
    # 筛选出第七列为空值的行
    filtered_df = ISIC_df[ISIC_df.iloc[:, 7].isna()]
    
    # 根据第四列统计数量
    count_series = filtered_df.iloc[:, 3].value_counts()
    
    # 将统计结果添加到 summary_df
    for hs_code, count in count_series.items():
        summary_df = summary_df.append({'Filename': os.path.basename(file), 'HS_Code': hs_code, 'Count': count}, ignore_index=True)

# 保存统计结果到一个新的文件
summary_output_file = os.path.join(input_folder, 'summary.csv')
summary_df.to_csv(summary_output_file, index=False)

print("处理完成，统计结果已保存到 summary.csv 文件中。")


# In[13]:


# 定义输入文件夹路径
input_folder = r'E:\闲鱼\HSCODE_OECD'
output_folder = r'E:\闲鱼\HSCODE_OECD'
# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 定义需要替换的值和对应的新值
replace_values = {
    '871419': 'C30',
    '871411': 'C30',
    '441830': 'C16',
    '732183': 'C27',
    '732113': 'C27'
}

# 处理每个文件
for file in file_list:
    # 读取文件
    ISIC_df = pd.read_csv(file)
    
    # 替换第四列特定值的第八列值
    for old_value, new_value in replace_values.items():
        ISIC_df.loc[ISIC_df.iloc[:, 3] == int(old_value), ISIC_df.columns[7]] = new_value
    
    # 保存处理后的文件
    ISIC_df.to_csv(file, index=False)

print("处理完成，文件已更新。")


# In[16]:


country_code=pd.read_csv(r"E:\闲鱼\BACI其他\country_codes_V202401b.csv")
# 定义输入文件夹路径
input_folder = r'E:\闲鱼\HSCODE_OECD'
output_folder = r'E:\闲鱼\HSCODE_国家'

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 处理每个文件
for file in file_list:
    # 读取文件
    ISIC_df = pd.read_csv(file)
    
    # 删除“ISIC Rev. 4”列
    if 'ISIC Rev. 4' in ISIC_df.columns:
        ISIC_df = ISIC_df.drop(columns=['ISIC Rev. 4'])
    
    # 匹配第二列的值与 country_code 第一列的值
    ISIC_df['output'] = ISIC_df.iloc[:, 1].map(country_code.set_index('country_code')['country_iso3'])
    
    # 匹配第三列的值与 country_code 第一列的值
    ISIC_df['input'] = ISIC_df.iloc[:, 2].map(country_code.set_index('country_code')['country_iso3'])
    
    # 生成输出文件名
    base_name = os.path.splitext(os.path.basename(file))[0]
    output_file = os.path.join(output_folder, base_name + '_国家.csv')
    # 保存处理后的文件
    ISIC_df.to_csv(output_file, index=False)
    
print("处理完成。")


# In[55]:



# 定义输入文件夹路径
input_folder = r'E:\闲鱼\HSCODE_国家'

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 初始化一个空的 DataFrame 用于存储统计结果
summary_df = pd.DataFrame(columns=['Filename', 'First_Column_Value', 'Count'])

# 处理每个文件
for file in file_list:
    # 读取文件
    ISIC_df = pd.read_csv(file)
    
    # 筛选出包含空值的行
    filtered_df = ISIC_df[ISIC_df.isna().any(axis=1)]
    
    # 根据第一列数值进行统计
    if not filtered_df.empty:
        count_series = filtered_df.iloc[:, 0].value_counts()
        
        # 将统计结果添加到 summary_df
        for value, count in count_series.items():
            summary_df = summary_df.append({'Filename': os.path.basename(file), 'First_Column_Value': value, 'Count': count}, ignore_index=True)

# 保存统计结果到一个新的文件
summary_output_file = os.path.join(input_folder, 'summary.csv')
# summary_df.to_csv(summary_output_file, index=False)

print("处理完成，统计结果已保存到 summary 文件中。")


# In[29]:


DIGIT_OECD=pd.read_excel(r"E:\闲鱼\DIGIT_OECD.xlsx")
# 分列 industry 列，并删除 Subcode 列
split_columns = DIGIT_OECD['industry'].str.split('_', expand=True)
split_columns.columns = ['Country', 'Code', 'Subcode']
DIGIT_OECD = pd.concat([DIGIT_OECD.drop(columns=['industry']), split_columns], axis=1).drop(columns=['Subcode'])

# 定义输入文件夹路径和输出文件夹路径
input_folder = r'E:\闲鱼\HSCODE_国家'
output_folder = r'E:\闲鱼\HSCODE_dig'

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 处理每个文件
for file in file_list:
    # 读取文件
    OECD_df = pd.read_csv(file)
    
    # 删除 'i' 和 'j' 列
    OECD_df = OECD_df.drop(columns=['i', 'j'])
    
    # 添加后缀 "_o" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    digit_oecd_o = DIGIT_OECD.rename(columns=lambda x: x + '_o' if x not in ['year', 'Country',"Code"] else x)
    
    # 匹配 t、output、Code 并拼接数据
    merged_df_out = pd.merge(OECD_df, digit_oecd_o, left_on=['t', 'output', 'Code'], right_on=['year', 'Country', 'Code'], how='left')
    merged_df_out = merged_df_out.drop(columns=['year', 'Country'])
    
    # 添加后缀 "_d" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    digit_oecd_d = DIGIT_OECD.rename(columns=lambda x: x + '_d' if x not in ['year', 'Country',"Code"] else x)
    
    # 匹配 t、input、Code 并拼接数据，列名后加 _in
    merged_df_in = pd.merge(merged_df_out, digit_oecd_d, left_on=['t', 'input', 'Code'], right_on=['year', 'Country', 'Code'], how='left')
    merged_df_in = merged_df_in.drop(columns=['year', 'Country'])
    
    #匹配DIGIT数据
    merged_df_in=merged_df_in[merged_df_in['output'].isin(DIGIT_OECD['Country']) & merged_df_in['input'].isin(DIGIT_OECD['Country'])]
    merged_df_in = merged_df_in[(merged_df_in['t'] >= 1995) & (merged_df_in['t'] <= 2020)]
    merged_df_in = merged_df_in.drop(columns=['k'])

    # 生成新的文件名
    base_name = os.path.basename(file)
    new_file_name = os.path.splitext(base_name)[0] + '_dig.csv'
    output_path = os.path.join(output_folder, new_file_name)
    
    # 保存处理后的文件
    merged_df_in.to_csv(output_path, index=False)
    
print("处理完成，文件已更新。")


# In[37]:


gvc=pd.read_excel(r"F:\工作\OECD-GVC数据95-20(Koopman).xlsx")

num=pd.read_excel(r"F:\工作\贸易协定数字条款_删除.xlsx")
# 生成所有年份从原有年份到2020年的数据
expanded_data = []

# 筛选 year 列，使其在 1995-2020 之间，并分块读取和处理 CSV 文件
columns_to_keep = ['year', 'country_id_o', 'country_id_d', 'distcap', 'contig', 'comlang_ethno', 'comrelig', 'pop_o', 'pop_d', 'gdp_o', 'gdp_d', 'fta_wto_raw']
gra=pd.read_csv(r"F:\工作\Gravity_csv_V202211\Gravity_V202211.csv",usecols=columns_to_keep)

for index, row in num.iterrows():
    for year in range(row['年份 '], 2021):
        new_row = row.copy()
        new_row['年份 '] = year
        expanded_data.append(new_row)

# 创建新的数据框
expanded_df = pd.DataFrame(expanded_data)

# 定义输入文件夹路径和输出文件夹路径
input_folder = r'E:\闲鱼\HSCODE_dig'
output_folder = r'E:\闲鱼\HSCODE_output'

# 获取文件列表
file_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 处理每个文件
for file in file_list:
    # 读取文件
    dig_df = pd.read_csv(file)
    # 添加后缀 "_o" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    gvc_o = gvc.rename(columns=lambda x: x + '_o' if x not in ['COU', 'IND',"Time"] else x)
    
    # 匹配 t、output、Code 并拼接数据
    merged_df_out = pd.merge(dig_df, gvc_o, left_on=['t', 'output', 'Code'], right_on=["Time",'COU', 'IND'], how='left')
    merged_df_out = merged_df_out.drop(columns=["Time",'COU', 'IND'])
    
    # 添加后缀 "_o" 到 DIGIT_OECD 除 year, Country, Code 列以外的列名
    gvc_d = gvc.rename(columns=lambda x: x + '_d' if x not in ['COU', 'IND',"Time"] else x)
    
    # 匹配 t、output、Code 并拼接数据
    merged_df_out = pd.merge(merged_df_out, gvc_d, left_on=['t', 'output', 'Code'], right_on=["Time",'COU', 'IND'], how='left')
    merged_df_out = merged_df_out.drop(columns=["Time",'COU', 'IND'])
    
    #匹配数字协议
    # 筛选出需要 merge 的数据
    chunk_filtered = pd.merge(merged_df_out, expanded_df, left_on=['output', 'input', 't'], right_on=['国家1', '国家2', '年份 '], how='left')
    chunk_filtered = pd.merge(chunk_filtered, expanded_df, left_on=['input', 'output', 't'], right_on=['国家1', '国家2', '年份 '], how='left', suffixes=('', '_r'))
    
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

    chunk_filtered = pd.merge(chunk_filtered, gra, left_on=['input', 'output', 't'], right_on=['country_id_o', 'country_id_d', 'year'], how='left')
    chunk_filtered = chunk_filtered.drop(columns=['country_id_o', 'country_id_d', 'year'])
    
    # 生成新的文件名
    base_name = os.path.basename(file)
    new_file_name = os.path.splitext(base_name)[0] + '.csv'
    output_path = os.path.join(output_folder, new_file_name)
    
    # 保存处理后的文件
    chunk_filtered.to_csv(output_path, index=False)
print("处理完成！")


# In[2]:


# 指定文件夹路径
input_folder = r'E:\闲鱼\HSCODE_output'
output_file = r'F:\工作\HSCODE_combined_output.csv'

# 获取文件列表
file_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

# 初始化一个空的数据框，用于存储拼接后的数据
combined_df = pd.DataFrame()

# 处理每个文件
for file in file_list:
    file_path = os.path.join(input_folder, file)
    
    # 读取当前文件
    current_df = pd.read_csv(file_path)
    
    # 将当前文件的数据拼接到 combined_df 中
    combined_df = pd.concat([combined_df, current_df], ignore_index=True)

# 将拼接后的数据保存到输出文件
combined_df.to_csv(output_file, index=False)

print("所有文件已拼接并保存到输出文件。")


# In[ ]:




