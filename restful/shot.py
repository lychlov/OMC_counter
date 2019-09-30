import pandas as pd
import os

date = '20190929'
hour = '19'


def sum_df(df):
    result_dict = {}
    for col in list(df.columns.values):
        result_dict[col] = df[col].sum()
    result_df = pd.DataFrame(result_dict, pd.Index(range(1)))
    return result_df


print('all_data start')
source = r'/home/ops/omc_counter/omc_data/{}/{}/'.format(date, hour)
all_data = []
for path in os.listdir(source):
    temp_data = pd.read_excel(source + path, header=1, usecols=[7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
    temp_data.replace("NIL", 0, inplace=True)
    all_data.append(temp_data)
all_data_map = map(sum_df, all_data)
all_sum = pd.concat(all_data_map)
target = r'/home/ops/omc_counter/omc_target/'
if not os.path.exists(target):
    os.makedirs(target)
all_sum.to_excel(target + "{}-{}-all_data.xlsx".format(date, hour))
print('all_data done')
result_dict = {}
for col in list(all_sum.columns.values):
    result_dict[col] = all_sum[col].sum()
    print(col, all_sum[col].sum())
result_df = pd.DataFrame(result_dict, pd.Index(range(1)))
result_df.to_excel(target + "{}-{}-result.xlsx".format(date, hour))
html_str = '<p>{}</p><br>' \
           '<a href="/target_file/{}-{}-all_data.xlsx">{}-{}-all_data.xlsx</a><br>' \
           '<a href="/target_file/{}-{}-result.xlsx">{}-{}-result.xlsx</a><br>' \
    .format(result_dict, date, hour, date, hour, date, hour, date, hour, )
print(html_str)
