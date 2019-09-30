from configures.omcs import omcs
from adapters.FTPAdapter import SFTPAdapter
from sanic import Sanic
from sanic.response import json, file, html
import os
import re
import pandas as pd

app = Sanic()


def sum_df(df):
    result_dict = {}
    for col in list(df.columns.values):
        result_dict[col] = df[col].sum()
    result_df = pd.DataFrame(result_dict, pd.Index(range(1)))
    return result_df


@app.route("/")
async def test(request):
    return json({"hello": "world"})


@app.route('/file/<date>/<hour>/<file_name>')
async def handle_request(request, date, hour, file_name):
    source = r'/home/ops/omc_counter/omc_data/{}/{}/{}'.format(date, hour, file_name)
    return await file(source)


@app.route('/target_file/<file_name>')
async def target_file_request(request, file_name):
    source = r'/home/ops/omc_counter/omc_target/{}'.format(file_name)
    return await file(source)


@app.route('/files/<date>/<hour>')
async def show_files(request, date, hour):
    source = r'/home/ops/omc_counter/omc_data/{}/{}'.format(date, hour)
    a = "<a href='/file/{}/{}/{}'>{}</a><br>\n"
    html_str = ''
    for file_name in os.listdir(source):
        html_str += a.format(date, hour, file_name, file_name)
    return html(html_str)


@app.route('/download/<date>/<hour>')
async def get_files(request, date, hour):
    pattern = r'.*2019SYBZ-HW.*?-{}-{}.xlsx'.format(date, hour)
    source = r'/home/ops/omc_counter/omc_data/{}/{}/'.format(date, hour)
    html_str = ''
    a = "<a href='/file/{}/{}/{}'>{}</a><br>"
    remote_dir = '/opt/PRS/server/var/prs/result_file/20191001BZ'
    if not os.path.exists(source):
        os.makedirs(source)
    for key, item in omcs.items():
        ftp_a = SFTPAdapter(item['ip'], item['user'], item['password'])
        files = ftp_a.get_all_files(item['path'])
        for file_name in files:
            if re.match(pattern, file_name):
                html_str += a.format(date, hour, file_name.split('/')[-1], file_name.split('/')[-1])
                ftp_a.sftp_get(file_name, source + file_name.split('/')[-1])
                print(source + file_name.split('/')[-1])
    return html(html_str)


@app.route('/result/<date>/<hour>')
async def get_result(request, date, hour):
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
    return html(html_str)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
