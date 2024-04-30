import pandas as pd
import requests
from pyecharts import options as opts
from pyecharts.charts import WordCloud
from pyecharts.charts import Bar
from pyecharts.charts import Geo
from pyecharts.charts import Scatter
from pyecharts.charts import Map

API_KEYS = ['f6e3c70955315533270eade055626bd9','589ae89e034692656e66f2e728c3df0d'] #单账号配额5000

def request_with_api_key(location, api_key):
    location_str = "{},{}".format(location[0], location[1])
    url = 'https://restapi.amap.com/v3/geocode/regeo?'
    params = {
        'location': location_str,
        'key': api_key,
        'extensions': 'base',
        'output': 'JSON',
        'roadlevel': 0,
    }
    r = requests.get(url, params=params)
    data = r.json().get('regeocode', {})
    city = data.get('addressComponent', {}).get('city', '')
    province = data.get('addressComponent', {}).get('province', '')
    if not city:
        city = province
    return city, province

def latitude_longitude_conversion(df, api_keys, output_file):
    counter = 0
    city_province_list = []  # 存储城市和省份的临时列表
    try:
        for location in df[['lon', 'lat']].values:
            for api_key in api_keys:
                city, province = request_with_api_key(location, api_key)
                if city or province:  # 如果成功获取到城市或省份信息，则跳出循环
                    break
                else:
                    print('null')
            counter += 1
            print(counter)
            city_province_list.append((city, province))

            # 每处理100条数据就保存一次中间状态
            if counter % 100 == 0:
                temp_df = df.iloc[:counter]  # 仅保存已处理的部分数据
                temp_df['city'] = [item[0] for item in city_province_list]
                temp_df['province'] = [item[1] for item in city_province_list]
                temp_df.to_csv(output_file, index=False)

    except Exception as e:
        print(f"Error occurred at index {counter}: {e}")
        print("Saving intermediate state...")
        temp_df = df.iloc[:counter]  # 保存已处理的部分数据
        temp_df['city'] = [item[0] for item in city_province_list]
        temp_df['province'] = [item[1] for item in city_province_list]
        temp_df.to_csv(output_file, index=False)
        print("Intermediate state saved.")

    finally:
        cities, provinces = zip(*city_province_list)  # 解压城市和省份列表
        df['city'] = cities  # 添加城市列
        df['province'] = provinces  # 添加省份列
        df.to_csv(output_file, index=False)  # 最终保存整个数据集
        print("Final state saved.")

    return df

data = pd.read_excel("./china.xls")
data.columns = ["id", "date", "lon", "lat", "depth", "type", "level", "loc", "incident"]

print(data.shape, "\n", data.dtypes)
x = data.isnull().sum().sum()
# print('共有NaN:', x)
x = data.duplicated().sum()
# print('共有重复行:', x)

data = latitude_longitude_conversion(data, API_KEYS,'china_new_tmp.csv')
data.to_csv('china_new.csv', index=False)
