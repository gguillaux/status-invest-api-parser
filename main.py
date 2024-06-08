import requests
import pandas as pd


def get_indicator_history(codes, time, by_quarter, future_data):
    url = 'https://statusinvest.com.br/acao/indicatorhistoricallist'
    params = {
        'codes': codes,
        'time': time,
        'byQuarter': by_quarter,
        'futureData': future_data
    }
    
    headers = {
        'Accept': '*/*',
        'User-Agent': 'insomnia/9.2.0'
    }
    
    response = requests.get(url, params=params, headers=headers)
    print(response.status_code, codes)

    if response.status_code == 200:
        data = response.json()
        return data['data']
    else:
        return None



def filter_indicators_json_data(json_data):
    main_keys = ('key', 'actual_F', 'avg_F', 'avgDifference_F', 
                      'minValue_F', 'minValueRank_F', 'maxValue_F', 'maxValueRank_F')

    indicators = []
    for item in json_data:
        indicators.append({key: item[key] for key in main_keys})
    return indicators



def parse_indicator_history(symbol):
    codes = symbol
    time = 5
    by_quarter = False
    future_data = False
    

    table_data = get_indicator_history(codes, time, by_quarter, future_data)

    if table_data:
        symbol_data = next(iter(table_data))
        raw_data = table_data[symbol_data]

        data = filter_indicators_json_data(raw_data)
        df = pd.DataFrame(data)
        df['ticker'] = symbol
        return df

    else:
        print('Table not found.')
        return None


if __name__ == '__main__':
    with open('asset_list.txt', 'r') as f:
        symbols = f.read().splitlines()
    dfs = [parse_indicator_history(symbol) for symbol in symbols]
    df = pd.concat(dfs, ignore_index=True)
    print(df)
    df.to_csv('assets_indicators.csv', index=False)
