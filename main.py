import sys
import requests
import requests_cache
import pandas as pd



if len(sys.argv) == 1:
    ASSETS_FILE = 'eletricas.txt'
else:
    ASSETS_FILE = sys.argv[1]

if len(sys.argv) < 3:
    HEAD_COUNT = 5
else:
    HEAD_COUNT = int(sys.argv[2])

requests_cache.install_cache('statusinvest_cache', backend='sqlite', expire_after=3600)


def get_indicator_history(codes, time, by_quarter, future_data):
    if codes[-2:] in ('31', '32', '33', '34', '35', '36', '37', '38', '39'):
        url = 'https://statusinvest.com.br/bdr/indicatorhistoricallist'
    else:
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
    main_keys = ('key', 'actual', 'actual_F', 'avg_F', 'avgDifference', 
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


def systematic_filters(df):
    # SPECIFY IF YOY WANT TO SORT THE DATA IN ASCENDING ORDER
    non_negative_filters = {
        'dy' : False,
        'p_l': True,
        'p_vp': True,
        'p_ebita': True,
        'p_ebit': True,
        'p_sr': True,
        'p_ativo': True,
        'p_capitlgiro': True,
        'ev_ebitda': True,
        'ev_ebit': True,
        'peg_Ratio': True,
        'roe': False,
        'roic': False,
        'lpa': False,
        'margemliquida': False,
        'receitas_cagr5': False,
        'lucros_cagr5': False,
        'liquidezcorrente': False
    }
    debt_filters = {
        'dividaliquida_patrimonioliquido': True,
        'dividaliquida_ebitda': True,
        'dividaliquida_ebit': True
    }

    champions = []
    df_non_negative = df[df['actual'] >= 0]
    for filter in non_negative_filters:
        sorting_mode = non_negative_filters[filter]
        filtered = df_non_negative[df_non_negative['key'] == filter].sort_values(by=['avgDifference', 'actual'], ascending=sorting_mode).head(HEAD_COUNT)
        champions.append(filtered)
    
    for filter in debt_filters:
        sorting_mode = debt_filters[filter]
        filtered = df[df['key'] == filter].sort_values(by=['avgDifference', 'actual'], ascending=sorting_mode).head(HEAD_COUNT)
        champions.append(filtered)
    df_champions = pd.concat(champions)
    df_champions.to_csv('champions.csv', index=False)
    most_occurences = df_champions['ticker'].value_counts().head(10)
    print("\n", most_occurences, "\n")


if __name__ == '__main__':
    with open(ASSETS_FILE, 'r') as f:
        symbols = f.read().splitlines()
    dfs = [parse_indicator_history(symbol) for symbol in symbols]
    df = pd.concat(dfs, ignore_index=True).replace(',', '.', regex=True)
    #print(df)
    systematic_filters(df)
    df.to_csv('raw_data_assets_indicators.csv', index=False)