import pandas as pd


def get_day_type(daytypecode):
    if daytypecode == 1:
        return 'Sunday/Public Holiday'
    elif daytypecode == 2:
        return 'Working Week Day'
    elif daytypecode == 7:
        return 'Saturday'
    else:
        return 'Error'


def demand_envelop(run_id):
    # df = pd.DataFrame()
    df_max = pd.read_csv('max_mwh.csv')
    # 1: Sunday/Public Holiday; 2: Working Week Day; 7: Saturday
    df_max['daytype_string'] = df_max['daytype'].apply(get_day_type)
    df_max['mw'] = df_max['max_mwh']*2
    df_max['summary_type'] = 'max'
    df_max = df_max.drop(columns=['max_mwh'])

    df_min = pd.read_csv('min_mwh.csv')
    # 1: Sunday/Public Holiday; 2: Working Week Day; 7: Saturday
    df_min['daytype_string'] = df_min['daytype'].apply(get_day_type)
    df_min['mw'] = df_min['min_mwh'] * 2
    df_min['summary_type'] = 'min'
    df_min = df_min.drop(columns=['min_mwh'])

    df_avg = pd.read_csv('avg_mwh.csv')
    # 1: Sunday/Public Holiday; 2: Working Week Day; 7: Saturday
    df_avg['daytype_string'] = df_avg['daytype'].apply(get_day_type)
    df_avg['mw'] = df_avg['avg_mwh'] * 2
    df_avg['summary_type'] = 'avg'
    df_avg = df_avg.drop(columns=['avg_mwh'])

    df = df_max.append([df_min, df_avg])
    df.to_excel('demand_envelope_{}.xlsx'.format(run_id))


if __name__ == '__main__':
    demand_envelop(run_id=50014)
