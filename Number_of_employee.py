import pandas as pd
import datetime

def load_file_utf8(path, file_name) :
    file = pd.read_csv('{0}/{1}.csv'.format(path, file_name), encoding='utf-8')
    return file

def delet(df, path):
    rubish = load_file_utf8(path, '지우기 목록')
    indice=[]
    for rubish in rubish['사원'] :
        index=df[df['사원']==rubish].index[0]
        indice.append(index)
    df=df.drop(indice)
    return df

def tem_employee(df, path):
    tem = load_file_utf8(path, '계약직 목록')
    for t in tem['사원'] :
        # df[df['사원']==t]['직급'] = '계약직'
        df.loc[(df['사원'] == t), '직급'] = '계약직'
        df
    return df

def arrange_department(df, date, path) :
    order_df = load_file_utf8(path, '직제순서')
    order_df['시점']=pd.to_datetime(order_df['시점'], format='%Y-%m-%d')
    order_df=order_df[order_df['시점']==date]
    df_merge = pd.merge(order_df, df, left_on='부서', right_on=df.index)
    df_merge = df_merge.sort_values(by=['직제순서'])
    df_merge=df_merge.drop(['시점', '직제순서'], axis=1)
    return df_merge

def main():
    path = 'C:/Users/user/Desktop/직원 현황 테스트'
    file = load_file_utf8(path, '210616_사원명부')
    df = pd.DataFrame(file)
    df = delet(df, path)
    df = tem_employee(df, path)
    df.to_csv("{0:s}/요약_.csv".format(path), encoding="cp949")
    print(df)
    df = pd.crosstab(df['부서'], df['직급'])
    df['소계'] = df['사무총장']+df['운영국장']+df['본부장']+df['1급']+df['2급']+df['3급']+df['4급']+df['5급']
    df['총계'] = df['소계']+df['계약직']+df['전문사무직 나급']+df['전문사무직 다급']+df['파견5급']
    print(df.columns)
    positions = ['사무총장', '운영국장', '본부장', '1급', '2급', '3급', '4급', '5급', '소계', '계약직',
                 '전문사무직 나급', '전문사무직 다급', '파견5급', '총계']
    df = df[positions]

    date = datetime.datetime(2021, 1, 1)
    df_arranged = arrange_department(df, date, path)

    print(df_arranged)
    df_arranged.to_csv("{0:s}/요약.csv".format(path), encoding="cp949")

if __name__ == '__main__' :
    main()