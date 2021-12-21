import pandas as pd
import numpy as np
import datetime

class NumberOfEmployee() :

    def __init__(self, folderPath) :
        self.folderPath = folderPath

        self.employee_list_file_name = '사원명부'
        self.delete_list = '지우기목록'
        self.temp_employee_list = '계약직목록'
        self.order_of_teams = '직제순서'
        self.order_of_positions = '직급순서'

        self.employee_list_df = None
        self.delete_list_df = None
        self.temp_employee_list_df = None
        self.order_of_teams_df = None
        self.order_of_positions_df = None

        self.employee_list_df = None
        self.number_table_df = None
        self.composition_table_df = None

    def load_file_utf8(self, file_name) :
        file = pd.read_csv('{0}/{1}.csv'.format(self.folderPath, file_name), encoding='utf-8')
        return file

    def get_input_df(self) :
        self.employee_list_df = pd.read_csv('{0}/{1}.csv'.format(self.folderPath, self.employee_list_file_name), encoding='utf-8')
        self.delete_list_df = pd.read_csv('{0}/{1}.csv'.format(self.folderPath, self.delete_list), encoding='utf-8')
        self.temp_employee_list_df = pd.read_csv('{0}/{1}.csv'.format(self.folderPath, self.temp_employee_list), encoding='utf-8')
        self.order_of_teams_df = pd.read_csv('{0}/{1}.csv'.format(self.folderPath, self.order_of_teams), encoding='utf-8')
        self.order_of_positions_df = pd.read_csv('{0}/{1}.csv'.format(self.folderPath, self.order_of_positions), encoding='utf-8')

    def get_suspicious(self) :
        df = pd.read_csv('{0}/{1}.csv'.format(self.folderPath, self.employee_list_file_name), encoding='utf-8')
        con_1 = df['사원'].str.extract(r'^([가-힣]{0,2})$').notnull().squeeze()  # 이름이 2글자 미만인 사원
        con_2 = df['사원'].str.extract(r'^(.{4,})$').notnull().squeeze()  # 이름이 네글자 이상인 사원
        con_3 = df['사원'].str.extract(r'(\d{1})').notnull().squeeze()  # 숫자가 들어간 사원
        con_4 = df['사원'].str.extract(r'(감사)').notnull().squeeze()  # 이름에 감사가 들어간 사원
        s = df.loc[con_1|con_2|con_3|con_4].loc[:,'사원']
        delete_list_df = self.load_file_utf8(self.delete_list)
        suspicious = pd.concat([s,s.apply(lambda x : True if x in delete_list_df['사원'].tolist() else False)], axis=1)
        return suspicious

    def delete(self, df):
        delete_list_df = self.load_file_utf8(self.delete_list)
        con = df['사원'].isin(delete_list_df['사원'].values.tolist())
        df=df.loc[~con]
        return df

    def tem_employee(self, df):
        temp_employee_df = self.load_file_utf8(self.temp_employee_list)
        con = df['사원'].isin(temp_employee_df['사원'].values.tolist())
        target_index = df.loc[con].loc[:,'직급'].index
        df.loc[target_index, '직급'] = '계약직'
        return df

    def arrange_department(self, df, date) :
        order_df = self.load_file_utf8(self.order_of_teams)
        order_df['시점']=pd.to_datetime(order_df['시점'], format='%Y-%m-%d')
        order_df=order_df[order_df['시점']==date]
        df_merge = pd.merge(order_df, df, left_on='부서', right_on=df.index)
        df_merge = df_merge.sort_values(by=['직제순서'])
        df_merge=df_merge.drop(['시점', '직제순서'], axis=1)
        return df_merge
    
    def creatComposition(self, df, result_df) :
        positions = ['사무총장', '운영국장', '본부장', '1급', '2급', '3급', '4급', '5급', '계약직',
                    '전문사무직 나급', '전문사무직 다급', '파견5급']

        dic = {'사무총장': np.nan, '운영국장':np.nan, '본부장':np.nan, '1급':np.nan, '2급':np.nan, '3급':np.nan, '4급':np.nan, '5급':np.nan, 
            '계약직':np.nan, '전문사무직 나급':np.nan, '전문사무직 다급':np.nan, '파견5급':np.nan}

        depart = df.loc[:, '부서'].unique().tolist()[0]
        con = result_df.loc[:, '부서']==depart
        departNumS = result_df.loc[con]
        
        for posi in positions :
            departNum = departNumS[posi].iat[0]
            if departNum == 0 :
                departNum = '-'
            else :
                departNum = f'({str(departNum)})'
            con = df.loc[:, '직급'] == posi
            lst = [departNum] + df.loc[con].loc[:, '사원'].values.tolist()
            string = "\n".join(lst)
            dic[posi] = string
                
        return dic

    def get_employee_list_df(self) :
        df = self.load_file_utf8(self.employee_list_file_name)
        df.dropna(how='all', inplace=True)
        df=self.delete(df)
        df = self.tem_employee(df)

        self.employee_list_df = df

        return df

    def get_number_table_df(self) :
        df = self.get_employee_list_df()
        result_df = pd.crosstab(df['부서'], df['직급'])
        result_df['소계'] = result_df['사무총장']+result_df['운영국장']+result_df['본부장']+result_df['1급']+result_df['2급']+result_df['3급']+result_df['4급']+result_df['5급']
        result_df['총계'] = result_df['소계']+result_df['계약직']+result_df['전문사무직 나급']+result_df['전문사무직 다급']+result_df['파견5급']
        positions = ['사무총장', '운영국장', '본부장', '1급', '2급', '3급', '4급', '5급', '소계', '계약직',
                        '전문사무직 나급', '전문사무직 다급', '파견5급', '총계']
        result_df = result_df[positions]
        date = datetime.datetime(2021,1,1)
        result_df = self.arrange_department(result_df, date)
        
        self.number_table_df = result_df

        return result_df
    
    def get_composition_table_df(self) :
        df = self.get_employee_list_df()           
        result_df = self.get_number_table_df()

        compoDf = df[['부서', '사원', '직급']]
        lstDepart = result_df['부서'].tolist()

        baseDf = pd.DataFrame(columns=['사무총장', '운영국장', '본부장', '1급', '2급', '3급', '4급', '5급', '계약직',
                 '전문사무직 나급', '전문사무직 다급', '파견5급'])

        for depart in lstDepart :
            con = compoDf['부서']==depart
            testDf = compoDf.loc[con]
            s = self.creatComposition(testDf, result_df)
            s = pd.Series(s, name=depart)
            baseDf = baseDf.append(s, ignore_index=True)
        baseDf.index = lstDepart

        baseDf2 = pd.DataFrame(columns=['소계', '총계'])
        for depart in lstDepart :
            con = result_df.loc[:, '부서'] == depart
            sum1 = result_df.loc[con].loc[:,'소계'].iat[0]
            sum2 = result_df.loc[con].loc[:,'총계'].iat[0]
            srs = pd.Series([sum1, sum2], index=['소계', '총계'])
            baseDf2 = baseDf2.append(srs, ignore_index=True)
        compoDf = pd.concat([baseDf.reset_index(), baseDf2.reset_index(drop=True)], axis=1, join='inner')
        compoDf = compoDf.set_index('index')
        compoDf.index.name = '부서'
        orderedColumns = ['사무총장', '운영국장', '본부장', '1급', '2급', '3급', '4급', '5급', '소계', '계약직', '전문사무직 나급',
            '전문사무직 다급', '파견5급', '총계']
        compoDf = compoDf[orderedColumns]

        result_df_ = result_df.set_index('부서')

        result_df_1 = result_df_.loc['대중소기업농어업협력재단':'농어촌기금관리부']
        result_1_sum = result_df_1.apply(lambda x: x.sum(), axis=0)
        result_1_sum.name = '재단 합계'

        result_df_2 = result_df_.loc['동반위 운영국':]
        result_2_sum = result_df_2.apply(lambda x: x.sum(), axis=0)
        result_2_sum.name = '위원회 합계'

        result_sum = result_df_.apply(lambda x: x.sum(), axis=0)
        result_sum.name = '재단-위원회 합계'

        compoDf_ = compoDf.append(result_1_sum)
        compoDf_ = compoDf_.append(result_2_sum)
        compoDf_ = compoDf_.append(result_sum)

        foundationIndice = result_df_.loc['대중소기업농어업협력재단':'농어촌기금관리부'].index.tolist()
        commiteeIndice = result_df_.loc['동반위 운영국':].index.tolist()

        orderedIndice = foundationIndice + ['재단 합계'] + commiteeIndice + ['위원회 합계'] + ['재단-위원회 합계']

        compoDf_ = compoDf_.reindex(index=orderedIndice)

        self.composition_table_df = compoDf_
        
        return compoDf_

    def save_all(self) :
        df = self.get_employee_list_df()
        timestamp = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        df.to_csv("{0:s}/{1}_사원명부.csv".format(str(self.folderPath), timestamp),encoding="cp949")

        result_df = self.get_number_table_df()
        timestamp = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        result_df.to_csv("{0:s}/{1}_직원현원표.csv".format(str(self.folderPath), timestamp),encoding="cp949")

        compoDf = self.get_composition_table_df()
        timestamp = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        compoDf.to_csv("{0:s}/{1}_부서별구성원표.csv".format(str(self.folderPath), timestamp),encoding="cp949")
