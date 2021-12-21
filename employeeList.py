from pathlib import Path
import pandas as pd

class idProvider:

    def __init__(self, df):
        self.df = df

    def get_id(self, name):
        con = self.df.loc[:, '사원'] == name
        id = self.df.loc[con]['사번']
        if len(id) == 1 : 
            id = id.iat[0]
        return id


class OrderPreprocessor:

    def __init__(self, orderDf):
        self.orderDf = orderDf

    def filt_nucessary_cols(self, df):
        necessary_cols = ['사번', '사원', '발령일', '발령명', '소속부서', '직급', '직책', '직종']
        df = df[necessary_cols]
        return df

    def transform_date_type(self, df):
        date_cols = ['발령일']
        for date_col in date_cols:
            df.loc[:, date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d')
        return df

    def deal_with_exception(self, df):
        con = df.loc[:, '발령명'] == '입사(수습)'
        id = df.loc[con].loc[:, '사번'].to_list()
        con2 = df.loc[:, '사번'].isin(id)
        con3 = df.loc[:, '발령명'] == '입사(일반)'
        idx = df.loc[con2&con3].index
        df.loc[idx, '발령명'] = '부서이동'
        return df

    def operation(self):
        df = self.orderDf.copy()
        df = df.dropna(how='all', axis=0)
        df = self.filt_nucessary_cols(df)
        df.loc[:, '사번'] = df.loc[:, '사번'].astype('int')
        df = self.transform_date_type(df)
        df = self.deal_with_exception(df)
        return df

class OrderFilter:

    def __init__(self, orderDf):
        self.orderDf = orderDf

    def filter(self, start, end=None):
        con1 = self.orderDf.loc[:, '발령일'] >= start
        self.order = self.orderDf.loc[con1]
        if end:
            con2 = self.orderDf.loc[:, '발령일'] <= end
            self.order = self.orderDf.loc[con1&con2]

        return self.order

class ListPreprocessor:

    def __init__(self, firstDf):
        self.firstDf = firstDf

    def filt_nucessary_cols(self, df):
        necessary_cols = ['사번', '사원', '생년월일', '부서', '직급', '현직급근무일', '직책', '입사일', '퇴사일', '사원구분', '발령명']
        df = df[necessary_cols]
        return df

    def transform_date_type(self, df):
        date_cols = ['입사일', '현직급근무일', '생년월일']
        for date_col in date_cols:
            df.loc[:, date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d')
        return df

    def create_leave(self, df):
        df = df.rename(columns={'발령명':'상태'})
        con = df.loc[:, '상태'].str.contains(r'^[휴직]', regex=True)
        id_1 = df.loc[con].index
        df.loc[id_1, '상태'] = '휴직' 
        
        id_2 = df.loc[~con].index
        df.loc[id_2, '상태'] = '정상근무' 
        return df

    def operation(self):
        df = self.firstDf.copy()
        df = self.filt_nucessary_cols(df)
        df = self.transform_date_type(df)
        df.loc[:, '사번'] = df.loc[:, '사번'].astype('int')
        df = self.create_leave(df)
        self.firstDf = df
        return self.firstDf

class ListTransformer:
    
    def transferDict(self):
        dictLst = self.firstDf.to_dict('records')
        temp_dict = {}
        for row_dict in dictLst:
            id_ = row_dict['사번']
            temp_dict[id_] = row_dict
        self.firstDict = temp_dict
        return self.firstDict

    def __init__(self, firstDf):
        self.firstDf = firstDf


class orderTransformer:

    def transferDict(self):
        dictLst = self.orderDf.to_dict('records')
        self.orderDf = dictLst
        return self.orderDf

    def __init__(self, orderDf):
        self.orderDf = orderDf

class orderCategorizer:
    callableDic = {}

    def createCallableDic(self):
        items = self.category.items()
        for item in items:
            value = item[0]
            keys = item[1]
            for key in keys:
                self.callableDic[key] = value

    def __init__(self):
        self.category = {'부서이동' : ['부서이동', '보직변경'], 
                        '승급':['승급'],
                        '입사': ['입사(일반)', '입사(수습)', '입사(파견직)'],
                        '퇴직': ['퇴직(희망퇴직)', '퇴직(계약만료)', '퇴직(정년퇴직)'],
                        '휴직' : ['휴직(육아)', '휴직(청원)']
                        }
        self.createCallableDic()

    def get_attribution(self, order):
        return self.callableDic.get(order)


class Update_status:
    def __init__(self, firstDic, orderDic):
        self.firstDic = firstDic
        self.orderDic = orderDic
    def command(self):
        firstDic = self.firstDic[self.orderDic['사번']]
        firstDic['부서'] = self.orderDic['소속부서']
        firstDic['직급'] = self.orderDic['직급']
        firstDic['직책'] = self.orderDic['직책']
        self.firstDic[self.orderDic['사번']] = firstDic
        print('='*50)
        print(f'발령처리 {firstDic}')
        return self.firstDic

class Join:
    def __init__(self, firstDic, orderDic):
        self.firstDic = firstDic
        self.orderDic = orderDic
    def get_state_of_contract(self):
        setter = {
            '일반직' : '일반직',
            '일반사무직' : '계약직',
            '전문사무직' : '계약직',
            '파견직' : '파견직_내부',
            '본부장' : '일반직'
            }
        return setter.get(self.orderDic['직종'])

    def command(self):
        self.firstDic[self.orderDic['사번']] ={}
        firstDic = self.firstDic[self.orderDic['사번']]
        firstDic['사번'] = self.orderDic['사번']
        firstDic['사원'] = self.orderDic['사원']
        firstDic['생년월일'] = None
        firstDic['부서'] = self.orderDic['소속부서']
        firstDic['직급'] = self.orderDic['직급']
        firstDic['현직급근무일'] = self.orderDic['발령일']
        firstDic['직책'] = self.orderDic['직책']
        firstDic['입사일'] = self.orderDic['발령일']
        firstDic['퇴사일'] = None
        firstDic['사원구분'] = self.get_state_of_contract()
        firstDic['상태'] = '정상근무'
        self.firstDic[self.orderDic['사번']] = firstDic
        print('='*50)
        print(f'퇴사처리 {firstDic}')
        return self.firstDic

class Quit_job:
    def __init__(self, firstDic, orderDic):
        self.firstDic = firstDic
        self.orderDic = orderDic
    def command(self):
        firstDic = self.firstDic[self.orderDic['사번']]
        firstDic['부서'] = self.orderDic['소속부서']
        firstDic['직급'] = self.orderDic['직급']
        firstDic['직책'] = self.orderDic['직책']
        firstDic['퇴사일'] = self.orderDic['발령일']
        firstDic['상태'] = '퇴직'
        self.firstDic[self.orderDic['사번']] = firstDic
        print('='*50)
        print(f'퇴사처리 {firstDic}')
        return self.firstDic

class Leave:
    def __init__(self, firstDic, orderDic):
        self.firstDic = firstDic
        self.orderDic = orderDic
    def command(self):
        firstDic = self.firstDic[self.orderDic['사번']]
        firstDic['부서'] = self.orderDic['소속부서']
        firstDic['직급'] = self.orderDic['직급']
        firstDic['직책'] = self.orderDic['직책']
        firstDic['상태'] = '휴직'
        self.firstDic[self.orderDic['사번']] = firstDic
        print('='*50)   
        print(f'휴직처리 {firstDic}')
        return self.firstDic

class commander:

    def __init__(self, firstDic, orderDic):
        self.orderDic = orderDic
        self.commandDic = {
            '부서이동' : Update_status(firstDic, orderDic),
            '승급' : Update_status(firstDic, orderDic),
            '입사' : Update_status(firstDic, orderDic),
            '퇴직' : Quit_job(firstDic, orderDic),
            '휴직' : Leave(firstDic, orderDic)
        }

    def operation(self):
        orderAttribution = orderCategorizer().get_attribution(self.orderDic['발령명'])
        print(f'발령 접수되었음 {orderAttribution}')
        return self.commandDic[orderAttribution].command()


if __name__ == '__main__':

    from datetime import datetime

    dirPath = Path.home().joinpath('Desktop', '직원현황작업폴더', '202109071632')
    fileName = '20210914_110909_사원명부.csv'
    path = dirPath / fileName
    firstDf = pd.read_csv(path, encoding = 'cp949')
    dirPath2 = Path.home().joinpath('Desktop')
    fileName2 = '발령일괄등록.csv'
    path2 = dirPath2 / fileName2
    orderDf = pd.read_csv(path2, encoding='utf-8')

    id = idProvider(firstDf).get_id('장수웅')
    print('='*50)
    print(id)

    firstDf = ListPreprocessor(firstDf).operation()
    lt = ListTransformer(firstDf)
    firstDic = lt.transferDict()
    print('='*50)
    print(f'firstDic : {firstDic[id]}')
    
    start = datetime(2021,5,1)
    
    orderDf = OrderPreprocessor(orderDf).operation()
    orderDf = OrderFilter(orderDf).filter(start)
    orderDic = orderTransformer(orderDf).transferDict()

    orderDf_ = pd.DataFrame(orderDic)
    con = orderDf_.loc[:, '사원'] == '김동준'
    orderDf__ = orderDf_.loc[con]
    print('='*50)
    print(f'orderDic :\n {orderDf__}')
    print('='*50)
    print(orderDic[3])

    attri = orderCategorizer().get_attribution('퇴직(희망퇴직)')
    print('='*50)
    print(attri)

    result = commander(firstDic, orderDic[3]).operation()
    print('='*50)
    print(result[orderDic[3]['사번']])
    df = pd.DataFrame(result.values())
    con = df.loc[:, '사원'] == '장수웅'
    print('='*50)
    print(df.loc[con])