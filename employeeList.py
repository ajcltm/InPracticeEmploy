from pathlib import Path
import pandas as pd
from datetime import datetime
from dataclasses import dataclass

class idProvider:

    def __init__(self, df):
        self.df = df

    def get_id(self, name):
        con = self.df.loc[:, '사원'] == name
        id = self.df.loc[con]['사번']
        if len(id) == 1 : 
            id = id.iat[0]
        else :
            id = None
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

class Commander:

    def __init__(self, firstDic, orderDic):
        self.orderDic = orderDic
        self.commandDic = {
            '부서이동' : Update_status(firstDic, orderDic),
            '승급' : Update_status(firstDic, orderDic),
            '입사' : Join(firstDic, orderDic),
            '퇴직' : Quit_job(firstDic, orderDic),
            '휴직' : Leave(firstDic, orderDic)
        }

    def operation(self):
        orderAttribution = orderCategorizer().get_attribution(self.orderDic['발령명'])
        print(f'발령 접수되었음 {orderAttribution}')
        return self.commandDic[orderAttribution].command()

class SetOfCommanders(Commander):

    def __init__(self, firstDic, orderDics):
        self.firstDic = firstDic
        self.orderDics = orderDics

    def operation(self):
        for orderDic in self.orderDics:
            self.firstDic = Commander(self.firstDic, orderDic).operation()
        return self.firstDic

@dataclass
class Dataclass:
    id : int
    name : str
    birth : datetime
    team : str
    position : str
    positionDate : datetime
    leader : str
    joinDate : datetime
    quitDate : datetime
    kindOfContract : str
    currentState : str
    teamLeader : str

class DataclassTransformer():
    def __init__(self, dics):
        self.dics = dics

    def get_team_leader(self, dic):
        team = dic.get('부서')
        df = pd.DataFrame(self.dics.values())
        con = df.loc[:, '부서'] == team
        teamDf = df.loc[con]
        con2 = teamDf.loc[:, '직책'].isna()
        teamLeader = teamDf.loc[~con2].loc[:, '사원'].iat[0]
        return teamLeader
        
    def transform(self, dic):
        dataclass = Dataclass(
            dic['사번'],
            dic['사원'],
            dic['생년월일'],
            dic['부서'],
            dic['직급'],
            dic['현직급근무일'],
            dic['직책'],
            dic['입사일'] ,
            dic['퇴사일'],
            dic['사원구분'],
            dic['상태'],
            self.get_team_leader(dic)
        )
        return dataclass

class Tracker:

    trackSpace = {}

    def set_very_first_trackSpace(self, veryFirstDate):
        ids = self.firstDic.keys()
        for id in ids:
            self.trackSpace[id] = []
            temp = {}
            temp[veryFirstDate] = self.firstDic.get(id)
            self.trackSpace[id].append(temp)

    def __init__(self, veryFirstDate, firstDic, orderDics):
        self.firstDic = firstDic
        self.afterDic = None 
        self.orderDics = orderDics
        self.set_very_first_trackSpace(veryFirstDate)

    def get_different_data(self, id):
        if not self.trackSpace.get(id):
            return None
        beforeDataclass = self.trackSpace.get(id)[-1]
        afterDataclass = self.afterDic.get(id)
        if not beforeDataclass :
            return afterDataclass
        bool = beforeDataclass == afterDataclass
        if bool == False:
            return afterDataclass
        return None
    
    def update(self):
        for orderDic in self.orderDics:
            id = orderDic.get('사번')
            self.afterDic = Commander(self.firstDic, orderDic).operation()
            updateThing = self.get_different_data(id)
            if not updateThing:
                temp = {}
                temp[orderDic.get('발령일')] = updateThing
                if self.trackSpace.get(id):
                    self.trackSpace[id].append(temp)
                else :
                    self.trackSpace[id] = []
                    self.trackSpace[id].append(temp)
        return self.trackSpace




if __name__ == '__main__':

    dirPath = Path.home().joinpath('Desktop', '직원현황작업폴더', '202109071632')
    fileName = '20210914_110909_사원명부.csv'
    path = dirPath / fileName
    firstDf = pd.read_csv(path, encoding = 'cp949')
    dirPath2 = Path.home().joinpath('Desktop')
    fileName2 = '발령일괄등록.csv'
    path2 = dirPath2 / fileName2
    orderDf = pd.read_csv(path2, encoding='utf-8')

    firstDf = ListPreprocessor(firstDf).operation()
    lt = ListTransformer(firstDf)
    firstDic = lt.transferDict()    # get firstDic
    
    start = datetime(2021,11,1)
    
    orderDf = OrderPreprocessor(orderDf).operation()
    orderDf = OrderFilter(orderDf).filter(start)
    orderDic = orderTransformer(orderDf).transferDict()     # get orderDic

    firstDate = datetime(2021,1,1)
    track = Tracker(firstDate , firstDic, orderDic).update()

    order = orderDic[1]
    id = order.get('사번')
    name = order.get('사원')
    print(f'{name} : {track[id]}')
    print(pd.DataFrame(data = order.values()))