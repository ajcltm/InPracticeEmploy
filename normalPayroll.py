from pathlib import Path
import pandas as pd

class NormalPayroll() :

    def __init__(self) :
        self.folderPath = Path.home().joinpath('Desktop').joinpath('직원보수작업폴더')  

    # 기본급-연봉df 불러오기(from parquet format) 
    def loadData(self):
        df = pd.read_parquet(self.folderPath/'직원통상급여.parquet')
        return df

    # 시간외수당 계산 함수
    def calculSigan(self, jikkb, year) :
        treeDic = { '2015': {'3급': 500000, '4급': 475000, '5급': 390000},
                    '2016': {'3급': 510000, '4급': 480000, '5급': 396000}, 
                    '2017': {'3급': 510000, '4급': 480000, '5급': 396000},
                    '2018': {'3급': 510000, '4급': 480000, '5급': 396000},
                    '2019': {'3급': 510000, '4급': 480000, '5급': 396000},
                    '2020': {'3급': 540000, '4급': 510000, '5급': 420000},
                    '2021': {'3급': 540100, '4급': 510900, '5급': 420000}
                    }
        sigan = treeDic[year][jikkb]
        return sigan
    

    # 직책수당 계산 함수
    def calculJikchek(self, jikchek, year):
        treeDic = { '2015': {'운영국장': 1700000, '본부장': 1000000, '실장': 900000, '부장': 800000, '팀장': 700000},
                    '2016': {'운영국장': 1700000, '본부장': 1000000, '실장': 900000, '부장': 800000, '팀장': 700000},
                    '2017': {'운영국장': 1700000, '본부장': 1000000, '실장': 900000, '부장': 800000, '팀장': 700000},
                    '2018': {'운영국장': 1700000, '본부장': 1000000, '실장': 900000, '부장': 800000, '팀장': 700000},
                    '2019': {'운영국장': 1700000, '본부장': 1000000, '실장': 900000, '부장': 800000, '팀장': 700000},
                    '2020': {'운영국장': 1700000, '본부장': 1000000, '실장': 900000, '부장': 800000, '팀장': 700000},
                    '2021': {'운영국장': 1700000, '본부장': 1000000, '실장': 900000, '부장': 800000, '팀장': 700000}
                    }
        sigan = treeDic[year][jikchek]
        return sigan
    
    # 통신비 계산 함수
    def clacultongsin(self, jikchek):
        group1 = ['운영국장', '본부장', '실장']
        group2 = ['부장', '팀장']
        if jikchek in group1:
            tongsin=100000
        else:
            tongsin=50000
        return tongsin
    

    # 수당 계산 함수
    def calculateSudangSri(self, series) :
        jikkb = series['직급']
        jikchek = series['직책']
        year = str(series['적용년월'].year)
        jikchekGroup = ['사원', '대리', '과장', '차장']
        if jikchek in jikchekGroup :
            sigan = (jikkb, year)
            jikchekPay = 0
            tongsin = 0
        else :
            sigan = 0
            jikchekPay = self.calculJikchek(jikchek, year)
            tongsin = self.clacultongsin(jikchek)

        jusik = 130000
        gyotong = 200000

        # sri = pd.Series(data={'jusik':jusik, 'gyotong':gyotong, 'sigan':sigan, 'jikchekPay':jikchekPay, 'tongsin':tongsin})
        sudangLst=['중식비', '교통비', '시간외수당', '직책수당', '통신비']
        sri = pd.Series(data=[jusik, gyotong, sigan, jikchekPay, tongsin], index=sudangLst)

        return sri

    # 연봉-기본급 df에 수당 정보 추가 함수
    def mergeSudangDf(self, df) :
        sudangDf = df.apply(lambda x: self.calculateSudangSri(x), axis=1)
        df = pd.concat([df, sudangDf], axis=1)
        return df