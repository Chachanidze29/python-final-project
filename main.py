import pandas as pd
import matplotlib as plt
import pymongo
import os
from threading import Thread

# ვქმნი კავშირს test_database-ს კოლექციასთან სახელად playerColl
conn = pymongo.MongoClient()
mBase = conn['test_database']
mColl = mBase['playerColl']


class Player:
    # კონსტრუქტორი რომელიც იღებს მონაცემების მასივს და ავსებს შესაბამის ველებს შესაბამისი მნიშვნელობებით
    def __init__(self, data) -> None:
        self.ID = data[0]
        self.gender = data[1]
        self.country = data[2]
        self.fName = data[3]
        self.lName = data[4]
        self.height = data[5]
        self.width = data[6]
        self.town = data[7]
        self.position = data[8]

    # მეთოდი რომლითაც ვაბრუნებ ობიექტის შესაბამის string-ს
    def __str__(self) -> str:
        return f'{self.ID}, {self.gender}, {self.country}, {self.fName}, {self.lName}, {self.height}, {self.width}, {self.town}, {self.position}'

    # მეთოდი რომლითაც ვაბრუნებ ობიექტის შესაბამის ლექსიკონს
    def toDict(self):
        return {
            '_id': int(self.ID),
            'gender': self.gender,
            'country': self.country,
            'fName': self.fName,
            'lName': self.lName,
            'height': int(self.height),
            'width': self.width,
            'town': self.town,
            'position': self.position,
        }

    # მეთოდი რომლითაც Player ტიპის ობოექტი გადამყავს toDict ფუნქიით ლექსიკონის ტიპის ობიექტში და შემდეგ ვწერ მას ბაზაში
    def writeInDb(self):
        data = self.toDict()
        mColl.insert_one(data)

    # ფუნქცია რომლითაც Player ტიპის ობიექტების list-ს ვწერ ფაილში
    @staticmethod
    def wrtineInFile(plList):
        with open('players.txt', 'w') as f:
            for player in plList:
                f.write(player.__str__()+'\n')

    # ფუნქცია რომლითაც DataFrame ობიექტი გადამყავს Player ტიპის ობიექტების მასივში
    @staticmethod
    def dfToObjList(df):
        data, playerList = [], []
        for i in df.index:
            for x in df:
                data.append(df[x][i])
            playerList.append(Player(data))
            data = []
        return playerList


# panda მოდულის DataFrame ობიექტს ვიღებ ექსელის ფაილიდან
df = pd.read_excel('project.xlsx')

# მონაცემებს ვაჯგუფებ სიმაღლის მიხედვით და გამომაქვს შესასბამისი დიაგრამა
df1 = df['Height'].groupby(df['Height']).count()
df1 = df1.to_frame()
df1.columns = ['Players by height']
df1.plot.line(ylabel='Players')
plt.pyplot.show()

# Pie ტიპის დიაგრამა რომლის მონაცემებია ის ობიექტები,
# რომლებიც არიან ქვეყნიდან USA და გამომაქვს თუ რამდენი მათგანია ქალი და რამდენი კაცი
df2 = df[df['Country'] == 'USA']
df2 = df2['Team'].groupby(df2['Team']).count()
df2 = df2.to_frame()
df2.columns = ['Quantities of women and men players who live in USA']
df2.plot.bar(ylabel='Players')
plt.pyplot.show()

# DataFrame-დან ვიღებ Player ტიპის ობიექტების მასივს
playerList = Player.dfToObjList(df)
# ვწერ მათ ბაზაში
try:
    for p in playerList:
        p.writeInDb()
except pymongo.errors.DuplicateKeyError:
    print('Such id already in db')

# Thread-ების გამოყენებით ვწერ playerList-ის მონაცემებს ფაილში
threads = []

for _ in range(os.cpu_count()):
    threads.append(Thread(target=Player.wrtineInFile, args=(playerList,)))

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()