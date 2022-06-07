# coding=utf-8
import pandas as pd
import yfinance as yf
from linebot import LineBotApi
from linebot.models import *
import json
from pymongo import MongoClient
from datetime import datetime, timedelta, date
import requests

# -------------------- v2 -----------------------
from collections import Counter
import jieba


class FinancialConsultBot(object):
    def __init__(self):
        self.stockList = [{'sp500': '^GSPC'}, {'Dow Jones': '^DJI'}, {'Russell 2000': '^RUT'}, {'NASDAQ': '^IXIC'}, {'PHLX Semiconductor': '^SOX'}, {'TSEC weighted index': '^TWII'}, {'Crude Oil': 'CL=F'}, {'Treasury Yield 5 Years': '^FVX'}, {'Treasury Yield 10 Years': '^TNX'}, {'Treasury Yield 30 Years': '^TYX'}, {'USD/TWD': 'TWD=X'}]

        with open('LinebotInfo.json', 'r') as f:
            self.linebotInfo = json.load(f)

        self.client = MongoClient('localhost', 27017)
        self.db = self.client['BigData']
        self.collection = self.db['News']
        self.Mood = self.db['Mood']

    # --------------------------------------------------------------
    def comment(self, stockDict: dict):
        stockName = list(stockDict.keys())[0]
        stockCode = list(stockDict.values())[0]
        data = yf.download(stockCode, period='7d')
        data['Diff'] = data['Close'].diff()
        data['Percent'] = data['Diff'] / data['Close'].shift() * 100
        data = data.round(2)

        text = '{} \n漲跌: {} \n漲跌幅: {}%'.format(stockName, str(data['Diff'][-1]), str(data['Percent'][-1]))
        return text

    def firstMessage(self):
        message = ''
        for i in self.stockList:
            message = message + self.comment(i) + '\n\n'

        return message

    # --------------------------------------------------------------
    def getData(self):
        data = []

        for mrd in self.collection.find():
            data.append(mrd)

        data = pd.DataFrame(data)
        return data

    def timecut(self):
        now = datetime.now()
        yesturday = now - timedelta(seconds=65700)
        timestamp = yesturday.timestamp()
        return int(timestamp)

    def secondMessage_V1(self, data):
        timecut = self.timecut()
        domesticData = data[data['Domestic'] == 1]
        foreginData = data[data['Domestic'] == 0]

        subData = domesticData.loc[data['Time'] > timecut]
        subData = subData.sort_values(by='Views', ascending=False)[:3].reset_index()

        text = ''
        for i in range(3):
            text = text + subData['Title'][i] + '\n' + subData['_id'][i] + '\n'

        subData = foreginData.loc[data['Time'] > timecut]
        subData = subData.sort_values(by='Views', ascending=False)[:3].reset_index()

        for i in range(3):
            text = text + subData['Title'][i] + '\n' + subData['_id'][i] + '\n'

        return text

    # ---------------
    # --------------- v2
    # ---------------
    def keyword(self, data):
        stopwords = requests.get('https://raw.githubusercontent.com/minchi0314/dict/main/stopwords.txt')
        stopwords = stopwords.text.split('\n')
        words = []

        for text in data['Title']:
            sentence_seged = jieba.cut(text.strip())
            for word in sentence_seged:
                if word not in stopwords:
                    words.append(word)

        tmp = Counter(words)
        keyword = tmp.most_common(1)[0][0]
        return keyword

    def secondMessage_V2(self, data):
        timecut = self.timecut()
        domesticData = data[data['Domestic'] == 1]
        foreginData = data[data['Domestic'] == 0]
        keyword_domestic = self.keyword(domesticData)
        keyword_foregin = self.keyword(foreginData)

        subData = domesticData.loc[data['Time'] > timecut]
        subData = subData.sort_values(by='Views', ascending=False)[:1].reset_index()

        text = ''
        text = text + subData['Title'][0] + '\n' + subData['_id'][0] + '\n'

        subData = domesticData.loc[domesticData['Title'].str.contains(keyword_domestic)]
        subData = subData.sample().reset_index()
        text = text + subData['Title'][0] + '\n' + subData['_id'][0] + '\n'

        subData = foreginData.loc[data['Time'] > timecut]
        subData = subData.sort_values(by='Views', ascending=False)[:1].reset_index()

        text = text + subData['Title'][0] + '\n' + subData['_id'][0] + '\n'

        subData = foreginData.loc[foreginData['Title'].str.contains(keyword_foregin)]
        subData = subData.sample().reset_index()
        text = text + subData['Title'][0] + '\n' + subData['_id'][0] + '\n'

        return text

    def secondMessage_V3(self, data):
        timecut = self.timecut()
        data = data.loc[data['Time'] > timecut]

        all_doc = list(data['Title'])

        all_doc_list = []
        for doc in all_doc:
            doc_list = [word for word in jieba.cut(doc)]
            all_doc_list.append(doc_list)

        dictionary = corpora.Dictionary(all_doc_list)
        corpus = [dictionary.doc2bow(doc) for doc in all_doc_list]

        doc_test_list = [word for word in jieba.cut('今天美股大漲100點')]
        doc_test_vec = dictionary.doc2bow(doc_test_list)

        tfidf = models.TfidfModel(corpus)

        index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=len(dictionary.keys()))
        sim = index[tfidf[doc_test_vec]]

        tmp = sorted(enumerate(sim), key=lambda item: -item[1])

        text = ''

        for i in range(6):
            text = text + data['Title'][tmp[i][0]] + '\n'
            text = text + data['_id'][tmp[i][0]] + '\n'
            text = text + '\n'

        return text
    # --------------------------------------------------------------
    def thirdMessage(self, data):
        domestic = data[data['Domestic'] == 1]
        num_domestic = int(domestic.shape[0])
        score_domestic = int(sum(domestic['Mood']))

        foregin = data[data['Domestic'] == 0]
        num_foregin = int(foregin.shape[0])
        score_foregin = int(sum(foregin['Mood']))

        text = '根據 {} 篇國內文章, 大數據分析結果: 今天市場情緒指標為 {} 分。 \n\n'.format(str(num_domestic), str(score_domestic))
        text = text + '根據 {} 篇國外文章, 大數據分析結果: 今天市場情緒指標為 {} 分。 '.format(str(num_foregin), str(score_foregin))

        now = datetime.now()
        pairs = {'_id': now.strftime('%Y-%m-%d'), 'Domestic': score_domestic, 'Foregin': score_foregin}
        self.Mood.insert_one(pairs)

        return text

    # --------------------------------------------------------------
    def sendMessages(self, data, to):
        line_bot_api = LineBotApi(self.linebotInfo['token'])

        firstMessage = self.firstMessage()
        line_bot_api.push_message(to, TextSendMessage(text=firstMessage))

        secondMessage = self.secondMessage_V1(data)
        line_bot_api.push_message(to, TextSendMessage(text=secondMessage))

        secondMessage = self.secondMessage_V2(data)
        line_bot_api.push_message(to, TextSendMessage(text=secondMessage))

        secondMessage = self.secondMessage_V3(data)
        line_bot_api.push_message(to, TextSendMessage(text=secondMessage))

        thirdMessage = self.thirdMessage(data)
        line_bot_api.push_message(to, TextSendMessage(text=thirdMessage))


# ------ test
import re
import numpy as np
import jieba
from gensim import corpora, models, similarities
import warnings
import os
from scipy.linalg import norm







if __name__ == '__main__':
    con = FinancialConsultBot()
    testAccount = con.linebotInfo['testUser']
    # testAccount = con.linebotInfo['testGroup']
    data = con.getData()
    print(con.secondMessage_V3(data))

    # con.sendMessages(to=testAccount, data=data)
    # print(con.secondMessage_V2(data))
    # print(con.keyword(data))
    # all_doc = list(data['Title'])
    #
    # all_doc_list = []
    # for doc in all_doc:
    #     doc_list = [word for word in jieba.cut(doc)]
    #     all_doc_list.append(doc_list)
    #
    # dictionary = corpora.Dictionary(all_doc_list)
    # # dictionary.token2id
    # corpus = [dictionary.doc2bow(doc) for doc in all_doc_list]
    #
    # doc_test_list = [word for word in jieba.cut('今天美股大漲100點')]
    #
    # doc_test_vec = dictionary.doc2bow(doc_test_list)
    # # print(doc_test_vec)
    #
    # tfidf = models.TfidfModel(corpus)
    # # print(tfidf[doc_test_vec])
    # index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=len(dictionary.keys()))
    # sim = index[tfidf[doc_test_vec]]
    #
    # a = sorted(enumerate(sim), key=lambda item: -item[1])
    # print(a)
    # print(data['Title'][a[0][0]])
    # print(data['_id'][a[0][0]])
    #
    # # print('#####  接近  ######')
    # # print(data['Title'][122])
    # # print(data['Title'][137])
    # # print(data['Title'][7])
    # # print(data['Title'][221])
    # # print(data['Title'][212])
    # # print('#####  遠離  #####')
    # # print(data['Title'][327:330])

