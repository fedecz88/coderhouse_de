import yfinance as yf
import matplotlib.pyplot as plt

#Puede ser necesario instalar psycopg2 (o psycopg2-binary si no funciona el original)
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import scoped_session, sessionmaker

import pandas as pd

#pfe = yf.Ticker('PFE')
#pfe.info
#print(pfe.info) #visual


#tickers = ['TSLA', 'META', 'AAPL', 'GOOG']
#
#for i,ticker in enumerate(tickers):
#    current_ticker = yf.Ticker(ticker)
#    plt.subplot(len(tickers),1,i+1)
#    current_ticker.history(period='365d')['Close'].plot(figsize=(16,60), title="Precio historico de 1 de un año para: "+ticker)
    #plt.show() #visual

#Crear conexión al postgres
engine = create_engine('postgresql://postgres:postgres@localhost:5435/finanzas')

goo = yf.Ticker('GOOG')
# sacar la informacion historica de 1 año hacia atras
hist = goo.history(period="1y")
hist['Date']=hist.index
hist=hist.reset_index(drop=True)
#hist.info # tenemos 8 columnas
print(hist.info) #visual

hist.to_sql('hist_google', engine, index=False, if_exists='replace')

#https://towardsdatascience.com/a-guide-on-how-to-interact-between-python-and-databases-using-sqlalchemy-and-postgresql-a6d770723474
db = scoped_session(sessionmaker(bind=engine))
db.execute(text('INSERT INTO hist_google ("Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits", "Date") VALUES (2,3,4,5,6,7,8,current_timestamp);'))
db.commit()
db.close()
