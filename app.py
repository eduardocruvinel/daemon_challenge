import sqlite3
import yfinance as yf
from company import Company
from stock_value import StockValue
from yahoo_fin.stock_info import get_data
from matplotlib import pyplot as plt

# Initially, let's open a new Database Connection

conn = sqlite3.connect(':memory:')

c = conn.cursor()

# Now, we'll create the 
# Creating companies and stock_values tables

c.execute("""CREATE TABLE companies (
            stock_name text PRIMARY KEY,
            company_name text,
            exchange text,
            currency text
            )""")

c.execute("""CREATE TABLE stock_values (
            stock_name text,
            date text,
            variable text,
            value real,
            FOREIGN KEY(stock_name) REFERENCES companies(stock_name)
            )""")
 
# Inserting data into databases
# Companies DB
 
def insert_company(company):
    with conn:
        c.execute("INSERT INTO companies VALUES (:stock_name, :company_name, :exchange, :currency)",
                                                {'stock_name': company.stock_name,
                                                 'company_name': company.company_name,
                                                 'exchange': company.exchange,
                                                 'currency': company.currency})

# Stock Values DB

def insert_stock_value(stock_value):
    with conn:
        c.execute("INSERT INTO stock_values VALUES (:stock_name, :date, :variable, :value)",
                                                   {'stock_name': stock_value.stock_name,
                                                    'date': stock_value.date,
                                                    'variable': stock_value.variable,
                                                    'value': stock_value.value})

# Get data from databases
## Companies DB

def get_all_companies():
    c.execute("SELECT * FROM companies")
    return c.fetchall()

def get_companies_by_stock_name(stock_name):
    c.execute("SELECT * FROM companies WHERE stock_name=:stock_name", {'stock_name': stock_name})
    return c.fetchall()

def get_companies_by_company_name(company_name):
    c.execute("SELECT * FROM companies WHERE company_name=:company_name", {'company_name': company_name})
    return c.fetchall()

def get_companies_by_exchange(exchange):
    c.execute("SELECT * FROM companies WHERE exchange=:exchange", {'exchange': exchange})
    return c.fetchall()

def get_companies_by_currency(currency):
    c.execute("SELECT * FROM companies WHERE currency=:currency", {'currency': currency})
    return c.fetchall()

# Values DB

def get_all_stock_values():
    c.execute("SELECT * FROM stock_values")
    return c.fetchall()

def get_stock_values_by_stock_name(stock_name):
    c.execute("SELECT * FROM stock_values WHERE stock_name=:stock_name", {'stock_name': stock_name})
    return c.fetchall()

def get_stock_values_by_date(date):
    c.execute("SELECT * FROM stock_values WHERE date=:date", {'date': date})
    return c.fetchall()

def get_stock_values_by_variable(variable):
    c.execute("SELECT * FROM stock_values WHERE variable=:variable", {'variable': variable})
    return c.fetchall()

def get_all_values_by_stock_name(stock_name):
    c.execute("SELECT date, value FROM stock_values WHERE stock_name=:stock_name AND variable=:variable", 
             {'stock_name': stock_name, 'variable': 'ClosePrice'})
    return c.fetchall()

# Ingestion

def populate_stock_values(ticker):
    data_1 = get_data(ticker)

    for index, row in data_1.iterrows():
        stock_name = ticker
        date = index.strftime('%Y-%m-%d')
        value = row[5]
        variable = 'Volume'

        stock_value_volume = StockValue(stock_name, date, variable, value)
        insert_stock_value(stock_value_volume)

        value = row[3]
        variable = 'ClosePrice'

        stock_value_close_price = StockValue(stock_name, date, variable, value)
        insert_stock_value(stock_value_close_price)
        
def populate_companies(ticker):
    data_2 = yf.Ticker(ticker).info

    stock_name = ticker
    company_name = data_2['longName']
    exchange = data_2['exchange']
    currency = data_2['financialCurrency']

    company = Company(stock_name, company_name, exchange, currency)
    insert_company(company)

# We'll use four different companies as examples

tickers = ['PETR4.SA', 'META', 'BBAS3.SA', 'SU.PA']

for ticker in tickers:
    populate_companies(ticker)
    populate_stock_values(ticker)

companies = get_all_companies()
stock_values = get_all_stock_values()

print(companies)
print(stock_values)

# Ouuff, it works! To be honest, I'm really relieved.

# Analysis
# It's a little bit slow, so I'll only make a historical close value Stock Chart for PETR4.SA

all_values = get_all_values_by_stock_name('PETR4.SA')

date = []
close_price_value = []

for tuple in all_values:
    date.append(tuple[0])
    close_price_value.append(tuple[1])

plt.plot(date, close_price_value)
plt.ylabel('Close Price Value')
plt.xlabel('Date')
plt.show()

# Such a beautiful plot, right?

# Finally, it's time to end the DB Connection. See you later!

conn.close()