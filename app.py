import sqlite3
import yfinance as yf
from company import Company
from stock_value import StockValue

conn = sqlite3.connect(':memory:')

c = conn.cursor()

# Key names -> pode duas palavras?
# Foreign key: Stock Name

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
            value real
            )""")

# Is it scalable?
# Insert new fields -> (2) flexible
 
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

## Get data from databases
## Companies DB
#
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

def get_stock_values_by_stock_name(stock_name):
    c.execute("SELECT * FROM stock_values WHERE stock_name=:stock_name", {'stock_name': stock_name})
    return c.fetchall()

def get_stock_values_by_date(date):
    c.execute("SELECT * FROM stock_values WHERE date=:date", {'date': date})
    return c.fetchall()

def get_stock_values_by_variable(variable):
    c.execute("SELECT * FROM stock_values WHERE variable=:variable", {'variable': variable})
    return c.fetchall()

# Update data in databases
# Companies DB

def update_stock_name_in_companies(company, stock_name):
    with conn:
        c.execute("""UPDATE companies SET stock_name = :stock_name
                    WHERE company_name = :company_name AND exchange = :exchange AND currency = :currency""",
                  {'stock_name': stock_name, 'company_name': company.company_name,
                   'exchange': company.exchange, 'currency': company.currency})

def update_company_name_in_companies(company, company_name):
    with conn:
        c.execute("""UPDATE companies SET company_name = :company_name
                    WHERE stock_name = :stock_name AND exchange = :exchange AND currency = :currency""",
                  {'stock_name': company.stock_name, 'company_name': company_name,
                   'exchange': company.exchange, 'currency': company.currency})

def update_exchange_in_companies(company, exchange):
    with conn:
        c.execute("""UPDATE companies SET exchange = :exchange
                    WHERE stock_name = :stock_name AND company_name = :company_name AND currency = :currency""",
                  {'stock_name': company.stock_name, 'company_name': company.company_name,
                   'exchange': exchange, 'currency': company.currency})

def update_currency_in_companies(company, currency):
    with conn:
        c.execute("""UPDATE companies SET currency = :currency
                    WHERE stock_name = :stock_name AND company_name = :company_name AND exchange = :exchange""",
                  {'stock_name': company.stock_name, 'company_name': company.company_name,
                   'exchange': company.exchange, 'currency': currency})

# Update column in Stock Values DB

def update_stock_name_in_stock_values(stock_value, stock_name):
    with conn:
        c.execute("""UPDATE stock_values SET stock_name = :stock_name
                    WHERE date = :date AND variable = :variable AND value = :value""",
                  {'stock_name': stock_name, 'date': stock_value.date,
                   'variable': stock_value.variable, 'value': stock_value.value})

def update_date_in_stock_values(stock_value, date):
    with conn:
        c.execute("""UPDATE stock_values SET date = :date
                    WHERE stock_name = :stock_name AND exchange = :exchange AND currency = :currency""",
                  {'stock_name': stock_value.stock_name, 'date': date,
                   'exchange': stock_value.exchange, 'currency': stock_value.currency})

def update_variable_in_stock_values(stock_value, variable):
    with conn:
        c.execute("""UPDATE stock_values SET exchange = :exchange
                    WHERE stock_name = :stock_name AND company_name = :company_name AND currency = :currency""",
                  {'stock_name': stock_value.stock_name, 'company_name': stock_value.company_name,
                   'exchange': variable, 'currency': stock_value.currency})

def update_value_in_stock_values(stock_value, value):
    with conn:
        c.execute("""UPDATE stock_values SET value = :value
                    WHERE stock_name = :stock_name AND date = :date AND variable = :variable""",
                  {'stock_name': stock_value.stock_name, 'date': stock_value.date,
                   'variable': stock_value.variable, 'value': value})

comp_1 = Company('PETR4', 'Petróleo Brasileiro SA Petrobras', 'B3', 'BRL')

insert_company(comp_1)

stock_value_1 = StockValue('PETR4', '2022-08-08', 'ClosePrice', 28.89)

insert_stock_value(stock_value_1)

companies = get_companies_by_stock_name('PETR4')
stock_values = get_stock_values_by_stock_name('PETR4')

print(companies)
print(stock_values)

update_currency_in_companies(comp_1, 'USD')
update_value_in_stock_values(stock_value_1, 100)

print(companies)
print(stock_values)

## Remove data from databases
##
##def remove_emp(emp):""
##    with conn:
##        c.execute("DELETE from employees WHERE first = :first AND last = :last",
##                  {'first': emp.first, 'last': emp.last})
##
##
##emp_1 = Employee('John', 'Doe', 80000)
##emp_2 = Employee('Jane', 'Doe', 90000)
##
##insert_emp(emp_1)
##insert_emp(emp_2)
##
##emps = get_emps_by_name('Doe')
##print(emps)
##
##update_pay(emp_2, 95000)
##remove_emp(emp_1)
##
##emps = get_emps_by_name('Doe')
##print(emps)
##
conn.close()