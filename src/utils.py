from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from pathlib import Path  
import pandas as pd 
import numpy as np
import configparser
import requests

def extract_data():
    url = requests.get('https://livrariadafisica.com.br/busca.aspx?area_id=4&tipo=area').text
    soup = BeautifulSoup(url, 'html.parser')

    books = []
    authors = []
    prices = []

    index = range(0, 30)
    for i in index:
        url_name_books = f'ContentPlaceHolder1_lstBusca_ProdutoControl2_{i}_lnkTitulo_{i}'
        url_name_author =f'ContentPlaceHolder1_lstBusca_ProdutoControl2_{i}_lnkAutor_{i}'
        url_price = f'ContentPlaceHolder1_lstBusca_ProdutoControl2_{i}_lblNossoPreco_{i}'
    
        book = soup.find(id=url_name_books).text
        author = soup.find(id=url_name_author).text
        price = soup.find(id=url_price).text

        books.append(book)
        authors.append(author)
        prices.append(price)

    dic = {'Books': books,
           'Authors': authors,
           'Prices': prices}          

    df = pd.DataFrame(dic)
    filepath = Path('../data/exctract.parquet')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    return df.to_parquet(filepath, index=False)


def transform_data():
    df = pd.read_parquet('../data/exctract.parquet')
    df['Prices'] = df['Prices'].str.replace('R$', '')
    df['Authors'] = df['Authors'].str.lower().str.title()

    filepath = Path('../data/transform.parquet')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    return df.to_parquet(filepath, index=False)

def save_in_db():
    config = configparser.ConfigParser()
    config.read('../database.ini')
    db_params = config['postgresql']
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = create_engine(db_url, echo=True)
    df = pd.read_parquet('../data/transform.parquet')
    return df.to_sql('astronomia', engine, if_exists='replace', index=False)






