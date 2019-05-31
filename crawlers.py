import requests
import json
import pandas as pd
import pymysql as msql
import sqlalchemy
import numpy as np
from datetime import datetime


endpoint = 'https://cosalon.pos365.vn/api/'

mysql_engine = sqlalchemy.create_engine("mysql+pymysql://root_1:vela_root1@13.251.234.97:3306/stageposi")
mysql_con = mysql_engine.connect()


def get_Authen_Cookies(endpoint, username, password):
    
    login_detail = {'Username': username,
                   'Password': password}
    
    r = requests.post(endpoint + 'auth/credentials?format=json', data = login_detail)
    
    reponse_json = r.json()
    
    res = {'ss-id' : reponse_json['SessionId']}
    
    return res


cookies = get_Authen_Cookies(endpoint, 'admin', 'Vela2019@')
def convert_empty_to_none(x):
    if type(x) == list:
        if not x:
            return None
        return x
    return x

convert_empty_to_none = np.vectorize(convert_empty_to_none)

def get_group_ids(groups):
    if len(groups) == 0:
        return None
    res = '-'.join([str(gr['GroupId']) for gr in groups])
    return res



def etl_data(endpoint,table_name):
    global cookies
    if table_name == 'order_to_product':
        list_order_sql = 'Select distinct `Id` from cosalon.orders'
        list_order_id = pd.read_sql(list_order_sql, con = mysql_con)
        list_order_id = list_order_id['Id'].values
        full_df = pd.DataFrame()
        for order_id in list_order_id:
            request_url = f'https://cosalon.pos365.vn/api/orders/detail?format=json&OrderId={order_id}'
            r = requests.get(request_url, cookies = cookies)
            if r.status_code == 401:
                cookies = get_Authen_Cookies(endpoint, 'admin', 'Vela2019@')
                r = requests.get(request_url, cookies = cookies)
            res = r.json()['results']
            df = pd.DataFrame(res)
            full_df = pd.concat([full_df, df])
        
        full_df.to_sql(name = 'order_to_product', con = mysql_con, schema = 'cosalon', if_exists = 'replace', index=False)
        return "DONE"

    if table_name == 'group_partners':
        request_url = f'https://cosalon.pos365.vn/api/groups/treeview?Type=1&format=json'
        r = requests.get(request_url, cookies = cookies)
        if r.status_code == 401:
            cookies = get_Authen_Cookies(endpoint, 'admin', 'Vela2019@')
            r = requests.get(request_url, cookies = cookies)
        full_df = pd.DataFrame()
        res = r.json()
        df = pd.DataFrame(res)
        full_df = pd.concat([full_df, df])
        
        full_df.to_sql(name = 'group_partners', con = mysql_con, schema = 'cosalon', if_exists = 'replace', index=False)
        return "DONE"

    r = requests.get(url = endpoint + f'/{table_name}?format=json&Type=1&top=100&skip=0', cookies=cookies)    
    if r.status_code == 401:
        cookies = get_Authen_Cookies(endpoint, 'admin', 'Vela2019@')
        r = requests.get(url = endpoint + f'/{table_name}?format=json&Type=1&top=100&skip=0', cookies=cookies)    

    
    res = r.json()['results']
    df = pd.DataFrame(res)

    if table_name == 'orders':
        df['PartnerId'] = df.PartnerId.fillna(-1)
        df['PartnerId'] = df['PartnerId'].astype(int)
    if table_name == 'partners':
        df['GroupIds'] = df['PartnerGroupMembers'].map(get_group_ids)

    df = df.applymap(str)
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = convert_empty_to_none(df[col])
    
    if table_name == 'partners':
        more_columns = ['Description', 'Address', 'Department','Phone2', 
                        'Province', 'ShippingAddress', 'TaxCode', 'Phone']
        for col in more_columns:
            if col not in df.columns:
                df[col] = None
    
    if table_name == 'products':
        more_columns = ['Coefficient', 'IsTimer', 'Code2']
        for col in more_columns:
            if col not in df.columns:
                df[col] = None
                
    if table_name == 'accountingtransaction':
        more_columns = ['ReturnId', 'ModifiedBy', 'ModifiedDate']
        for col in more_columns:
            if col not in df.columns:
                df[col] = None
    
    
    df.to_sql(name = f"{table_name}", con = mysql_con, schema = 'cosalon', if_exists = 'replace', index=False)
    
    i = 1
    while df.shape[0] == 100:
        r = requests.get(url = endpoint + f'/{table_name}?format=json&Type=1&top=100&skip={i * 100}', cookies=cookies)
        if r.status_code == 401:
            cookies = get_Authen_Cookies(endpoint, 'admin', 'Vela2019@')
            r = requests.get(url = endpoint + f'/{table_name}?format=json&Type=1&top=100&skip=0', cookies=cookies)

        res = r.json()['results']
        df = pd.DataFrame(res)

        if table_name == 'orders':
            df['PartnerId'] = df.PartnerId.fillna(-1)
            df['PartnerId'] = df['PartnerId'].astype(int)
        if table_name == 'partners':
            df['GroupIds']= df['PartnerGroupMembers'].map(get_group_ids) 
        df = df.where(pd.notnull(df), None)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = convert_empty_to_none(df[col])
        df = df.applymap(str)
        df.to_sql(name = f"{table_name}", con = mysql_con, schema = 'cosalon', if_exists = 'append', index=False)
        i = i + 1    
    
    return 'DONE'


table_list = ['orders','partners','products','accountingtransaction','order_to_product', 'group_partners', 'users']



for table in table_list:
    res = etl_data(endpoint, table)
    print(res, table, datetime.now())
