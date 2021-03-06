#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import mysql.connector

# Sqlconnector for DbConnection
con = mysql.connector.connect(user='public', password='*******', host='*******', database='db_name')
#Sqlquery for data
query = "SELECT ROW_NUMBER() OVER(ORDER BY month, order_id ASC) AS id, a.* FROM zlog.order_details a WHERE customer_classification='Corporate' and registration_number <> '' "
df = pd.read_sql(query, con=con)
df.head(5)

#Rank for first transacted truck
df['rank'] = df.groupby('registration_number')['id'].rank(ascending=True)
fdf = df[df['rank'] == 1]
corp_acq_cost = fdf[['order_id', 'month', 'rank']]
corp_acq_cost['bonus'] = np.where(corp_acq_cost['month'] == 1, 500,2000)
corp_acq_cost.to_csv('Corporate Truck Acquisition Cost.csv', index=False)
corp_acq_cost.head()
