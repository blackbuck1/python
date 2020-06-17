#!/usr/bin/python
# -*- coding: utf-8 -*-
# import mysql.connector

import pandas as pd
from pyhive import presto
import numpy as np

# Sqlconnector for DbConnection

con_zinka = mysql.connector.connect(user='public', password='password',
                                    host='XX.XXX.XXX.XXX',
                                    database='zlog')
con_presto = presto.connect(host='XX.XXX.XXX.XXX', port=8080)

qry_trucks = 'select id, truck_no from divum.blackbuck.fleetapp_truck'
qry_txn_gps = \
    "select date_format(created_on + interval '5' HOUR + interval '30' MINUTE, '%Y%m') AS txn_month, truck_number, SUM(paid_amount) AS gps_amount from divum.blackbuck.gps_subscription_txns group by 1,2"
qry_wallet = \
    """select date_format(w.updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') AS txn_month,
                 truck_id,
                 status,
                 SUM(amount) AS recharge_amount
                 from services_payment.blackbuck.wallet_wallettransactionhistory
                 where status IN ('HPCL-Card-Recharge', 'HPCLCardless', 'Reliance-Card-Recharge', 'RelianceCardless', 'Card-Recharge', 'FasTag Recharge', 'GPS subscription')
                 group by 1,2,3
              """.replace('\n'
        , ' ').replace('\t', '')

#get all trucks where truck_number is not blank
all_trucks = pd.read_csv('all_services_trucks.csv')
trucks = all_trucks[all_trucks.truck_no == ''].count()

#transform gps data into format
gps_txn = pd.read_csv('gps_txn_data.csv')
gps = gps_txn[gps_txn.gps_amount > 0]
gps.count()
gps['gps_amount'].sum()
gps['fuel_amount'] = 0
gps['toll_amount'] = 0
gps.head()

#join wallet_history to trucks on truck_id
wallet_history = pd.read_csv('wallet_txn_data.csv')
wallet_history.count()
wallet_history['recharge_amount'].sum()
wallet = wallet_history
df = wallet.merge(all_trucks, left_on='truck_id', right_on='id',
                  how='left')

#add additional column for gps, fuel & toll
df1 = df.iloc[:, [0, 5, 2, 3]]
df1['gps_amount'] = np.where(df1['status'] == 'GPS subscription',
                             df1['recharge_amount'], 0)
df1['fuel_amount'] = np.where((df1['status'] != 'FasTag Recharge')
                              & (df1['status'] != 'GPS subscription'),
                              df1['recharge_amount'], 0)
df1['toll_amount'] = np.where(df1['status'] == 'FasTag Recharge',
                              df1['recharge_amount'], 0)

#summaries data on month & truck level
non_gps = df1.groupby(['txn_month', 'truck_no'
                      ]).agg({'gps_amount': np.sum,
                             'fuel_amount': np.sum,
                             'toll_amount': np.sum})
non_gps = non_gps.reset_index()
non_gps.rename(columns={'truck_no': 'truck_number'}, inplace=True)
non_gps.head()

#merger gps non_gps (fuel & toll)
data = gps
data = data.append(non_gps)

#again summaries data on month & truck level
fdata = data.groupby(['txn_month', 'truck_number'
                     ]).agg({'gps_amount': np.sum,
                            'fuel_amount': np.sum,
                            'toll_amount': np.sum})

fdata = fdata.reset_index()
fdata.head()

#add flag for gps, fuel & toll
fdata['gps'] = np.where(fdata['gps_amount'] > 0, 1, 0)
fdata['fuel'] = np.where(fdata['fuel_amount'] > 0, 1, 0)
fdata['toll'] = np.where(fdata['toll_amount'] > 0, 1, 0)
fdata['txd_category'] = fdata['gps'] + fdata['fuel'] + fdata['toll']
fdata.tail()

#get cross selling between category
final = fdata.groupby(['txd_category', 'txn_month']).agg({
    'truck_number': pd.Series.nunique,
    'gps_amount': np.sum,
    'fuel_amount': np.sum,
    'toll_amount': np.sum,
    })

final = final.reset_index()

final.to_csv('final.csv', index=False)
