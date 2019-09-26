# Required library
import pandas as pd
import numpy as np
import mysql.connector

# Mysql Connection & Query
con = mysql.connector.connect(user = 'urusername', password = 'urpassword', host = '172.18.8.91', database = 'db_name')
query = "SELECT * FROM base_order_v9 WHERE year>=2017 AND qtr <= 15 "
base_order = pd.read_sql(query, con=con)

grouped = base_order.groupby(['from_city', 'to_city','from_cluster','to_cluster','qtr_name'])

# Aggregate on grouped level
lane = grouped.agg({'revenue': np.sum,
                       'gmv': np.sum, 
                       'base_cost': np.sum,
                       'cost': np.sum,
                       'tiger_tonnage': np.sum,
                       'actual_sp_number': pd.Series.nunique,
                       'registration_number': pd.Series.nunique,
                       'order_id': pd.Series.count
                      })
                      
lane.to_csv('lane_data_qtr.csv')
