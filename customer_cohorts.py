# Required library
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import mysql.connector
import seaborn as sns

# Mysql Connection & Query
con = mysql.connector.connect(user = 'urusername', password = 'urpassword', host = '172.18.8.91', database = 'db_name')
query = "SELECT actual_customer_name, customer_classification, customer_onboard_qtr AS OnboardQuarter, qtr_name AS Quarter, ROUND(SUM(gmv),0) AS gmv, COUNT(order_id) AS orders, ROUND(SUM(demand_discount),0) AS demand_discount FROM zinka.orders_snapshot WHERE qtr <=15 GROUP BY actual_customer_name, customer_classification, customer_onboard_qtr, qtr_name"
df = pd.read_sql(query, con = con)
df.tail()

grouped = df.groupby(['OnboardQuarter', 'Quarter'])

# count the unique users, orders, and total revenue per Group + Period
cohorts = grouped.agg({
  'actual_customer_name': pd.Series.nunique,
  'gmv': np.sum,
  'orders': np.sum,
  'demand_discount': np.sum
})

# make the column names more meaningful
renaming = {
  'actual_customer_name': 'TotalCustomers',
  'gmv': 'TotalGMV',
  'orders': 'TotalOrders',
  'demand_discount': 'TotalDiscount'
}
cohorts = cohorts.rename(columns = renaming)
cohorts.head()

def cohort_period(df):

#Creates a `CohortPeriod` column, which is the Nth period based on the customer 's first transacting qtr.

df['CohortPeriod'] = np.arange(len(df)) + 1
return df

cohorts = cohorts.groupby(level = 'OnboardQuarter').apply(cohort_period)
cohorts.head(15)

cohorts = cohorts.reset_index()
cohorts = cohorts.set_index(['OnboardQuarter', 'Quarter'])
cohorts.head()

cohorts_size = cohorts['TotalCustomers'].groupby(level = 'OnboardQuarter').first()
cohorts_size.head(15)

#applying it
user_retention = (cohorts['TotalCustomers'].unstack('OnboardQuarter').divide(cohorts_size, axis = 1))
user_retention.head(15)

#Graph
#Change default figure and font size
plt.rcParams['figure.figsize'] = 20, 8
plt.rcParams['font.size'] = 12

user_retention.plot()
plt.title('Cohorts: User Retention')
plt.xticks(range(1, 13))
plt.xlim(1, 15)
plt.ylabel('% of Cohort Customers')
plt.show()

#Final Retention Cohorts
sns.set(style = 'white')

# plt.figure(figsize = (15, 8))
plt.title('Cohorts: Customers Retention')
sns.heatmap(user_retention.T,
  cmap = plt.cm.RdYlGn,
  mask = user_retention.T.isnull(), #data will not be shown where it 's True
  annot = True, #annotate the text on top 
  fmt = '.0%') #string formatting when annot is True
plt.show()


