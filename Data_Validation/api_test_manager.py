import requests
import comparison_helper
import pandas as pd
import db_helper


def diff(list1, list2):
    c = set(list1).union(set(list2))  # or c = set(list1) | set(list2)
    d = set(list1).intersection(set(list2))  # or d = set(list1) & set(list2)
    return list(c - d)

resp = requests.get('http://api-int.dit.connectcdk.com/api/ds-flex-accounting/v1/purchase-orders/1/1309',
                     headers={'Content-Type':'application/json',
                              'x-store-id':'S000000109',
                              'x-enterprise-id':'E000109',
                              'REMOTE_USER':'sys-user-driveflex-admin',
                              'Authorization': 'Bearer d453c0d4-7011-48c4-9784-ad6b28d8c60e'})

print (resp)
json_response = resp.json()
list_array = json_response['detailLines']
new_list_array = pd.DataFrame.from_records(list_array)
print(json_response)


api_response = pd.DataFrame.from_records(json_response)
api_response.columns = api_response.columns.str.lower()
new_list_array.columns = new_list_array.columns.str.lower()
api_response = api_response.drop('detaillines', axis=1)
new_frame = [api_response,new_list_array]
result = pd.concat(new_frame, axis=1, sort=False)
api_response = result
print(api_response)
#api_response = api_response.append(new_list_array)
#print(api_response)
#print(new_list_array)


DB = db_helper.DbHelper
con_string = DB.generate_connection_string(DB,'postgresql','root','jJU0BtCjA-d9yQ',
                                    'dmsf-nac-dev-aurora-postgresql.cluster-ro-cmp1t6mgjvfd.us-west-2.rds.amazonaws.com','3306','postgres', '123')
DB.create_conn(DB,con_string)
postgres_response = DB.query_execution(DB,
                                       "select * from qa.accounting_fpo_order a inner join qa.accounting_detail_line b on a.accounting_fpo_order_id = b.accounting_fpo_order_id inner join qa.vendor on a.vendor_id = vendor.vendor_id  where a.po_number = '1309' and a.company_id = '1'")
postgres_response.columns = postgres_response.columns.str.replace('_', '')
print(postgres_response.to_string())

postgres_missing = list(set(postgres_response.columns) - set(api_response.columns))
api_missing = list(set(api_response.columns) - set(postgres_response.columns))
print(postgres_missing)
print(api_missing)
#api_response.set_option('display.max_rows', 500)
#api_response.set_option('display.max_columns', 500)
#api_response.set_option('display.width', 1000)
#print(api_response)

"""""
postgres_response["checkamount"] = postgres_response["checkamount"].round(1)
postgres_response["discountamount"] = postgres_response["discountamount"].round(1)
api_response["checkamount"] = api_response["checkamount"].apply(pd.to_numeric)
api_response["discountamount"] = api_response["discountamount"].apply(pd.to_numeric)

api_response["checkamount"] = api_response["checkamount"].round(1)
api_response["discountamount"] = api_response["discountamount"].round(1)
"""
# drop = diff(list(postgres_response.columns), list(api_response.columns))

postgres_response = postgres_response.drop(postgres_missing, axis=1)

print(postgres_response)
api_response = api_response.drop(api_missing, axis=1)
print(postgres_response.to_string())
print(api_response.to_string())

print(comparison_helper.compare_dataframes(postgres_response,api_response))
DB.close_conn(DB)