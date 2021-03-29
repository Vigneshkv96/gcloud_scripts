##### Import Section ######
try:
   from google.cloud import bigquery
   from google.oauth2 import service_account
   import pandas,argparse
except Exception as e:
    print("ErrorCode:   01")
    print("ErrorMsg:    Import Error")
    print("Error:   str(e)")


######## Arguments Section ####
parser = argparse.ArgumentParser()
parser.add_argument("-projectid", "--projectid", help = "Project Id (bigquery)")
parser.add_argument("-dataset", "--dataset", help = "Dataset (bigquery)")
parser.add_argument("-tablename","--tablename", help = "TableName (bigquery)")
args = parser.parse_args()
###############################

##### variable section ####
credentials = table = client = sql_query = queried_table = None
sql_query = "select * from `{0}.{1}.{2}` limit 10".format(args.projectid,args.dataset,args.tablename)
###########################

######### Method section #######
def readbq():
    credentials = service_account.Credentials.from_service_account_file('/home/ubuntu/testing.json')
    client = bigquery.Client(credentials= credentials,project=args.projectid)
    queried_table =client.query(sql_query).to_dataframe()
    return queried_table

def main():
    table = readbq()
    print(table)

if __name__ == "__main__":
        main()
