##### Import Section ######
try:
   from google.cloud import bigquery
   from google.oauth2 import service_account
   from oauth2client.service_account import ServiceAccountCredentials
   import pandas,argparse,pygsheets
   import gspread_dataframe as gd
   import gspread
   from df2gspread import df2gspread as d2g

except Exception as e:
    print("ErrorCode:   01")
    print("ErrorMsg:    Import Error")
    print("Error:   str(e)")


######## Arguments Section ####
parser = argparse.ArgumentParser()
parser.add_argument("-projectid", "--projectid", help = "Project Id (bigquery)")
parser.add_argument("-dataset", "--dataset", help = "Dataset (bigquery)")
parser.add_argument("-tablename","--tablename", help = "TableName (bigquery)")
parser.add_argument("-filter","--filter",help="Give the Filter field you want query with i.e 1.Description ")
parser.add_argument("-filtervalue", "--filtervalue",help="Give the Filter value for Description filter give value like atm cash")
parser.add_argument("-spreadsheet", "--spreadsheet",help="Give the spreadsheet name which you want create")
args = parser.parse_args()
###############################

##### variable section ####
credentials = table = client = sql_query = queried_table = None
sql_query = "select * from `{0}.{1}.{2}` where {3}='{4}'".format(args.projectid,args.dataset,args.tablename,args.filter,args.filtervalue)
sql = "select * from `{0}.{1}.{2}`".format(args.projectid,args.dataset,args.tablename)

###########################

######### Method section #######
def readbq(condition):
    credentials = service_account.Credentials.from_service_account_file('/home/ubuntu/testing.json')
    client = bigquery.Client(credentials= credentials,project=args.projectid)
    if(condition == "filter"):
        queried_table =client.query(sql_query).to_dataframe()
    else:
        queried_table =client.query(sql).to_dataframe()
    return queried_table
def spreadsheet(table1,table2):
       #authorization
       gc = pygsheets.authorize(service_file='/home/ubuntu/testing.json')
       credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/ubuntu/testing.json')
       res = gc.create(args.spreadsheet)
       sh = gc.open(args.spreadsheet)
       wks = sh.add_worksheet('with filter applied')
       wks1 = sh.add_worksheet('Without Filter')
       table1_row = len(table1)
       table1_col = len(table1.columns)
       table2_row = len(table2)
       table2_col = len(table2.columns)
       wks.set_dataframe(table1,(1,1))
       wks1.set_dataframe(table2,(1,1))
       sh.share('vigneshkv96@gmail.com',)
       return sh.url

def main():
    table_filter = readbq("filter")
    print("*************************Table with Filter {0}:{1} applied****************".format(args.filter,args.filtervalue))
    print(table_filter)
    print(type(table_filter))
    print("*************************Table without Filter ***************************")
    table = readbq("nofilter")
    print(table)
    print("Spreadshet URL is as follows")
    shurl=spreadsheet(table_filter,table)
    print(shurl)




if __name__ == "__main__":
        main()
