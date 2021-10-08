import cx_Oracle as cx  
import time
 
uselessField = []

timeStamp = str(int(time.time()))  #秒级别时间戳
filePath = r"C:/" + timeStamp  + ".txt"
File = open(filePath, "w",encoding='utf-8')

def sysPrint(str,i):
    #pass
    print(str)
    if i==1:
        File.write(str +"\n")
        
def fieldFormat(data,a):
    data = str(data)
    if a==0:
        data = data.replace('(','').replace(')','').replace(',','').replace("'",'')
    else:
        data = data.replace('(','').replace(')','').replace(',','')
    return data

# get table colunm
def getTableField (con,tableName):
    fields = []
    cursor = con.cursor()      
    sql = "select column_name from user_tab_columns where table_name='" + tableName +  "'order by column_name"
    cursor.execute(sql)  
    data = cursor.fetchall()
    for a in data:
        a= fieldFormat(a,0)
        if a not in uselessField:
            fields.append(a)
    cursor.close()
    return fields

#get table name 
def getTablesName(con):
    tables = []
    cursor = con.cursor()
    sql = "SELECT TABLE_NAME FROM USER_TABLES"
    cursor.execute(sql)
    data = cursor.fetchall()
    for a in data:
        a= fieldFormat(a,0)
        if a not in uselessField:
            tables.append(a)
    return tables
    

if __name__=="__main__":
    cx.init_oracle_client(lib_dir=r"D:\software\PLSQL\instantclient_11_2")

    con_main = cx.connect('NCBSCO/DlXi*01o@188.177.156.125:1521/devel2')  #参数
    con_spare = cx.connect('ncbsco/GyIs#02n@188.177.163.240:1521/CBSDB')  #SIT

    sysPrint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>脚本开始执行>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n",0)

    tablesName = getTablesName(con_main)
    for tableName in tablesName:
        sysPrint("\n>>>>>>>>>>>>>>>>>>>>>>>>>开始比较表" + tableName + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n",1)
        fields_main = getTableField(con_main,tableName)
        fields_spare = getTableField(con_spare,tableName)
        if fields_main != fields_spare:
            sysPrint(tableName,1)
    sysPrint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>脚本执行完毕>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n",0)
    print("end")
    con_main.close()
    con_spare.close()
    
    File.close()


'''
    更新日期
    20210616 V1.0 初版
'''