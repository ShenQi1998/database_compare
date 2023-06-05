import cx_Oracle as cx  
import time

#which table to comapre
#比较的表
tablesName = ["TABLE1","TABLE2"]

#the field not to compare
#不参与比较的字段
uselessField = ["COLUMN1","COLUMN2","COLUMN3"]   


timeStamp = str(int(time.time()))  #秒级别时间戳  get time stamp
filePath = r"C:/" + timeStamp  + ".txt"
File = open(filePath, "w",encoding='utf-8')

#to write local file or print to console
#打印控件
def sysPrint(str,i):
    print(str)
    if i==1:
        File.write(str +"\n")
        
# to format
def fieldFormat(data,a):
    data = str(data)
    if a==0:
        data = data.replace('(','').replace(')','').replace(',','').replace("'",'')
    else:
        data = data.replace('(','').replace(')','').replace(',','')
    return data

#get table name 
def getTableName_ch(con,tableName):
    cursor = con.cursor()
    sql = "select COMMENTS from user_tab_comments where table_name='" + tableName +  "'"
    cursor.execute(sql)  #执行sql语句  execute sql
    data = cursor.fetchone()
    if(data != None):
        data = fieldFormat(data,0)
    else:
        data = " "
    return data

# get table column
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

# get column describe
def getFieldName_ch(con,tableName,fieldName):
    cursor = con.cursor() 
    sql = " select COMMENTS from user_col_comments where table_name='" + tableName +  "'and COLUMN_NAME ='" + fieldName + "'"
    cursor.execute(sql)
    data = cursor.fetchone()
    if(data != None):
        data = fieldFormat(data,0)
    else:
        data = " "
    return data 

#get unique
def getUnique (con,tableName):
    unique = []
    cursor = con.cursor()
    sql = "select INDEX_NAME from user_indexes where table_name='"+ tableName + "' and UNIQUENESS = 'UNIQUE'"
    cursor.execute(sql) 
    data = cursor.fetchone()
    if(data is not None):
        data = fieldFormat(data,1)
        sql = "select COLUMN_NAME from user_ind_columns where index_name = " + data + " order by COLUMN_POSITION"
        cursor.execute(sql)
        for a in cursor:
            a= fieldFormat(a,0)
            unique.append(a)
    else:
        # raise Exception("无发获取表[" + tableName + "]唯一索引")
        raise Exception("can't get table [" + tableName + "] unique")
    cursor.close()  #关闭游标
    return unique


#############################################################################
# database one connection                                                   #
# database two connection                                                   #
# tablename                                                                 #
# unique                                                                    #
# tablename's column                                                        #
# parameter 0 : database one compare to database two                        #
#           1 : database two compare to database one                        #
#############################################################################
def compare(con_main,con_spare,tablesName,uniques,fields,canshu):
    cursor_main = con_main.cursor()
    cursor_spare = con_spare.cursor()
    lossCount = 0

    tablesName_ch = getTableName_ch(con_main,tablesName)        #get table desc

    sql = "select "  
    for unique in uniques:
        sql = sql + unique + ","
    sql = sql[:-1] + " from " + tablesName 

    cursor_main.execute(sql)                                    #execute
    uniqueData = cursor_main.fetchall()
    for a in uniqueData:

        sql = "select "
        for field in fields:
            sql = sql + field + ","
        sql = sql[:-1] + " from " + tablesName  + ' where ' 

        condition = ""
        for i,unique in enumerate(uniques):
            if(str(a[i]) != 'None'):
                condition = condition + unique + "='" +  str(a[i]) + "' and "
            else:
                condition = condition + unique + " is bull and " 
        condition = condition[:-5] 
        sql = sql + condition
        data_main = cursor_main.execute(sql).fetchone()
        data_spare = cursor_spare.execute(sql).fetchone()

        if(canshu == 0):
            if(data_spare is not None):
                wrongField = ""
                wrongField_ch = ""
                for i in range(0,len(data_main)):
                    if data_main[i] != data_spare[i]:
                        wrongField = wrongField + fields[i] + ","
                        FieldName_ch = getFieldName_ch(con_main,tablesName,fields[i])
                        wrongField_ch = wrongField_ch + FieldName_ch + ","
                if(len(wrongField)>0):
                    sql_worng = "select " + wrongField[:-1] + " from " + tablesName + " where " + condition + ";"
                    # P = "表 " + tablesName + "(" + tablesName_ch + ") 字段 " + wrongField[:-1] + "(" + wrongField_ch[:-1] + ") 不同 >>>>> SQL:\n" + sql_worng  + "\n"
                    P = "table (" + tablesName + ") colunm (" + tablesName_ch + ")" + wrongField[:-1] + "(" + wrongField_ch[:-1] + ") 不同 >>>>> SQL:\n" + sql_worng  + "\n"
                    sysPrint(P,1) 
            else:
                lossCount = lossCount + 1
                sql_worng = "select * from " + tablesName + " where " + condition + ";"
                # P = "表 " + tablesName + " 记录为空 >>>>> SQL:\n" + sql_worng + "\n"
                P = "table (" + tablesName + ") is empty >>>>> SQL:\n" + sql_worng + "\n"
                sysPrint(P,1)
        else:
            if(data_spare is None):
                P="select * from " + tablesName + " where " + condition + ";"
                sysPrint(P,1)

    if(canshu == 0):
        countAll_main = cursor_main.execute("select count(*) from " + tablesName ).fetchone()[0]
        countAll_spare = cursor_spare.execute("select count(*) from " + tablesName ).fetchone()[0]
        count = countAll_spare + lossCount - countAll_main
        #P = "表 " + tablesName + " 比参数表多出了" + str(count) + "条数据,他们为"
        P = "table " + tablesName + " is " + str(count) + "more than main table , they are "
        sysPrint(P,1)
        if(count != 0):
            compare(con_spare,con_main,tableName,uniques,fields,1)
        sysPrint("" , 0)

    cursor_main.close()                                                             #关闭游标  close cursor
    cursor_spare.close()                                                            #关闭游标 close cursor


if __name__=="__main__":

    # oracle instan client , need change to local folder
    # 此处应修改为本机路径
    cx.init_oracle_client(lib_dir=r"D:\software\PLSQL\instantclient_11_2")

    #database link 
    #Format: username/password@ip:port/instance name
    #格式： 用户名/密码@ip:端口/实例名
    con_main = cx.connect('NCBSCO/GyIs#02n@188.177.167.250:1521/cbsdb')  
    con_spare = cx.connect('NCBSCO/GyIs#02n@188.177.171.201:1521/cbsdb') 

    sysPrint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Compare Begin >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n",0)
    for tableName in tablesName:
        sysPrint("\n>>>>>>>>>>>>>>>>>>>>>>>>> Compare " + tableName + " Begin >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n",1)
        uniques = getUnique(con_main,tableName) #获取表的唯一索引    get table uniques
        fields = getTableField(con_main,tableName) #获取表的所有字段  get table all column 
        compare(con_main,con_spare,tableName,uniques,fields,0) # to compare
    sysPrint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Compare Finish >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n",0)
    
    con_main.close()     #关闭数据库连接   Close local database 1   
    con_spare.close()    #关闭数据库连接   Close local database 2   
    
    File.close()         #关闭本地文件 close local file
