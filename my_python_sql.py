#check_install() function checks that user has proper installed and imported packages in its environment
def check_install():
    import importlib
    import subprocess
    import sys
    import os.path
    from os import path
    global pd, np, pyodbc, path, sys
    try:
        import pandas as pd
    except ImportError:
        print("{0} has to be installed\n".format('pandas'))            
        subprocess.call([sys.executable, '-m', 'pip', 'install', 'pandas'])
    finally:
        import pandas as pd
    try:
        import numpy as np
    except ImportError:
        print("{0} has to be installed\n".format('numpy'))            
        subprocess.call([sys.executable, '-m', 'pip', 'install', 'numpy'])
    finally:
        import numpy as np
    try:
        import pyodbc
    except ImportError:
        print("{0} has to be installed\n".format('pyodbc'))            
        subprocess.call([sys.executable, '-m', 'pip', 'install', 'pyodbc'])
    finally:
        import pyodbc as pyodbc    
    print("Environment has been properly constructed.\n")

#connect_database() function connects to the database and manages connection errors    
def connect_database(my_server="LAPTOP-1NPS6A7O", my_database="Survey_Sample_A19"):
    global sql_conn #we create a global variable for sql connection
    
    pyodbc.pooling = False #this parameter makes the connection to be effectively closed on the server when you delete it
    
    my_string='DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+my_server+';DATABASE='+my_database+';Trusted_Connection=yes'
    try:
        sql_conn=pyodbc.connect(my_string)
        print("Connection to database {0} on Server {1} succeeded.\n".format(my_database,my_server))
        return sql_conn
    
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        if sqlstate == '28000':
            print("LDAP Connection failed: check password. \n")
        else:
            print("Connection failed. Error is {0} \n".format(str(ex.args)))
        sys.exit('Database connection issue \n') #if no connection possible, we stop the program

# my_sq_cursor2() function defines a SQL command string to be used in cursor2 to fetch on each survey ID which of the questions are present
def my_sq_cursor2(currentSurveyId):
    my_string= 'SELECT * FROM ( SELECT SurveyId,QuestionId,1 as InSurvey FROM SurveyStructure WHERE SurveyId ='+str(currentSurveyId)
    my_string=my_string+' UNION SELECT '+str(currentSurveyId) +' as SurveyId, Q.QuestionId, 0 as InSurvey FROM Question as Q'
    my_string=my_string+' WHERE NOT EXISTS(SELECT * FROM SurveyStructure as S WHERE S.SurveyId = '+str(currentSurveyId)
    my_string=my_string+' AND S.QuestionId = Q.QuestionId)) as t ORDER BY QuestionId'
    return my_string

#survey_structure() function reports the survey structure based on cursor fetching on the SQL survey database
def survey_structure(my_sql_connection):
    cursor1=my_sql_connection.cursor()
    try:
        cursor1.execute("SELECT SurveyId FROM Survey ORDER BY SurveyId")
        my_final_data=[0,0,0]
        for surveyId in cursor1.fetchall():
            my_surveyId=surveyId[0]
            cursor2=my_sql_connection.cursor()
            cursor2.execute(my_sq_cursor2(my_surveyId))
            for currentSurveyIdInQuestion, currentQuestionID, currentInSurvey in cursor2.fetchall():
                my_data=np.array([currentSurveyIdInQuestion, currentQuestionID, currentInSurvey])
                my_final_data=np.vstack((my_final_data,my_data))
        my_survey_structure=pd.DataFrame(my_final_data[1:,:]).astype(int)
        my_survey_structure.columns=['SurveyId','QuestionID','QuestionInSurvey (1=YES,0=NO)']
        return my_survey_structure
    except:
        sys.exit("Error occured while accessing data. Please check connection information. \n")
    finally: 
        cursor1.close()
        cursor2.close()

# build_strColumnsQueryPart() function builds the SQL string to check questions for each survey and collect the respose data
def build_strColumnsQueryPart(currentQuestionID, currentInSurvey):
    strQueryTemplateForAnswerColumn = ' COALESCE((SELECT a.Answer_Value FROM Answer as a WHERE a.UserId = u.UserId AND a.SurveyId = <SURVEY_ID> AND a.QuestionId = <QUESTION_ID>), -1) AS ANS_Q<QUESTION_ID> ';
    strQueryTemplateForNullColumnn = ' NULL AS ANS_Q<QUESTION_ID> '
    if currentInSurvey == 0 :#CURRENT QUESTION IS NOT IN THE CURRENT SURVEY
        strColumnsQueryPart = strQueryTemplateForNullColumnn.replace('<QUESTION_ID>',str(currentQuestionID))
    else:
        strColumnsQueryPart =strQueryTemplateForAnswerColumn.replace('<QUESTION_ID>',str(currentQuestionID))
    return str(strColumnsQueryPart)

# build_strCurrentUnionQueryBlock() function unions the independent queries built for each survey with the build_strColumnsQueryPart() function
def build_strCurrentUnionQueryBlock(currentSurveyId,strColumnsQueryPart):
    strQueryTemplateOuterUnionQuery = ' SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> FROM [User] as u WHERE EXISTS (SELECT * FROM Answer as a WHERE u.UserId = a.UserId AND a.SurveyId = <SURVEY_ID>)'
    strCurrentUnionQueryBlock = ''
    strCurrentUnionQueryBlock=strQueryTemplateOuterUnionQuery.replace('<DYNAMIC_QUESTION_ANSWERS>', str(strColumnsQueryPart))
    strCurrentUnionQueryBlock=strCurrentUnionQueryBlock.replace('<SURVEY_ID>', str(currentSurveyId))
    return str(strCurrentUnionQueryBlock)

#build_final_query() function iterates over the survey structure to build the final query using subfonctions /
#build_strColumnsQueryPart() and build_strCurrentUnionQueryBlock()
def build_final_query(my_survey_structure):
    question_id_list=my_survey_structure['QuestionID'].unique()
    max_question=np.max(question_id_list)
    survey_id_list=my_survey_structure['SurveyId'].unique()
    max_survey=np.max(survey_id_list)
    strFinalQuery=''
    for my_survey_id in survey_id_list:
        my_string_1=''
        for my_question_id in question_id_list:
            my_currentInSurvey=my_survey_structure[(my_survey_structure['SurveyId']==my_survey_id)&(my_survey_structure['QuestionID']==my_question_id)].iloc[:,2].values
            my_string_1=my_string_1+build_strColumnsQueryPart(my_question_id,my_currentInSurvey)
            if my_question_id <max_question:
                my_string_1=my_string_1 + ' , '
            else:
                my_string_1=my_string_1
        strFinalQuery=strFinalQuery+build_strCurrentUnionQueryBlock(my_survey_id,my_string_1)
        if my_survey_id <max_survey:
            strFinalQuery=strFinalQuery + ' UNION '
        else:
            strFinalQuery=strFinalQuery
    #we save the final Query in a .txt file
    try:
        with open('./my_query.txt', 'w') as f:
            f.write(strFinalQuery)
        print("Final query has been saved as 'my_query.txt' in working directory. \n")
    except:
        sys.exit("Error occured while disk saving the final query. \n")


#survey_struct_exists() function checks that a survey structure has already been stored in the working directory and returns it /
# if no survey_structure file exists, it creates and returns it
def survey_struct_exists(my_sql_connection):
    my_output=1
    my_new_survey=survey_structure(my_sql_connection)
    if path.exists("./last_survey_structure.csv")==True:
        print("Survey structure file exists already. Let's check if it has changed. \n")
        my_old_survey=pd.read_csv("./last_survey_structure.csv",sep=',',header=None, skiprows=1,index_col=0).astype(int)
        my_old_survey.columns=['SurveyId','QuestionID','QuestionInSurvey (1=YES,0=NO)']
        if my_new_survey.equals(my_old_survey)==True:
            print("The survey structure has not changed. \n")
            my_output=1
        else:
            print("The survey structure has changed. We build, save and overwrite the old one. \n")
            my_output=0
            my_new_survey.to_csv('./last_survey_structure.csv', sep=',')
            print("'Last_survey_structure.csv' has been properly created in current directory. \n")
    else:
        print("There is no survey structure file saved. It has been created and saved. \n")
        my_new_survey.to_csv('./last_survey_structure.csv', sep=',')
        my_output=0
        
    #then we get or build the final query depending if the survey structure has changed
    if my_output == 0:
        build_final_query(my_new_survey) #we build final query
        with open('./my_query.txt', 'r') as file:
            my_final_query = file.read().replace('\n', '')   
    else:
        with open('./my_query.txt', 'r') as file:
            my_final_query = file.read().replace('\n', '')     
    return my_final_query

#Main() function orchestrates the job to be performed by calling all usefull functions
def main(my_Server,my_Database):
    
    global sql_conn
    
    print("\nWelcome. This is main function running. \n")
    
    #First we check the packages installed
    check_install()
    
    #Second, we connect to database
    my_conn=connect_database(my_Server, my_Database)
    
    #Third, we check survey structure and get final query (new built or already existing)
    my_final_query=survey_struct_exists(my_conn)
    
    #Fourth, we make the sql query to get the All survey data and save them on "AllSurveydata.csv" file in working directory
    df = pd.read_sql(my_final_query, my_conn)
    df.fillna(value=pd.np.nan, inplace=True)
    df.to_csv("./AllSurveydata.csv")
    print("Please find the 10 first rows of database hereunder. \n")
    print(df.head(10),'\n')
    
    print("Job completed. 'AllSurveydata.csv' has been saved in working directory. \n")
    
    #we close SQL connection and delete it
    sql_conn.close()
    del sql_conn
    print("The connection to the server has been properly closed and deleted. \n")

if __name__ == '__main__':
    theServer=input("What is the SQL SERVER name ? ") #user input interface to get the SERVER name
    theDatabase=input("What is the SQL DATABASE name ? ")#user input interface to get the DATABASE name
    main(theServer,theDatabase)