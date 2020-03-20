# python_for_SQL
python script to make dynamic SQL queries

1. a jupyter notebook describes all the construction steps and the objectives of the program  

2. a .py script can be run from your favourite python execution platform to run SQL queries on survey  

3. results files that are saved in the working directory:  
  - final_query.txt = the final SQL query string in a .txt file  
  - last_Survey_Structure.csv = the last survey structure file saved as a csv. It lists question IDs for each Survey ID. It is created each time a connection to the survey database is made in order to check if any changes have been made.  
  - AllSurveyData.csv = the survey results saved as a csv file. It lists for each UserId, the answer to all questions in each Survey ID. when a question is missing, the Nan value is written.  

4. what can be improved:  
  - consider the database as a class for connection open and close easily
  - better error management
