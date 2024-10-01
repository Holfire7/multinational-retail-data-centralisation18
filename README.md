PROJECT TITLE:

Multinational Retail Data Centralisation  

TABLE OF CONTENTS
- Project Description
- Installation Instructions
- Usage Instructions
- File structure
- License Information

PROJECT DESCRIPTION: 

The project aims to extract retail data from varioUs sources of data such as from csv, s3 and json files from remote files. Then using sql commands to change the schema and query the gotten data to see if it was done right. It centrally involves the getting of sales, product and customer data from several retail stores in three major countries(US, DE and GB). The goal of the project was to use a unified patform to standardise, clean and analyse the data for business reports and insights. 
    The main aim of the project was to create a database for a multinational retail company that makes access to sales, products and customer data easy across various regions, clean and male-uniform the data for better decision making and query the data for better insights.
    Through this project, I have learned to:
    - extract data from various data sources such as , CSV, api and cloud sources.
    - I have through the use of pandas learned to clean and standardise data formats to make them uniform.
    - I have been able to put the data into a single database and write SQL queries to retrieve the data.
    - optimise database performance and esuring data integrity.

INSTALLATION INSTRUCTIONS:
- clone the git repo using the CLI environment.
- make sure to have the necessary python dependencies installed.
- create the file that will have your cloud credentials(eg. AWS , database, etc).
- set up the database in order to query the data.

USAGE INSTRUCTIONS:
- Use the DataExtraction class to download and extract the data from the various sources listed in the code(eg. api, AWS S3, csv, etc)
- The Datacleaning class is used to clean and make the data uniform, thereby removing erronous data. It is also used to clean, convert and standardise the weight class.
- The data_base utils file contains the code that is mainly used to connect to the AWS rds where the file is located and also used to set up the connection to the database for upload to the database. Note that the credentials are saved in a yaml file.

FILE STRUCTURE:

The files are arranged alphabetically on the the folder named miltinational retal data centralisation folder.

LICENSE:




