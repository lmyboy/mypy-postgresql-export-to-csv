# My CSV exporter for Postgresql Database

This is a simple script that conect to a PostgreSQL database, and save the select result at a csv file.
The scrtp uses the COPY to create the file from the data stream, saving memory during the proccess.

##  How to use

- create the .env file based on .env.example file
- the search variable is an arrey of tuples that must contain two strings, the first one it's the name of the outpu file (name.csv) and the second one must contain the select SQL script and must not cotain the ; at the end. You could use this project to export mutiple files and SQL scripts in series, jus add another tuple.
