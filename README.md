# MiniHive

#Steps to execute

1. hdfs dfs -mkdir /minihive

2. javac shell.java

3. java shell

# SQL Queries Syntax

LOAD <filename.csv> as (col1=int, col2=str, col3=int, ........)

SELECT col1,col2 from filename where col2 = value

SELECT <AGGREGATE_FUNC>(col_name) from filename where some_column = some_value

AGGREGATE_FUNC can be COUNT, MIN, MAX
 
