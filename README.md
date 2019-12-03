# MiniHive

## Steps to execute
1. javac shell.java
2. java shell

## SQL Queries Syntax

LOAD <filename.csv> as (col1=int, col2=str, col3=int, ........)

SELECT col1,col2 from filename where col2 = value

SELECT <AGGREGATE_FUNC>(col_name) from filename where some_column = "some_value" (for strings)

SELECT <AGGREGATE_FUNC>(col_name) from filename where some_column == some_value (for integers)

SELECT <AGGREGATE_FUNC>(col_name) from filename where some_column <operator> some_value (where <operator> can be >,<,>=,<=,!=)

AGGREGATE_FUNC can be COUNT, MIN, SUM
 
