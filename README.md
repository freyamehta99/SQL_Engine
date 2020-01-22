# Mini-SQL-Engine
An SQL engine that reads CSV files with integer data, and processes simple queries

### Run Query
Run `python3 sqlengine.py 'query;'`

### Valid queries
- `Select all records:
	- Example Query: `Select * from table_name;`
- Aggregate functions: Simple aggregate functions on a single column namely - ​Sum, average, max and min ​.
​    - Example Query: `Select max(col1) from table1;`
- Project Columns (could be any number of columns) from one or more tables:
	- Example Query: `Select col1, col2 from table_name;`
-  Select/project with distinct (for only one attribute) from one table.
	- Example Query: `Select distinct(col1) from table_name;`
- Select with where from one or more tables :
	- In the where queries, there would be a maximum of one AND/OR operator with no NOT operators.
	- Relational operators that are to be handled in the assignment, the operators include "<, >, <=, >=, =".
 	- Example Query: `Select col1,col2 from table1,table2 where col1=10 AND col2=20;`
- Projection of one or more(including all the columns) from two tables with one join condition :
	- NO REPETITION OF COLUMNS – THE JOINING COLUMN SHOULD BE PRINTED ONLY ONCE.
	- Example Query1: `Select * from table1, table2 where table1.col1=table2.col2;`
	- Example Query2: `Select col1,col2 from table1,table2 where table1.col1=table2.col2 AND table2.col2>=10.`
