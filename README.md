# CS562

This is a repo for SIT CS562 final project, where we generate a query processing engine for Ad-Hoc OLAP queries.



### Detailed Description

- The MF and EMF queries for the project will be based on the following *schema*

  ```
  sales(cust, prod, day, month, year, state, quant)
  ```

  The table stores the information about the purchases of a product by a customer on a date and state for a sale amount.

- The *input* for the query processing engine is the list of arguments for the new operator Φ (in place of the actual query represented in SQL) – i.e., you can assume the query has already been transformed into a corresponding relational algebraic expression. For example for the following query,

  ```sql
  select cust, sum(x.quant), sum(y.quant), sum(z.quant)
  from sales
  group by cust: x, y, z
  such that x.state = 'NY'
  				and y.state = 'NJ'
  				and z.state = 'CT'
  having sum(x.qaunt) > 2 * sum(y.quant) 
  			or avg(x.quant) > avg(z.quant)
  ```

  you can expect an input such as

  ```
  SELECT ATTRIBUTE(S):
  cust, 1_sum_quant, 2_sum_quant, 3_sum_quant
  
  NUMBER OF GROUPING VARIABLES(n):
  3
  
  GROUPING ATTRIBUTES(V):
  cust
  
  F-VECT([F]):
  1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
  
  SELECT CONDITION-VECT([σ]):
  1.state=’NY’
  2.state=’NJ’
  3.state=’CT’ 
  
  HAVING_CONDITION(G):
  1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant
  ```

- Given the input as described above, the query processing engine **generates a program** (written in Python) which implements the evaluation algorithm mentioned earlier. The generated program (for a given MF/EMF query) goes against the sales table stored in the PostgreSQL database and generates the output corresponding to the query represented by the input. The generated program can be separately compiled and executed to produce the final output for the input query.



### To Test/Run this engine

Since both members are using Mac OS, all command examples below may not apply in other systems.

1. create table and add date to PostgreSQL maing use of PgAdmin

2. install postgresql in system, have the server run automatically, create user if needed

   ```bash
   brew install postgresql
   brew services start postgresql
   /usr/local/opt/postgres/bin/createuser -s postgres
   ```

3. install packages

   ```bash
   pip3 install psycopg2
   pip3 install PTable
   ```

4. run query.py

   ```bash
   python3 query.py
   ```

5. manully type input or test_file name (should be in the same directory)

   ```bash
   Input a query or a file name that contains a query
   test1.txt
   ```

6. check directory contents, you should be able to see "engine.py" generated.

   ```bash
   python3 engine.py
   ```

7. final output table will be printed in terminal and also write to a file "output.txt"

   



 