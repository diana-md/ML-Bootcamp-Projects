# SQL Queries with Spark

In this project Spark SQL and Jupyter Notebook were used to query multiple tables in a database. This project demonstrates my familiarity with Spark SQL interface, which scales easily, making it great for working with huge datasets. It also demonstrates my mastery of using SQL language to manipulate and extract information from relational databases. 

Multiple csv files were uploaded into Spark to create a relational database. Three tables of interest were created and populated with data from csv files. The schema of the three tables is as follows:

Bookings Schema
root
-  bookid: integer (nullable = true)
- facid: integer (nullable = true)
 |-- memid: integer (nullable = true)
 |-- starttime: timestamp (nullable = true)
 |-- slots: integer (nullable = true)

Facilities Schema
root
 |-- facid: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- membercost: double (nullable = true)
 |-- guestcost: double (nullable = true)
 |-- initialoutlay: integer (nullable = true)
 |-- monthlymaintenance: integer (nullable = true)

Members Schema
root
 |-- memid: integer (nullable = true)
 |-- surname: string (nullable = true)
 |-- firstname: string (nullable = true)
 |-- address: string (nullable = true)
 |-- zipcode: integer (nullable = true)
 |-- telephone: string (nullable = true)
 |-- recommendedby: integer (nullable = true)
 |-- joindate: timestamp (nullable = true)
 
 SQL Queries were used to answer the following questions:
 - List the names of the facilities that charge a fee to members.
 - How many facilities do not charge a fee to members?
 - How can you produce a list of facilities that charge a fee to members, where the fee is less than 20% of the facility's monthly maintenance cost?
 - How can you retrieve the details of facilities with ID 1 and 5? Write the query without using the OR operator.
 - How can you produce a list of facilities, with each labelled as 'cheap' or 'expensive', depending on if their monthly maintenance cost is more than $100?
 - You'd like to get the first and last name of the last member(s) who signed up.
 - How can you produce a list of all members who have used a tennis court?
 - How can you produce a list of bookings on the day of 2012-09-14 which will cost the member (or guest) more than $30?
 - Produce a list of facilities with a total revenue less than 1000.
