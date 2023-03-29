Brian Koga
CS 351
Assignment 5

The file assignment5.py contains all of the python code for this assignment. It has two methods that are used to create a connection to the database
and to create the tables in the database. It also has methods to execute the creation of the database and filling it with the data. The main method
has two lines at the start, one (create_relations) that calls the method to create the database and all of the relations in it. This takes as input
a username, password, and the name of database that you want to create. The second line (insert_data) again takes a username, password, and database
name (though this is one that has already been created). It also takes the name of the csv file that will be used to fill the database.

The rest of the main method contains the 5 queries separated and with commas.  You will also need to change the arguments to the create_connection
ethod. If desired, you could comment out the first two lines that create the database and run all of the queries, or comment out some of the queries and run specific ones.

Note: It takes quite a long time to execute the insert_data method, presumably because there is over 5000 tuples and over 10 relations, some of which 
(such as keywords) will have many insert calls per movie entry