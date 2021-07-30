# Data-Science-3-tables-to-1

You've been working as a Junior Data Engineer since now 3 months, at Plastic Free Boutique.

Your first mission was to build a strong, robust and scalable customers database for the exponential growth the company will soon have. Your manager is very happy.

We've just acquired a new company Only Wood Box which will be a is perfect solution for our packaging department. They are really expert to make wood package at a competitive price, light and robust.

Expert in their technology, they didn't believe in the digital world... they didn't have invest in their infrastructure despite the decent quantity of customers. Before quitting their engineer told us, at least we have stored all the information, I don't really understood what he meant.

You should use import pandas as pd

Your mission will be to integrate their 3 customers (yes 3 :D) table into ours.

Table 1

Table 2

Table 3

Store everything in the sqlite3 table customers you've designed.

"gender" - 'string'
"firstname" - 'string'
"Lastname" - 'string'
"email" - 'string'
"age" - 'integer'
"city" - 'string'
"country" - 'string'
"created_at" - 'string'
"referral" - 'string'
Your function will be called my_m_and_a and will receive the content of the 3 CSV and the name of the sqlite3 database you will have to use.

VERY IMPORTANT
We want to move on after this merge & acquisition, we don't want to keep their .csv, if they are seen in your repository during your 1-1 meeting (Peer Review) it will be consider as a fail for this project.

Example00
Input: content_database_1, content_database_2, content_database_3, 'plastic_free_boutique.sql'
Output: None
