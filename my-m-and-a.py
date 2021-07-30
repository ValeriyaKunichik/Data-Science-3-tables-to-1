import pandas as pd
import sqlite3

def df_to_sql(df, sql_file):
    connection = sqlite3.connect(sql_file)
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS customers( gender text, firstname text, lastname text, email text, age integer, city text, country text, created_at text, referral text)')
    connection.commit()
    df.to_sql('customers', connection, if_exists='replace', index = False)
    cursor.close()
    connection.close()

def change_columns_order(df):
    df = df.reindex(["Gender", "FirstName", "LastName", "Email", "Age", "City", "Country", "Created_at", "Referral"], axis=1)
    df = df.fillna(value="None")
    return df

def clean_country(df):
    df['Country'] = 'USA'

def clean_city(df):
    df["City"] = df["City"].str.replace(r'\W','_', regex = True)
    df['City'] = df['City'].str.title()

def clean_email(df):
    df["Email"] = df["Email"].str.lower()
    df.drop_duplicates(subset ="Email", keep = False, inplace = True)

def clean_name(df):
    df['FirstName'] = df['FirstName'].str.replace(r'\W','',regex = True)
    df['LastName'] = df['LastName'].str.replace(r'\W','',regex = True)
    df['FirstName'] = df['FirstName'].str.capitalize()
    df['LastName'] = df['LastName'].str.capitalize()

def clean_gender(df):
    gender = {'0':'Male', '1':'Female','M': 'Male','F': 'Female'}
    df['Gender'] = df['Gender'].replace(gender)

def split_name(df):
    df[['FirstName','LastName']] = df.Name.str.split(expand=True)
    df.drop('Name', inplace=True, axis=1)

def clean_prefix(df):
    for column_name in df.columns:
        df[column_name] = df[column_name].str.replace(r'string_|integer_|boolean_|character_','',regex = True)
    df["Age"] = df["Age"].str.replace(r'[a-zA-Z]','',regex = True)    

def my_m_and_a(content_database_1, content_database_2, content_database_3, sql_file):   
    df1 = pd.read_csv(content_database_1)
    df2 = pd.read_csv(content_database_2, sep = ';', header = None, names = ['Age', 'City', 'Gender', 'Name', 'Email'])
    df3 = pd.read_csv(content_database_3, sep = '\t', header = 0, names= ['Gender','Name','Email','Age','City','Country'])
    
    clean_prefix(df3)
    split_name(df2)
    split_name(df3)
    df1 = change_columns_order(df1)
    df2 = change_columns_order(df2)
    df3 = change_columns_order(df3)

    df_arr=[df1, df2, df3]

    for each_df in df_arr:
        clean_gender(each_df)
        clean_name(each_df)
        clean_email(each_df)
        clean_city(each_df)
        clean_country(each_df)

    df = pd.concat(df_arr, ignore_index=True)  

    df_to_sql(df, sql_file)

#my_m_and_a('only_wood_customer_us_1.csv', 'only_wood_customer_us_2.csv', 'only_wood_customer_us_3.csv', 'plastic_free_boutique.sql')
