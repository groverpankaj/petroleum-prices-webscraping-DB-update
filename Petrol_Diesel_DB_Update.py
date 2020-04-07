import tabula    # python3 -m pip install tabula-py
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import os
# import pymysql  # in info file

# info file (Internal use only)
import Petrol_Diesel_DB_Update_info as info


#  Clean the dataframe
def cleanDataFrame(input_df):
    column_names = ['priceDt'] + cityList
    input_df.columns = column_names   # Change column names

    # Convert to string, strip, remove whitespace, convert to float
    for city in cityList:
        input_df[city] = input_df[city].astype(str)
        input_df[city] = input_df[city].str.strip()
        input_df[city] = input_df[city].str.replace(' ', '')
        input_df[city] = input_df[city].astype('float')

    return input_df


# Update Database
def databaseUpDate(tableName, input_df, item):
    # SELECT in MySQL (Last Record Date)
    cur.execute("SELECT priceDt FROM " + tableName +
                " ORDER BY priceDt DESC LIMIT 1")

    for row in cur:
        db_date = row[0]

    print('\n' + item + ' DB Updated till: ' + str(db_date) + '\n')

    recordsUpdated = 0

    # Reverse Iteration
    for index, row in input_df[::-1].iterrows():
        daysDiff = (row['priceDt'] - db_date).days

        if (daysDiff >= 1):
            for city in cityList:
                insertQuery = "INSERT INTO " + tableName + info.insertQuerySuffix
                cur.execute(insertQuery, (city, row['priceDt'], row[city]))
                recordsUpdated += 1

    print(item, ' records updated: ', recordsUpdated, '\n')


######### Code Start #######

# Source Page URL
source_page_url = info.source_page_url

source = requests.get(source_page_url)

sauce = source.text
soup = BeautifulSoup(sauce, 'lxml')

content_soup = soup.find("div", {"id": "contentid"})

# Get all links in the given div content
allAnchorTags = content_soup.find_all('a', href=True)

# Find the link which has innerHTML matching the searchText
# Search Text <a href=''>SearchText</a>
for anchor in allAnchorTags:
    if(info.searchText in anchor.contents[0]):
        # Relative Path to absolute path
        pdfLink = info.source_url_prefix + anchor['href']
        break


# Extact Date from the LinkNane
# Link name has date with format -> text_text_text_day_month_year.PDF
linkNameSplit = pdfLink.split('_')

# last element and slice .PDF at end
year = linkNameSplit[len(linkNameSplit) - 1][:4]
month = linkNameSplit[len(linkNameSplit) - 2]
day = linkNameSplit[len(linkNameSplit) - 3]

if(len(month) == 1):
    month = '0' + month
if(len(day) == 1):
    day = '0' + day

linkDate = year + '-' + month + '-' + day

# Save the PDF in local hard disk
r = requests.get(pdfLink, stream=True)

# File name of PDF to be saved 
pdfFile = linkDate + info.pdfFileSuffix

with open(pdfFile, "wb") as pdf:
    for chunk in r.iter_content(chunk_size=1024):

        # writing one chunk at a time to pdf file
        if(chunk):
            pdf.write(chunk)


# List of cities in order of table given in PDF
cityList = info.cityList


# PDF Table to CSV
tabula.convert_into(pdfFile, "tempFile.csv", output_format="csv", pages='1-2')

# CSV to DataFrame
df = pd.read_csv('tempFile.csv')

# Unwanted Rows
rowToDelete = []

for index, row in df.iterrows():
    try:
        rowDate = datetime.strptime(
            row[0], '%d-%b-%y').date()  # 5-Mar-20 to YYYY-MM-DD
    except:
        rowDate = ''
        rowToDelete.append(index)  # Blank rows with no date, Unwanted rows

    df.iloc[index, 0] = rowDate
    df.iloc[index, 5] = rowDate

# Drop Unwanted rows
df = df.drop(df.index[rowToDelete])


# Divide table in Petrol and Diesel Dataframe
df_petrol = df.iloc[:, 0:5]
df_diesel = df.iloc[:, 5:]

# Clean Data Frame
df_petrol = cleanDataFrame(df_petrol)
df_diesel = cleanDataFrame(df_diesel)

print(df_petrol.head(), '\n')
print(df_diesel.head(), '\n')


# connection
conn = info.conn
cur = conn.cursor()


# Update Database
databaseUpDate(info.petrolTableName, df_petrol, "Petrol")
databaseUpDate(info.dieselTableName, df_diesel, "Diesel")

# Close Connection
cur.close()
del cur
conn.close()

# Remove CSV File
os.remove('tempFile.csv')

input("\nAll is Well :)")
