## Assignment ##
This package contains following three assignment

- Spark Core Assignment
- Spark Assignment
- PySpark Assignment

### Spark Core Assignment

Spark Core Assignment Dataset Description:
Consider the two data files (users.csv, transactions.csv).
 Users file has the following fields:
- User_ID(represent same user id as transaction userid)
- EmailID 
- NativeLanguage
- Location

Transactions file has the following fields:

- Transaction_ID
- Product_ID
- UserID
- Price
- Product_Description

 Questionnaire:
By making use of Spark Core (i.e., without using Spark SQL) find out:
- Count of unique locations where each product is sold.
- Find out products bought by each user.
- Total spending done by each user on each product.

### Spark Assignment
 - Here is an extract of what the GHTorrent log looks like:

DEBUG, 2017-03-23T10:02:27+00:00, ghtorrent-40 -- ghtorrent.rb: Repo EFForg/https-everywhere exists

DEBUG, 2017-03-24T12:06:23+00:00, ghtorrent-49 -- ghtorrent.rb: Repo Shikanime/print exists

 INFO, 2017-03-23T13:00:55+00:00, ghtorrent-42 -- api_client.rb: Successful request. URL: https://api.github.com/repos/CanonicalLtd/maas-docs/issues/365/events?per_page=100, Remaining: 4943, Total: 88 ms

 WARN, 2017-03-23T20:04:28+00:00, ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 3031

 DEBUG, 2017-03-23T09:06:09+00:00, ghtorrent-2 -- ghtorrent.rb: Transaction committed (11 ms)

#### Each log line comprises of a standard part (up to .rb:) and an operation-specific part. The standard part fields are like so:
- Logging level, one of DEBUG, INFO, WARN, ERROR (separated by ,)
- A timestamp (separated by ,)
- The downloader id, denoting the downloader instance (separated by --)
- The retrieval stage, denoted by the Ruby class name, one of:

  •event_processing

  •ght_data_retrieval

  •api_client

  •etriever

  •ghtorrent
### Questions:
- Write a function to load it in an RDD.
- How many lines does the RDD contain?	
- Count the number of WARNing messages
- How many repositories where processed in total? Use the api_client lines only.
- Which client did most HTTP requests?
- Which client did most FAILED HTTP requests? Use group_by to provide an answer.
- What is the most active hour of day?
- What is the most active repository (hint: use messages from the ghtorrent.rb layer only)?
## PySpark Assignment

### Create a table using below information.

Product Name	Issue Date	Price	Brand	Country	Product number

Washing Machine	1648770933000	20000	Samsung	India	0001

Refrigerator	1648770999000	35000	  LG	null	0002

Air Cooler	1648770948000	45000	  Voltas	null	0003

### After creating table, please find the below points;
- Convert the Issue Date with the timestamp format.
#### Example: 

Input: 1648770933000 -> Output: 20
22-03-31T23:55:33.000+0000

Input: 1648770999000 -> Output: 2022-03-31T23:56:39.000+0000
Input: 1648770948000 -> Output: 2022-03-31T23:55:48.000+0000

- Convert timestamp to date type

### Example: Input: 2022-03-31T23:55:33.000+0000 -> Output: 2022-03-31

- Remove the starting extra space in Brand column for LG and Voltas fields
- Replace null values with empty values in Country column

- Create a table using below information
SourceId	TransactionNumber	Language	ModelNumber	StartTime	Product Number
150711	123456	EN	456789	2021-12-27T08:20:29.842+0000
	0001
150439	234567	UK	345678	2021-12-27T08:21:14.645+0000
	0002
150647	345678	ES	234567	2021-12-27T08:22:42.445+0000
	0003

### After creating table, please find the below points;
- Change the camel case columns to snake case 

#### Example: SourceId: source_id, TransactionNumber: transaction_number
- Add another column as start_time_ms and convert the values of StartTime to milliseconds.
####Example: 
- 
Input: 2021-12-27T08:20:29.842+0000 -> Output: 1640593229842
Input: 2021-12-27T08:21:14.645+0000 -> Output: 1640593274645
Input: 2021-12-27T08:22:42.445+0000 -> Output: 1640593362445
Input: 2021-12-27T08:22:43.183+0000 -> Output: 1640593363183

- Combine both the tables based on the Product Number 

  •and get all the fields in return.

  •And get the country as EN




