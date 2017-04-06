# Table of Contents
1. Running program
2. Libraries
3. Loading,Cleaning and PreProcessing
4. Feature 1
5. Feature 2
6. Feature 3
7. Feature 4
8.Assumption
9. Future Work


#Running Program

The code is in src as process_log.py. 
I have removed arguements from run.sh because I have hardcoded the file names for each features.
The file name flexibilty can be added in future.
you can directly run the program using ./run.sh

# Libraries

For the purpose of this challenge I have used pandas and regular expressions
They can be installed using 
pip install pandas
pip install regex

# Loading, Cleaning and PreProcessing 

For loading purpose, I us open file.
Using regular expressions, using a complete pattern, I find host timestamp, timezone, http response and bytes.
I use other regular expressions to split the request into method, uri and http.
I keep a copy of the line  in original column to use it in future
We then change '-' in bytes to 0 as required and change the format for timestamp and bytes for better computation

This process takes time.
This is because it is necessary to ensure that data is collected in correct form to avoid any discrepencies in future

# Feature 1
For this purpose I use value_counts function of dataframe and then reindex and use to_csv method to write it in the file as desired.
This output is written in ./log_output/hosts.txt

# Feature 2
For this case I group dataframes by the resource uri (resources) and then sum the bytes using inbuilt functions, I store it in bandwidthConsumption and sort values and slice only the first 10 rows to get top 10.

This output is written in ./log_output/resources.txt

# Feature 3
For this case I sort data by timestamp and then group dataframe by the timestamp and then count occurence of each timestamp using inbuilt functions.

I use reindexing to fill the missing time periods (I am taking each second) and filling the count with 0

If the length of this sliced datframe is less than 10 then we dont care about anything else.
Else we sum the counts for 3600 (60 minute) window frame

Before saving the file i convert the count from float to int for better readability.

This output is written in ./log_output/hours.txt

# Feature 4
First I arrange dataframe by hosts and then their timestamps so that its i more structured.
I develop a column of 1 for every line which has 3 consecutives 401 (checked by using shift) , the host is the same and the difference in the 3rd and 1st occurence is less than equal to 20 s.
Now I set the index as the host because it becomes much faster to slice then.
We find the hosts who have 3rd failed consecutive attempts and run a loop only over these hosts.
Inside this loop I run 1 more loop for every 5 minute dataframe (this basically resets the frame after 5 minutes for each host)
These are then logged to ./log_output/blocked.txt

#Assumption

I have assumed that this log is always of 1 time zone and we required only specific names of the feature files

#Future Work

Such kind of tasks would have been more effecient on Map Reduce or SParkusing multiple clusters. This is more aggreagtion kind of problem and hence distribution by groups or each row by key value would have been much quicker be it processing the data or analysing it.

