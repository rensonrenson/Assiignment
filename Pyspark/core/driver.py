from Pyspark.core.utils import *

spark = sparkSession()

fileDF = converyDF(spark)
fileDF.show()

# Convert timestamp to date type
converyDate = dateConvert(fileDF)
converyDate.show()

# Remove the starting extra space in Brand column for LG and Voltas fields
remove_space_Brand = remove_space(fileDF)
print(remove_space_Brand)

# Replace null values with empty values in Country column
removeNull_value = removeNull(fileDF,"country")
removeNull_value.show()

# Write a schema for table2
trans_DF =trans_Schema(spark)
trans_DF.show()
# Change the camel case columns to snake case
snake_case = convertsnake_case(trans_DF)
snake_case.show()

# Add another column as start_time_ms and convert the values of StartTime to milliseconds
convery_milli_sec = converyMilliSec(trans_DF,"timestamp")
convery_milli_sec.show()

#Combine both the tables based on the Product Number
joinDF = joinDF(fileDF,trans_DF)
joinDF.show()

#get the country as EN
filter_country_en= filterBYEN(joinDF,"country")
filter_country_en.show()