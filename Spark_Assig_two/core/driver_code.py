from Spark_Assig_two.core.utils_code import *
# create SparkSession
spark = sparkSe()
# create dataframe
log_Df=logfile(spark)

# dataframe to use with column to split the column
logs_file =Withcolumn_Log(log_Df)
# logs_file.show()

#Count the number of WARNing messages
warn_count = find_warn(logs_file,"Logging")
warn_count.show()

# #Count the number of Total line
total_line_count = total_line(log_Df)
total_line_count.show()
# #
# #How many repositories where processed in total
total_Api_Client =api_Client(logs_file)
total_Api_Client.show()
#
# #Which client did most HTTP requests
most_Http_count = most_Http(logs_file)
most_Http_count.show()
#
# #Which client did most FAILED HTTP requests
failed_Request_count = faild_Request(logs_file)
failed_Request_count.show()
#
# #most active hour
most_Active_Hour_Count =most_Acite_Hour(logs_file)
most_Active_Hour_Count.show()
#
# #most active repository
active_Repository_count = most_Active_Repository(logs_file)
active_Repository_count.show()

