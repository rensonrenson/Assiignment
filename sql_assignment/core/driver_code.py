from sql_assignment.core.utils_code import *
# create SparkSession
spark = sparkSe()
# create dataframe
fruits_details_df=fruits_details(spark)
fruits_details_df.show()

pivoitdata_df=pivoitdata(fruits_details_df)
print(pivoitdata_df)

unpivoitdata_df=unpivoitdata(pivoitdata_df)
print(unpivoitdata_df)

emp_details_df=emp_details(spark)
print(emp_details_df)

emp_window_df=empwindows_details(emp_details_df)
emp_window_df.show()

emp_aggfunc_df=emp_aggfunc(emp_window_df)
emp_aggfunc_df.show()
