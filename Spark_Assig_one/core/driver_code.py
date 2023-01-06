from Spark_Assig_one.core.utils_code import *


spark = SparkSe()

tran_DF = transactionDF(spark)
# print(tran_DF)
# tran_DF.show()

user_DF = userDF(spark)
# print(user_DF)
# user_DF.show()
join_DF = joinDF(user_DF,tran_DF)
join_DF.show()

cout_loc = count_location(join_DF,"location","product_description")
cout_loc.show()

couut_product = product_Bought(join_DF,"user_id")
couut_product.show()
#
total_price = total_spending(join_DF,"userid","price")
total_price.show()