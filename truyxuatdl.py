import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, when,year,month,dayofmonth
# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Championship Analysis") \
    .getOrCreate()
data = spark.read.format('csv').option('header','true').load('D:/Bigdata/Đề tài 8/E1.csv')

data = data.withColumn("FTHG", col("FTHG").cast(IntegerType()))
data = data.withColumn("FTAG", col("FTAG").cast(IntegerType()))
data = data.withColumn("HS", col("HS").cast(IntegerType()))
data = data.withColumn("AS", col("AS").cast(IntegerType()))

#  Đếm số lượng đội và liệt kê
teams_count = data.select("HomeTeam").distinct().count()
teams_list = data.select("HomeTeam").distinct().rdd.flatMap(lambda x: x).collect()
print("****3.3: ")
print("Số đội trong mùa giải:", teams_count)
print("Danh sách đội:", teams_list)

# Tìm số trận hòa
draw_matches_count = data.filter(col("FTR") == "D").count()
print("Số trận hòa:", draw_matches_count)

# Tính tổng số bàn thắng các đội ghi được trên sân nhà
total_home_goals = data.agg({"FTHG": "sum"}).collect()[0][0]
total_away_goals = data.agg({"FTAG": "sum"}).collect()[0][0]
print("Tổng số bàn thắng của đội chủ nhà:", total_home_goals)

# Tìm những trận có tổng số bàn thắng > 3
matches_gt_3_goals = data.filter((col("FTHG") + col("FTAG")) > 3)
print("Số trận có tổng bàn thắng lớn hơn 3:",matches_gt_3_goals.count())
print("Những trận có tổng số bàn thắng > 3:")
matches_gt_3_goals=matches_gt_3_goals.select('Date','HomeTeam','AwayTeam','FTHG', 'FTAG')
matches_gt_3_goals.show(25)

# Tìm những trận của Burnley được thi đấu trên sân nhà và có số bàn thắng >= 3
burnley_matches = data.filter((col("HomeTeam") == "Burnley") & ((col("FTHG") + col("FTAG")) >= 3))
print("Những trận của Burnley có số bàn thắng >= 3:")
burnley_matches=burnley_matches.select('Date','HomeTeam','AwayTeam','FTHG', 'FTAG')
burnley_matches.show(25)

# Tìm những trận mà Reading thua
reading_defeats = data.filter((col("HomeTeam") == "Reading") & (col("FTR") == "A") | (col("AwayTeam") == "Reading") & (col("FTR") == "H"))
print("Những trận mà Reading thua:")
reading_defeats=reading_defeats.select('Date','HomeTeam','AwayTeam','FTHG', 'FTAG')
reading_defeats.show()

# Nhóm theo HomeTeam và đếm số lượng FTR
ftr_counts = data.groupBy("HomeTeam").pivot("FTR").count().fillna(0)
print("Số lượng FTR theo từng đội:")
ftr_counts.show()

# Tạo cột mới dựa trên số bàn thắng ghi được trong trận đấu
data = data.withColumn('Note',when((col('FTHG') + col('FTAG')) < 2, 'well')
                       .when((col('FTHG') + col('FTAG')) < 4, 'very good')
                       .otherwise('amazing'))
print("Dữ liệu sau khi thêm cột Note: ")
data1=data.select('Date','HomeTeam','AwayTeam','FTHG', 'FTAG', 'Note')
data1.show(25)
# filtered_df = data1.filter((year("Date") == 2020))
# filtered_df.show(20)
# Đóng SparkSession
spark.stop()
