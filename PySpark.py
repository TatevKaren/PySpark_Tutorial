import pyspark.sql.functions as f
import pyspark.sql.types as t
import pyspark.sql.functions as f

# Loading Data
data = spark.read.format("csv").load("dbfs:/FileStore/Tatev/temp/healthcare_dataset_stroke_data.csv", header = True)
display(data)

# Viewing Data
display(data.limit(20))

# Show
data.show(20)

# Selecting Data
selected_data = data.select("id", "gender", "age", "stroke")
display(selected_data)

# Selecting Data while renaming the variables
selected_data = data.selectExpr("id as ID", "gender as Gender", "age as Age", "stroke as Stroke")
display(selected_data)

# Counting Data
print(data.count())

print(selected_data.select("ID").count())

# Unique Values
print(selected_data.select("ID").distinct().count())

display(selected_data.select("Stroke").distinct())



# Ordering Data
display(selected_data.orderBy("Age"))
display(selected_data.orderBy(f.desc("Age")))

# Filtering Data
display(selected_data.filter("Stroke == 1"))
print(selected_data.filter("Stroke == 1").count())

num_females = selected_data.filter("Gender == 'Female'").count()
num_males = selected_data.filter("Gender == 'Male'").count()
num_females_withstroke = selected_data.filter("Gender == 'Female'").filter("Stroke == 1").count()
num_males_withstroke = selected_data.filter("Gender == 'Male'").filter("Stroke == 1").count()
print(num_females_withstroke*100/num_females)
print(num_males_withstroke*100/num_males)

print(selected_data.filter("Gender == 'Female'").count())
print(selected_data.filter("Gender == 'Male'").count())

print(selected_data.filter("Gender == 'Female'").filter("Stroke == 1").count())
print(selected_data.filter("Gender == 'Male'").filter("Stroke == 1").count())

display(sum_stroke.filter("Gender == 'Female'"))
print(sum_stroke.filter("Gender == 'Female'").collect()[0][1])


# Creating New Variables & Changing Data Types
selected_data = selected_data.withColumn("Age", f.col("Age").cast(t.IntegerType())).withColumn("Stroke", f.col("Stroke").cast(t.IntegerType()))
display(selected_data)

display(selected_data.withColumn("gender_lower", f.lower(f.col("Gender"))))

sum_stroke = selected_data.groupBy("Gender").agg(f.sum(f.col("Stroke")).alias("sum_stroke"))
display(sum_stroke)


# Deleting Data
selected_data = selected_data.drop("gender_lower")
display(selected_data)

# Conditions
selected_data = selected_data.withColumn("gender_dummy",f.when(f.col("Gender") == 'Female', 1).otherwise(0))
display(selected_data)

display(selected_data.filter(f.col("Age").isin([80,81,82])))

# Data Aggregation
display(selected_data.groupBy("Gender").agg(f.max(f.col("Age"))))
display(selected_data.groupBy("Gender").agg(f.min(f.col("Age"))))

agg_data = selected_data.groupBy("Gender").agg(f.collect_list(f.col("Stroke")).alias("Stroke_list_per_gender"))
display(agg_data)

display(selected_data.groupBy("Gender").agg(f.avg("Stroke").alias("Average Stroke Rate")))

agg_data = agg_data.withColumn("num_obs_per_gender", f.size(f.col("Stroke_list_per_gender")))
display(agg_data.select("Gender", "num_obs_per_gender"))


