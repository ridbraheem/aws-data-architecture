import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import when


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node ExtractActiveMeters
ExtractActiveMeters_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="parking_meters_database",
    table_name="active_meters",
    transformation_ctx="ExtractActiveMeters_node1",
)

# Script generated for node ConvertToFloat
ConvertToFloat_node2 = ApplyMapping.apply(
    frame=ExtractActiveMeters_node1,
    mappings=[
        ("meter_model", "string", "meter_model", "string"),
        ("gross_paid_amt", "string", "gross_paid_amt", "float"),
        ("street_block", "string", "street_block", "string"),
        ("session_end_dt", "string", "session_end_dt", "string"),
        ("session_start_dt", "string", "session_start_dt", "string"),
        ("active_meter_flag", "string", "active_meter_flag", "string"),
        ("post_id", "string", "post_id", "string"),
        ("latitude", "string", "latitude", "string"),
        ("p_post_id", "string", "p_post_id", "string"),
        ("payment_type", "string", "payment_type", "string"),
        ("meter_vendor", "string", "meter_vendor", "string"),
        ("longitude", "string", "longitude", "string"),
        ("street_name", "string", "street_name", "string"),
        ("cap_color", "string", "cap_color", "string"),
    ],
    transformation_ctx="ConvertToFloat_node2",
)

# Script generated for node AggregateByStreet
SqlQuery936 = """
select 
street_block
, sum(gross_paid_amt) total_gross_amount
, count(*)  total_transactions
, array_join(collect_set(payment_type),',') AS payment_types    
, array_join(collect_set(meter_model),',') AS meter_models
from 
myDataSource
group by 1

"""
AggregateByStreet_node1685509317406 = sparkSqlQuery(
    glueContext,
    query=SqlQuery936,
    mapping={"myDataSource": ConvertToFloat_node2},
    transformation_ctx="AggregateByStreet_node1685509317406",
)

#Convert to dataframe
DF = DynamicFrame.toDF(AggregateByStreet_node1685509317406)

#Select features and convert to SparkML required format
vecAssembler = VectorAssembler(inputCols=["total_gross_amount", "total_transactions"], outputCol="features")
assembled_df = vecAssembler.transform(DF)

#Fit and Run Kmeans

kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(assembled_df.select('features'))
transformed = model.transform(assembled_df)

# Rename label predictions to values using when-otherwise statements
transformed = transformed.withColumn("label_category", when(transformed["prediction"] == 0, "Low Traffic")
                                           .when(transformed["prediction"]  == 2, "Medium Traffic")
                                           .when(transformed["prediction"]  == 1, "High Traffic"))

# Drop two columns from the DataFrame
transformed = transformed.drop("prediction", "features")

#Save data to destination
transformed.write.mode('overwrite').parquet('sf-parking/core/meters_aggregated/segmented_meters')




#NewDynamicFrame = DynamicFrame.fromDF(transformed, glueContext, "nested")


# Script generated for node MeterSegmentation
# MeterSegmentation = glueContext.getSink(
#     path="s3://sandbox-ridwanui/sf-parking/core/meters_aggregated/",
#     connection_type="s3",
#     updateBehavior="UPDATE_IN_DATABASE",
#     partitionKeys=[],
#     compression="gzip",
#     enableUpdateCatalog=True,
#     transformation_ctx="MeterSegmentation",
# )
# MeterSegmentation.setCatalogInfo(
#     catalogDatabase="parking_meters_database", catalogTableName="meters_aggregated"
# )
# MeterSegmentation.setFormat("glueparquet")
# MeterSegmentation.writeFrame(NewDynamicFrame)




job.commit()
