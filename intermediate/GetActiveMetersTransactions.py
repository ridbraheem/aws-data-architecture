import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node ExtractParkingMeters
ExtractParkingMeters_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="parking_meters_database",
    table_name="parking_meters",
    transformation_ctx="ExtractParkingMeters_node1",
)

# Script generated for node ExtractParkingMeterTransactions
ExtractParkingMeterTransactions_node1685408484512 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="parking_meters_database",
        table_name="parking_meters_transactions",
        transformation_ctx="ExtractParkingMeterTransactions_node1685408484512",
    )
)

# Script generated for node DropUselessColumns
DropUselessColumns_node2 = ApplyMapping.apply(
    frame=ExtractParkingMeters_node1,
    mappings=[
        ("post_id", "string", "post_id", "string"),
        ("active_meter_flag", "string", "active_meter_flag", "string"),
        ("meter_vendor", "string", "meter_vendor", "string"),
        ("meter_model", "string", "meter_model", "string"),
        ("cap_color", "string", "cap_color", "string"),
        ("street_name", "string", "street_name", "string"),
        ("longitude", "string", "longitude", "string"),
        ("latitude", "string", "latitude", "string"),
    ],
    transformation_ctx="DropUselessColumns_node2",
)

# Script generated for node DropColumns
DropColumns_node1685408549980 = DropFields.apply(
    frame=ExtractParkingMeterTransactions_node1685408484512,
    paths=["transmission_datetime", "meter_event_type"],
    transformation_ctx="DropColumns_node1685408549980",
)

# Script generated for node Renamed keys for JoinBothTables
RenamedkeysforJoinBothTables_node1685408393684 = RenameField.apply(
    frame=DropUselessColumns_node2,
    old_name="post_id",
    new_name="p_post_id",
    transformation_ctx="RenamedkeysforJoinBothTables_node1685408393684",
)

# Script generated for node Drop Parking Duplicates
DropParkingDuplicates_node1685408598829 = DynamicFrame.fromDF(
    DropColumns_node1685408549980.toDF().dropDuplicates(),
    glueContext,
    "DropParkingDuplicates_node1685408598829",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685408449383 = DynamicFrame.fromDF(
    RenamedkeysforJoinBothTables_node1685408393684.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1685408449383",
)

# Script generated for node JoinBothTables
DropParkingDuplicates_node1685408598829DF = (
    DropParkingDuplicates_node1685408598829.toDF()
)
DropDuplicates_node1685408449383DF = DropDuplicates_node1685408449383.toDF()
JoinBothTables_node1685408623219 = DynamicFrame.fromDF(
    DropParkingDuplicates_node1685408598829DF.join(
        DropDuplicates_node1685408449383DF,
        (
            DropParkingDuplicates_node1685408598829DF["post_id"]
            == DropDuplicates_node1685408449383DF["p_post_id"]
        ),
        "left",
    ),
    glueContext,
    "JoinBothTables_node1685408623219",
)

# Script generated for node GetActiveMeters
GetActiveMeters_node1685408692836 = Filter.apply(
    frame=JoinBothTables_node1685408623219,
    f=lambda row: (bool(re.match("M", row["active_meter_flag"]))),
    transformation_ctx="GetActiveMeters_node1685408692836",
)

#Convert to dataframe
DF = DynamicFrame.toDF(GetActiveMeters_node1685408692836)

#Save data to destination
DF.write.mode('overwrite').parquet('/sf-parking/intermediate/active_meters/')

# # Script generated for node FinalActiveMeters
# FinalActiveMeters_node1685408732059 = glueContext.getSink(
#     path="s3://sandbox-ridwanui/sf-parking/intermediate/active_meters/",
#     connection_type="s3",
#     updateBehavior="UPDATE_IN_DATABASE",
#     partitionKeys=[],
#     compression="gzip",
#     enableUpdateCatalog=True,
#     transformation_ctx="FinalActiveMeters_node1685408732059",
# )
# FinalActiveMeters_node1685408732059.setCatalogInfo(
#     catalogDatabase="parking_meters_database", catalogTableName="active_meters"
# )
# FinalActiveMeters_node1685408732059.setFormat("glueparquet")
# FinalActiveMeters_node1685408732059.writeFrame(GetActiveMeters_node1685408692836)
job.commit()
