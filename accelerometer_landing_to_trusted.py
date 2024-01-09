import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Truseted
CustomerTruseted_node1704319074207 = glueContext.create_dynamic_frame.from_catalog(
    database="test",
    table_name="customer_trusted_final",
    transformation_ctx="CustomerTruseted_node1704319074207",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1704319072229 = glueContext.create_dynamic_frame.from_catalog(
    database="test",
    table_name="accelerator_landing",
    transformation_ctx="AccelerometerLanding_node1704319072229",
)

# Script generated for node Join
Join_node1704319113800 = Join.apply(
    frame1=CustomerTruseted_node1704319074207,
    frame2=AccelerometerLanding_node1704319072229,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1704319113800",
)

# Script generated for node Drop Fields
DropFields_node1704319572027 = DropFields.apply(
    frame=Join_node1704319113800,
    paths=[
        "email",
        "phone",
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1704319572027",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1704319136579 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704319572027,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-bucket-example-course/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1704319136579",
)

job.commit()
