import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Source Node
SourceNode_node1704213145975 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-bucket-example-course/Customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="SourceNode_node1704213145975",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1704213387530 = Filter.apply(
    frame=SourceNode_node1704213145975,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1704213387530",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1704213617224 = glueContext.getSink(
    path="s3://udacity-bucket-example-course/Customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node1704213617224",
)
TrustedCustomerZone_node1704213617224.setCatalogInfo(
    catalogDatabase="test", catalogTableName="customer_trusted_final"
)
TrustedCustomerZone_node1704213617224.setFormat("json")
TrustedCustomerZone_node1704213617224.writeFrame(PrivacyFilter_node1704213387530)
job.commit()
