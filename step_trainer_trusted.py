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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1704748962281 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-bucket-example-course/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1704748962281",
)

# Script generated for node Customer Curated
CustomerCurated_node1704752212213 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-bucket-example-course/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1704752212213",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1704752296672 = ApplyMapping.apply(
    frame=CustomerCurated_node1704752212213,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1704752296672",
)

# Script generated for node Join
Join_node1704752267163 = Join.apply(
    frame1=StepTrainerLanding_node1704748962281,
    frame2=RenamedkeysforJoin_node1704752296672,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1704752267163",
)

# Script generated for node Drop Fields
DropFields_node1704752358042 = DropFields.apply(
    frame=Join_node1704752267163,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_customerName",
        "right_email",
        "right_phone",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1704752358042",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1704752595415 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704752358042,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-bucket-example-course/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_landing_node1704752595415",
)

job.commit()
