import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1772034748328 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://arhys-d609/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1772034748328")

# Script generated for node Customer Trusted
CustomerTrusted_node1772034788718 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://arhys-d609/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1772034788718")

# Script generated for node Customer Privacy Filter (Join)
CustomerPrivacyFilterJoin_node1771969338416 = Join.apply(frame1=AccelerometerLanding_node1772034748328, frame2=CustomerTrusted_node1772034788718, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilterJoin_node1771969338416")

# Script generated for node Drop Unwanted Fields
SqlQuery0 = '''
SELECT
    x,
    y,
    z,
    timestamp,
    user
FROM
    myDataSource;
'''
DropUnwantedFields_node1771970063136 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerPrivacyFilterJoin_node1771969338416}, transformation_ctx = "DropUnwantedFields_node1771970063136")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropUnwantedFields_node1771970063136, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1771969224698", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1771969391685 = glueContext.getSink(path="s3://arhys-d609/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1771969391685")
AccelerometerTrusted_node1771969391685.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1771969391685.setFormat("json")
AccelerometerTrusted_node1771969391685.writeFrame(DropUnwantedFields_node1771970063136)
job.commit()