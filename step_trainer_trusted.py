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

# Script generated for node Customer Curated
CustomerCurated_node1772035058008 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://arhys-d609/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1772035058008")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1772035088791 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://arhys-d609/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1772035088791")

# Script generated for node Rename CC Serial Number
RenameCCSerialNumber_node1771979673140 = RenameField.apply(frame=CustomerCurated_node1772035058008, old_name="serialnumber", new_name="cc_serialnumber", transformation_ctx="RenameCCSerialNumber_node1771979673140")

# Script generated for node Join ST Landing and CC on serialnumber
SqlQuery0 = '''
SELECT * FROM T1
INNER JOIN T2
ON T1.serialnumber = T2.cc_serialnumber;
'''
JoinSTLandingandCConserialnumber_node1771979499512 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"T2":RenameCCSerialNumber_node1771979673140, "T1":StepTrainerLanding_node1772035088791}, transformation_ctx = "JoinSTLandingandCConserialnumber_node1771979499512")

# Script generated for node Drop Unwanted Fields
SqlQuery1 = '''
SELECT sensorreadingtime, serialnumber, distancefromobject
FROM myDataSource;
'''
DropUnwantedFields_node1771970063136 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":JoinSTLandingandCConserialnumber_node1771979499512}, transformation_ctx = "DropUnwantedFields_node1771970063136")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=DropUnwantedFields_node1771970063136, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1771969224698", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1771969391685 = glueContext.getSink(path="s3://arhys-d609/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1771969391685")
StepTrainerTrusted_node1771969391685.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1771969391685.setFormat("json")
StepTrainerTrusted_node1771969391685.writeFrame(DropUnwantedFields_node1771970063136)
job.commit()