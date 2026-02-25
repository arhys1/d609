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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1771969292631 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1771969292631")

# Script generated for node Customer Trusted
CustomerTrusted_node1771969318202 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1771969318202")

# Script generated for node Customer Privacy Filter (Join)
CustomerPrivacyFilterJoin_node1771969338416 = Join.apply(frame1=AccelerometerTrusted_node1771969292631, frame2=CustomerTrusted_node1771969318202, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilterJoin_node1771969338416")

# Script generated for node Drop Unwanted Fields and Duplicates
SqlQuery0 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource;
'''
DropUnwantedFieldsandDuplicates_node1771970063136 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerPrivacyFilterJoin_node1771969338416}, transformation_ctx = "DropUnwantedFieldsandDuplicates_node1771970063136")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropUnwantedFieldsandDuplicates_node1771970063136, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1771969224698", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1771969391685 = glueContext.getSink(path="s3://arhys-d609/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1771969391685")
CustomerCurated_node1771969391685.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
CustomerCurated_node1771969391685.setFormat("json")
CustomerCurated_node1771969391685.writeFrame(DropUnwantedFieldsandDuplicates_node1771970063136)
job.commit()