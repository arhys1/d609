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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1771980798856 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1771980798856")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1771980799292 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1771980799292")

# Script generated for node Join AT and STT
SqlQuery0 = '''
SELECT T1.timestamp, T2.serialnumber, T1.user, T1.x, T1.y, T1.z, 
     T2.distancefromobject
FROM T1
INNER JOIN T2
ON T1.timestamp = T2.sensorreadingtime;
'''
JoinATandSTT_node1771980895742 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"T1":AccelerometerTrusted_node1771980799292, "T2":StepTrainerTrusted_node1771980798856}, transformation_ctx = "JoinATandSTT_node1771980895742")

# Script generated for node ML Curated
EvaluateDataQuality().process_rows(frame=JoinATandSTT_node1771980895742, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1771980796178", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MLCurated_node1771981411697 = glueContext.getSink(path="s3://arhys-d609/ml_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MLCurated_node1771981411697")
MLCurated_node1771981411697.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
MLCurated_node1771981411697.setFormat("json")
MLCurated_node1771981411697.writeFrame(JoinATandSTT_node1771980895742)
job.commit()