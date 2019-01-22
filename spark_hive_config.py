spark.sql("use user_darrenzhou")
from pyspark.conf import SparkConf
conf = SparkConf()
conf.set("hive.exec.dynamic.partition", "true")
conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

spark.sql("create TABLE user_darrenzhou.U88B (DESIGN_ID STRING,STEP STRING,LOT STRING,OFFSHORE_ASM_COMPANY STRING,FAB_FACILITY_CODE STRING,CONFIGURATION_WIDTH STRING,PRODUCT_GRADE STRING,MACHINE_ID STRING,MFG_WORKWEEK STRING,WAFER STRING,XPOS STRING,YPOS STRING,FID STRING,SOFT_BIN_NO STRING,SOFT_BIN_NAME STRING,START_ETIME STRING) ")
spark.sql("INSERT OVERWRITE TABLE U88B SELECT tte.* FROM tte")