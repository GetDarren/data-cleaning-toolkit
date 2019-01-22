mica.select(concat_ws(':',mica.DieX, mica.DieY)).show()

#or use UDF:

func = udf(lambda c1,c2: c1 +":" + c2, StringType())
mica.withColumn("fid", func(col('LotId'),col('WaferId'))).show()

#Define an UDF
def position_X(s):
    if s in ["0","1","2","3","4","5","6","7","8","9"]:
        return 'P0'+s
    elif s in ["-1","-2","-3","-4","-5","-6","-7","-8","-9"]:
        return 'N0'+s[1]
    elif s >= "10":
        return 'P' + s
    else:
        return 'N' + s[1:]
spark.udf.register("position_X",position_X)

def position_Y(s):
    if len(s) == 1:
        return '0' + s
    else:
        return s
spark.udf.register("position_Y",position_Y)

from pyspark.sql.functions import *
from pyspark.sql.functions import udf,col
from pyspark.sql.types import *

pdf = pdf.select('*',substring("LotId",1,7) ,substring("WaferId",6,2)).drop("LotId","WaferId")
pdf = pdf.withColumnRenamed('substring(LotId, 1, 7)', 'LotId')
pdf = pdf.withColumnRenamed('substring(WaferId, 6, 2)', 'WaferId')
Die_X = udf(position_X, StringType())
pdf = pdf.withColumn("DieX", Die_X(col('DieX')))
Die_Y = udf(position_Y, StringType())
pdf = pdf.withColumn("DieY", Die_Y(col('DieY')))
pdf = pdf.withColumn('Fid',concat_ws(':',pdf.LotId, pdf.WaferId,pdf.DieX,pdf.DieY)).drop("DieX","DieY","LotId","WaferId")

# convert pyspark.Row to python list
char_reg = []
for i in range(len(char_reg1)):
    char_reg.append(char_reg1[i].CHAR_REG)
    
    
    # CHAR_REG is the key of char_reg1

from pyspark.sql.functions import *
def drop_duplicate_FID_by_time(pdf):
    pdf2 = pdf.groupBy('FID').agg(max('START_ETIME')).withColumnRenamed('FID', 'r_FID')
    pdf2 = pdf2.withColumnRenamed('max(START_ETIME)', 'end_time')
    pdf = pdf.join(pdf2, (pdf2.r_FID == pdf.FID) & (pdf.START_ETIME == pdf2.end_time)).drop('r_FID').drop('end_time')
    pdfRDD = pdf.rdd
    pdf = spark.createDataFrame(pdfRDD, pdf.schema)
    pdf.createOrReplaceTempView("tte")
    spark.table("tte")
    pdf1 = spark.sql("select DESIGN_ID,STEP,LOT,OFFSHORE_ASM_COMPANY,FAB_FACILITY_CODE,CONFIGURATION_WIDTH,PRODUCT_GRADE,MACHINE_ID,MFG_WORKWEEK,WAFER,XPOS,YPOS,SUBSTR(FID,1,7) as FID,SOFT_BIN_NO,SOFT_BIN_NAME,START_ETIME from tte")
    return pdf1

# pyspark dataframe:  summary a column + change column dtype

from pyspark.sql.types import *
from pyspark.sql import functions as F
res = gdf.unionAll(
        gdf.select([
            F.lit('ALL').alias('SUMMARY_ID'),
            F.sum(gdf.upass).alias('upass'),
            F.sum(gdf.uin).alias('uin'),
        ]))
        
        
def ConvertColumnType(df, col, newType):
    df = df.withColumn(col, df[col].cast(newType))
    return df
    
gdf = ConvertColumnType(gdf, "upass", IntegerType())


# StressCurve DataFrame Manipulation
tdf = tdf.filter(tdf.FID > "0000000:00:N00:00:0:0:00:00:0:000:f")
tdf = tdf.filter("TEST_FACILITY == 'XIAN' and FAB_FACILITY_CODE == '16' and VERSION like ('%37%') and FAB_EXCR_ID == 'EXCR000'")
tdf = tdf.drop("TEST_FACILITY","FAB_FACILITY_CODE","FAB_EXCR_ID","DESIGN_ID")
tdf = tdf.withColumn("FID", substring("FID",1,17))
tdf = tdf.withColumn("ABI", when(tdf.SOFT_BIN_NO == '14', substring("DEF_FIRST_FAIL",4,2)).otherwise("0"))
tdf = tdf.withColumn("EDTION", when((substring("VERSION",12,1) == "U") | (substring("VERSION",12,1) == "V"),  '1').otherwise("2"))