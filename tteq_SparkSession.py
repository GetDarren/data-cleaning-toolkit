def StartSpark(name,master, config):
    def set_environ():
        import os,sys
        os.environ["SPARK_MAJOR_VERSION"] = "2"
        os.environ["SPARK_HOME"] = "/usr/hdp/current/spark2-client"
        os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
        os.environ["PYSPARK_PYTHON"] = "/opt/miniconda/bin/python"
        sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.4-src.zip")
        sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--master yarn-client --driver-memory 16G --jars \
    /u/pesoft/hadoop/repos/scala/release/com/micron/pesoft/tteq/2.1.0/tteq-2.1.0.jar \
    pyspark-shell"
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext
    set_environ()
    conf = SparkConf()
    conf.setMaster(master)
    for option in config.keys():
        conf.set(option, config[option].replace(" ",""))
    spark = SparkSession.builder\
        .enableHiveSupport()\
        .appName(name)\
        .config(conf=conf)\
        .getOrCreate()
    return spark
class TTEContext():
    from pyspark.mllib.common import callMLlibFunc, _py2java, _java2py
    from pyspark.sql.types import ArrayType, IntegerType
    from time import gmtime, strftime
    def __init__(self, ss):
        # Authenticate
        self._sc = ss.sparkContext
        # self._sql_context = hbasecontext.sql_context()
        self._jvm = ss._jvm
        self._spark = ss
    def get_tteqproc_by_file(self, files, glob=None, section=None, actions=None, attrformat=None):
        from pyspark.mllib.common import callMLlibFunc, _py2java, _java2py
        from pyspark.sql.types import ArrayType, IntegerType
        if section is None:
            section = "TEST"
        if attrformat is None:
            attrformat = "-format=MFG_WORKWEEK,END_ETIME,SUMMARY_ID,STARTLOT,FID,XPOS,YPOS,STEP,SITE,SOFT_BIN_NO," \
                         "SOFT_BIN_NAME,ERROR_BIN_NAME,DEF_UNIQ_FAIL,DEF_FIRST_FAIL"
        pylist = []
        pylist.append("-data=" + files)
        if glob is not None:  # step
            pylist.extend(glob.split(" "))
        else:
            pass
        pylist.append("%section=TEST")
        pylist.extend("AGG_LEVEL=0,TAG=*".split(","))
        pylist.append("FID!=0000000:00:P00:00:0:0:00:00:0:000:f")
        pylist.append("FID!=0000000:00:N00:00:0:0:00:00:0:000:f")
#         pylist.append("STEP=PGSRT,HSRT,CFIN")
#         pylist.append("TEST_FACILITY=XIAN")
#         pylist.append("STANDARD_FLOW=YES")
        if section is not "TEST":
            pylist.append("%section=" + section)
        if actions is not None: # DESIGN, FAB, STANDARD_FLOW
            pylist.extend(actions.split(" "))
        else:
            pass
        pylist.append("-format=" + attrformat)
        java_array = self._sc._gateway.new_array(self._sc._gateway.jvm.java.lang.String, len(pylist))
        for i in xrange(len(pylist)):
            java_array[i] = pylist[i]

        clproc = self._jvm.com.micron.pesoft.tteq.CLProcessor(java_array)
        tteqproc = self._jvm.com.micron.pesoft.tteq.TTEQProcessor(clproc, self._jvm.com.micron.pesoft.utilities.NullManifest())
        # create the class sections.
        clsec = self._jvm.com.micron.pesoft.tteq.summary.SectionName

        # Tuple2 of dataframes returned. df supposed to be the filtered dataframe.
        if section == "TEST":
            tups = tteqproc.process(clsec.Test())
        elif section == "CHAR":
            tups = tteqproc.process(clsec.Char())
        elif section == "OTHER":
            tups = tteqproc.process(clsec.Other())
        elif section == "EXTRA":
            tups = tteqproc.process(clsec.Extra())
        elif section == "BAD_BLOCK":
            tups = tteqproc.process(clsec.BadBlock())
        elif section == "TEST_CYCLE":
            tups = tteqproc.process(clsec.TestCycle())
        elif section == "BURN_BOARD":
            tups = tteqproc.process(clsec.BurnBoard())  
        else:
            tups = tteqproc.process(clsec.Test())

        summary = _java2py(self._spark, tups._1())
        # details = _java2py(self._spark, tups._2())
        return summary
    def get_char_list(self, lot):
        hdfs_path=path_by_lotlist(lot)
        df = self.get_tteqproc_by_file(hdfs_path,section="CHAR",attrformat="FID,CHAR_REG")
        CharList = df.select("CHAR_REG").distinct().collect()
        return CharList
    def get_CharValue(self,lotlist,CharList,ExtraAttri):
        """
        attributes: FID,CHAR_REG,CHAR_VALUE,CHAR_UNIT already stored.
        """
        CharStr = "\'" + "','".join(CharList) + "\'"
        print CharStr
        hdfs_path=path_by_lotlist(lotlist)
        print hdfs_path
        attributes = "FID,CHAR_REG,CHAR_VALUE,CHAR_UNIT," + ExtraAttri
        print attributes
        df = self.get_tteqproc_by_file(hdfs_path,section="CHAR",attrformat=attributes)
        df=df.filter(df['CHAR_VALUE'] < 'A')
        df=df.filter(df['FID'] > '0000000:00:N00:00:0:0:00:00:0:000:f')
        dfRDD=df.rdd
        df = self._spark.createDataFrame(dfRDD, df.schema)
        df.createOrReplaceTempView("tte")
        self._spark.table("tte")
        strsql = "select SUBSTR(FID,1,17) AS " + attributes + " FROM tte WHERE CHAR_REG in (" + CharStr + ")"
        df1= self._spark.sql(strsql)
        return df1