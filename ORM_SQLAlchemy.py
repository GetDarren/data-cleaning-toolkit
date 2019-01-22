# Linux:
class mssql_operation(object):

    def __init__(self,server,database,username,password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
    def con(self):   #
        os.environ["ODBCINI"] = '/mucfg/unixodbc/2.3.1/odbc.ini'
        os.environ["ODBCSYSINI"] = '/mu/sdk/unixODBC/2.3.1-gcc443-rhel5-64/etc'
        os.environ['TDSVER'] = '8.0'
        # For Python 2
        params = urllib.pathname2url("DRIVER={FreeTDS};SERVER=" + self.server + ";DATABASE="+self.database+";UID="+self.username+";PWD="+self.password)
        # For Python 3
        #params = urllib.parse.quote("DRIVER={FreeTDS};SERVER=" + self.server + ";DATABASE="+self.database+";UID="+self.username+";PWD="+self.password)
        engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        return engine
        
        
   # Windows: