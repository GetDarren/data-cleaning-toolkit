def path_by_lotlist(lotlist):
    if isinstance(lotlist, list):
        lotlist = ",".join(lotlist)
    elif isinstance(lotlist, str):
        pass
    else:
        print "Invalid Lotlist type, please re-input"
        return
    command = "/home/hdfsprod/HBaseQueryTool/HBaseQueryTool.sh --table=prod_mti_ww_test_tte:summary --action=get --rowkey={}\
 --verbose --columns=ROWKEY,h:HDFS_PATH".format(lotlist)
    result = subprocess.check_output(command.split(" "))
    hdfs_paths_single_file =re.findall(r'\/\w{4}\/\w{3}\/\w{2}\/\w{4}\/\w{3}\/\w{4}\/\d{4}\/\d{2}\/\d{2}\/\w{7}\.\w{5,9}\_\d{5,20}\.\w{7}',result,re.MULTILINE)
    hdfs_paths_summary_file = re.findall(r'\/\w{4}\/\w{3}\/\w{2}\/\w{4}\/\w{3}\/\w{4}\/\d{4}\/\d{2}\/\d{2}\/\w{4}\.\d{4}\-\d{2}\-\d{2}\..*',result,re.MULTILINE)
    hdfs_paths = hdfs_paths_single_file + hdfs_paths_summary_file
    hdfs_paths = ",".join(set(hdfs_paths))
    return hdfs_paths
def path_by_timeslot(start, end, design_id, step):
    cstr = "/u/pesoft/hadoop/bin/jsums -mode=tte -con=hb:prod -cols=HDFS_PATH " + \
    "-start=" + start + " -end=" + end + " DESIGN_ID=" + design_id + \
    " STEP="+ step + " TEST_FACILITY=XIAN STANDARD_FLOW=YES"
    result = subprocess.check_output(cstr.split(" "))
    hdfs_paths_single_file =re.findall(r'\/\w{4}\/\w{3}\/\w{2}\/\w{4}\/\w{3}\/\w{4}\/\d{4}\/\d{2}\/\d{2}\/\w{7}\.\w{5,9}\_\d{5,20}\.\w{7}',result,re.MULTILINE)
    hdfs_paths_summary_file = re.findall(r'\/\w{4}\/\w{3}\/\w{2}\/\w{4}\/\w{3}\/\w{4}\/\d{4}\/\d{2}\/\d{2}\/\w{4}\.\d{4}\-\d{2}\-\d{2}\..*',result,re.MULTILINE)
    hdfs_paths = hdfs_paths_single_file + hdfs_paths_summary_file
    hdfs_paths = ",".join(set(hdfs_paths))
    return hdfs_paths

def path_by_lotlist(lotlist):
    if isinstance(lotlist, list):
        lotlist = ",".join(lotlist)
    elif isinstance(lotlist, str):
        pass
    else:
        print "Invalid Lotlist type, please re-input"
        return
    command = "/home/hdfsprod/HBaseQueryTool/HBaseQueryTool.sh --table=prod_mti_ww_test_tte:summary --action=get --rowkey={}\
 --verbose --columns=ROWKEY,h:HDFS_PATH".format(lotlist)
    result = subprocess.check_output(command.split(" "))
    hdfs_paths_single_file =re.findall(r'\/\w{4}\/\w{3}\/\w{2}\/\w{4}\/\w{3}\/\w{4}\/\d{4}\/\d{2}\/\d{2}\/\w{7}\.\w{5,9}\_\d{5,20}\.\w{7}',result,re.MULTILINE)
    hdfs_paths_summary_file = re.findall(r'\/\w{4}\/\w{3}\/\w{2}\/\w{4}\/\w{3}\/\w{4}\/\d{4}\/\d{2}\/\d{2}\/\w{4}\.\d{4}\-\d{2}\-\d{2}\..*',result,re.MULTILINE)
    hdfs_paths = hdfs_paths_single_file + hdfs_paths_summary_file
    hdfs_paths = ",".join(set(hdfs_paths))
    return hdfs_paths