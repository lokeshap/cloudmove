__author__ = '33323'
from botocore.client import Config
import boto3
import time
import sys
import os
import utilities
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat
import pymongo
import paramiko
import subprocess as sub
import string, threading
import multiprocessing
import logging
import datetime
from datetime import datetime
#from tqdm import tqdm
import platform
import subprocess

#Declaration of Variables
hname=os.popen('hostname').read().rstrip()
logfile=datetime.now().strftime(hname+"_"+"main.log"+'_%d_%m_%Y.log')
logging.basicConfig(filename=logfile,level=logging.INFO)
path = os.path.abspath('./aa1tox100.yaml')
configs = utilities.read_yaml(path)
bucket_name = configs['Bucket_Name']
number_of_process = configs['Number_of_Process']
number_of_thread = configs['Number_of_Thread']
number_of_keys = configs['Number_of_Keys']
Email_ID = configs['Email_ID']
source_host=configs['Source_Host_Details']['Host_Name']
destination_host=configs['Destination_Host_Details']['Host_Name']
DB_IP = configs['DB_IP']
source_url=configs['Source_Host_Details']['endpoint_url']
destination_url=configs['Destination_Host_Details']['endpoint_url']
host_connect_timeout=configs['Host_Connect_Timeout']
read_connect_timeout=configs['Read_Connect_Timeout']
type_of_operation=configs['Operation_Type']
Node1=configs['Node1']
Node2=configs['Node2']
Node3=configs['Node3']
Node4=configs['Node4']
Node5=configs['Node5']
Node6=configs['Node6']
NumofNodes=configs['No_of_Nodes']
username=configs['Username']
password=configs['Password']
db_ip=configs['DB_IP']
db_user=configs['DB_user']
db_password=configs['DB_password']
db_name=configs['DB_name']
#Multipart
Tmultipart_threshold = configs['Multipart_threshold']
Tmultipart_chunksize = configs['Multipart_Size']
Tmax_concurrency = configs['Max_concurrency']
Tio_chunksize = configs['IO_chucksize']
TUse_Threads = configs['Use_Threads']


timeout_config = Config(connect_timeout=host_connect_timeout, read_timeout=read_connect_timeout)
source =  boto3.client('s3',
                    endpoint_url=configs['Source_Host_Details']['endpoint_url'],
                    aws_access_key_id=configs['Source_Host_Details']['aws_access_key_id'],
                    aws_secret_access_key=configs['Source_Host_Details']['aws_secret_access_key'],
                    use_ssl=False,
                    config=timeout_config
                    )

source1 = S3Connection(aws_access_key_id=configs['Source_Host_Details']['aws_access_key_id'],
                       aws_secret_access_key=configs['Source_Host_Details']['aws_secret_access_key'],
                       is_secure=False, port=configs['Source_Host_Details']['port'],
                       host=configs['Source_Host_Details']['ip'],
                       calling_format=OrdinaryCallingFormat())

destination = boto3.client('s3',
                           endpoint_url=configs['Destination_Host_Details']['endpoint_url'],
                           aws_access_key_id=configs['Destination_Host_Details']['aws_access_key_id'],
                           aws_secret_access_key=configs['Destination_Host_Details']['aws_secret_access_key'],
                           use_ssl=False,
                           config=timeout_config
                           )

destination1 = S3Connection(aws_access_key_id=configs['Destination_Host_Details']['aws_access_key_id'],
                            aws_secret_access_key=configs['Destination_Host_Details']['aws_secret_access_key'],
                            is_secure=False, port=configs['Destination_Host_Details']['port'],
                            host=configs['Destination_Host_Details']['ip'], calling_format=OrdinaryCallingFormat()
                            )

def introduction():
    print "#################################################################################"
    print "																					"
    print "Usage: python cloudmove.py [options]										  	        "
    print "																					"
    print "Cloudmove is a serice to migrate data from HGST ActiveArchive or Amplidata to 	"
    print "HGST ActiveScale object storage . It's a WDC S3 to S3 migration tool	      	    "
    print "																					"
    print "Options:  																	    "
    print "check_connectivity   -  To verify network connectivity and bucket access      	"
    print "copy                 -  Start the baseline migration job 					    "
    print "restart              -  To restart the migration job 						    "
    print "sync                 -  To perform the incremental migration job	      		    "
    print "																					"
    print "																					"
    print "																					"
    print "																					"
    print "#################################################################################"


def scalar_access_check():
    logging.info('===================================================')
    logging.info('========= In Scalar_access_check function =========')
    logging.info('Starting S3 boto connection !!!')
    my_src = boto3.client('s3',
                          endpoint_url=configs['Source_Host_Details']['endpoint_url'],
                          aws_access_key_id=configs['Source_Host_Details']['aws_access_key_id'],
                          aws_secret_access_key=configs['Source_Host_Details']['aws_secret_access_key'],
                          use_ssl=False,
                          config=timeout_config
                          )
    my_dst = boto3.client('s3',
                          endpoint_url=configs['Source_Host_Details']['endpoint_url'],
                          aws_access_key_id=configs['Source_Host_Details']['aws_access_key_id'],
                          aws_secret_access_key=configs['Source_Host_Details']['aws_secret_access_key'],
                          use_ssl=False,
                          config=timeout_config
                          )
    try:
        my_src.list_buckets()
        my_dst.list_buckets()
        logging.info('Connection established .... !!!')
        logging.info('========= source and destination bucket access is successful!!!  =========')
        #print "SUCCESS: Src and Dst buckets are accessible !!!"
    except: # Exception as e:
        #print e
        #print "ERROR: Src and Dst bucket are NOT accessible !!!"
        #raise Exception(' Source OR Destination NOT accessible .. Please check')
        logging.info('========= NOT NOT able to access source and destination buckets  =========')
        print "Not able to connect to source OR destination"
        sys.exit(1)

def host_list():
   d = {}
   for i in range(1,NumofNodes+1):
       d["Node%s"%i]=configs["Node%s"%i]
   return d

def network_access_check():
    hosts_dict = host_list()
    #print hosts_dict
    for k,v in hosts_dict.items():
         ping(v)

def ping(ip):
    logging.info('===================================================')
    logging.info('Ping test for the system:' '%s', ip)
    plat = platform.system()
    if plat == "Windows":
        p = subprocess.Popen(
            ["ping", "-n", "2", "-l", "1", "-w", "100", ip],
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE
        )
    if plat == "Linux":
        p = subprocess.Popen(
            ["ping", "-c", "2", "-l", "1", "-s", "1", "-W", "1", ip],
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE
        )
    #out, error = ping_stat.communicate()
    p.wait()
    if p.poll():
        logging.info('%s' '  is down' , ip)
        logging.info('Please fix the connection and restart the transfer')
        logging.info('===================================================')
        sys.exit(1)
        print ip+" is down"
        print "Check the network and try again !!"
    else:
        logging.info('%s' '  is up', ip)
        logging.info('===================================================')
        #print ip+" is up"

def ssh_server(host,cmd):
    #print "host=", host
    #print "cmd =" , cmd
    logging.info('---------------------------------------------------------------------')
    logging.info('ssh session started for host' '%s', host)
    logging.info('ssh submitted command : ' '%s',cmd)
    global username
    global password
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)
    #transport = ssh.get_transport()
    #channel = transport.open_session()
    #channel.exec_commnad(cmd)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    #print stdout.readlines()
    logging.info('ssh session completed for host' '%s', host)
    logging.info('---------------------------------------------------------------------')

def ssh_kill(host,cmd):
    logging.info('---------------------------------------------------------------------')
    logging.info('ssh session started for host' '%s', host)
    logging.info('ssh submitted KILL command over the process : ' '%s', cmd)
    global username
    global password
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)
    # transport = ssh.get_transport()
    # channel = transport.open_session()
    ssh.get_transport().open_session().exec_command("kill -9 `ps -aef |grep %s | grep -v grep | awk '{print $2}'`" % cmd)
    logging.info('ssh session and process kill completed for host' '%s', host)
    logging.info('---------------------------------------------------------------------')

def list_increment_objects():
       connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)  # defaults to port 27017
       # db = connection.test
       # db.Rincrement.drop()
       db = connection[db_name]
       scol = bucket_name + "_sourcecol"
       dcol = bucket_name + "_destinationcol"
       Nscol = bucket_name + "_Nsourcecol"
       Ndcol = bucket_name + "_Ndestinationcol"
       Rscol = bucket_name + "_Rsourcecol"
       Rdcol = bucket_name + "_Rdestinationcol"
       Rincrement = bucket_name + "_Rincrement"
       #db[Ndcol].drop()
       db[Rincrement].drop()
       #Rincrement = db.Rincrement


       logging.info('In function list_increment_objects to get all the current list of keys from current source')
       logging.info('######################################################################')
       logging.info('Mongo database connection established ')
       logging.info('Started getting the list of all objects from source')
       start_time = time.time()
       for key in source1.get_bucket(bucket_name):
           db[Rincrement].insert({"bucket": bucket_name, "obj_key": key.key, "obj_size": key.size, "obj_etag": key.etag,"obj_status": "False"})
           # print "key etag :" , key.etag ; #print "key name :" , key.key  #print "key size :", key.size
       stop_time = time.time() - start_time
       logging.info('Source collection created with all the objects. Total taken is : ''%s', stop_time)
       logging.info('End of function list_increment_objects ')

def find_increment_objects():
       connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)  # defaults to port 27017
       db = connection[db_name]
       scol = bucket_name + "_sourcecol"
       dcol = bucket_name + "_destinationcol"
       Nscol = bucket_name + "_Nsourcecol"
       Ndcol = bucket_name + "_Ndestinationcol"
       Rscol = bucket_name + "_Rsourcecol"
       Rdcol = bucket_name + "_Rdestinationcol"
       Rincrement = bucket_name + "_Rincrement"
       DELsource = bucket_name + "_DELsource"
       db[Nscol].drop()
       db[DELsource].drop()

       logging.info('In function find_increment_objects now ..to find only new objects that needs to be copied ')
       logging.info('######################################################################')
       logging.info('Mongo database connection established ')
       logging.info('Starting to find new keys to be copied ...')
       start_time = time.time()
       Rcount = 0
       Slist = list(db[scol].find({}))  # {}, {"obj_key": 1, "obj_etag": 1, "obj_size": 1, "_id": 0})
       Rlist = list(db[Rincrement].find({}))  # {}, {"obj_key": 1, "obj_etag": 1, "obj_size": 1, "_id": 0})
       Slen = db[scol].find({}).count()  #, {"obj_key": 1, "obj_etag": 1, "obj_size": 1, "_id": 0}).count()
       # print "Slen : %s" , Slen
       for ii in Rlist:
           for jj in Slist:
               # print "ii : %s  jj : %s" % (ii["obj_key"],jj["obj_key"])
               if ii["obj_key"] == jj["obj_key"]:
                   # print "    Same key exists..check etag and size"
                   # print "    Setag : %s   Retag : %s " % ( ii["obj_etag"].encode('utf8'), jj["obj_etag"].encode('utf8'))
                   if ii["obj_etag"] != jj["obj_etag"] or ii["obj_size"] != jj["obj_size"]:
                       # print "    Eetag and size different"
                       # print "    Insert to NSlist its modified element %s ", ii["obj_key"]
                       db[Nscol].insert({"bucket": bucket_name, "obj_key": ii["obj_key"], "obj_size": ii["obj_size"],
                                          "obj_etag": ii["obj_etag"], "obj_status": "False"})
                       break
                   else:
                       pass
                       # print "    Its same key as Source ..Dont insert"
               elif ii["obj_key"] != jj["obj_key"]:
                   Rcount += 1
           if Rcount == Slen:
               # print "   Insert to NSlist..Its new element %s" , ii["obj_key"]
               db[Nscol].insert({"bucket": bucket_name, "obj_key": ii["obj_key"], "obj_size": ii["obj_size"],"obj_etag": ii["obj_etag"], "obj_status": "False"})
           Rcount = 0
       stop_time = time.time() - start_time
       logging.info('Time taken to compare Current source Vs Copied source and get NEW object list is : ''%s', stop_time)
       logging.info('Completed finding new objects to be copied . Going to find list of objects to be deleted ')
       # Nlist = db.Nsourcecol.find({})
       # print "Keys to be added are:"
       # for ii in Nlist:
       #     print ii["obj_key"]
       print "Number of new keys to be added are : ", db[Nscol].find().count()

       logging.info('Started to find Keys to be DELETED on source ')
       start_time = time.time()
       Nlen = db[Rincrement].find({}, {"obj_key": 1, "obj_etag": 1, "obj_size": 1, "_id": 0}).count()
       Rcount = 0
       for ii in Slist:
           for jj in Rlist:
               # print "ii : %s  jj : %s" % (ii["obj_key"], jj["obj_key"])
               if ii["obj_key"] != jj["obj_key"]:
                   Rcount += 1
           if Rcount == Nlen:
               # print "   Insert to NSlist..Its new element %s", ii["obj_key"]
               db[DELsource].insert({"bucket": bucket_name, "obj_key": ii["obj_key"], "obj_size": ii["obj_size"],
                               "obj_etag": ii["obj_etag"], "obj_status": "False"})
           Rcount = 0
       stop_time = time.time() - start_time
       logging.info('Time taken to find objects that needs to be deleted is: ''%s', stop_time)
       print "Number of keys to be deleted from existing source : ", db[DELsource].find().count()
       logging.info('Completed finding Delete objects ')
       if db[Nscol].find().count() == 0:
           delete_oldsource_objects()

       if  (db[Nscol].find().count() == 0) and (db[DELsource].find().count() == 0) :
           print "============================================================="
           print "Source and Destination are In-sync  !!! "
           print "Migration completed !!!"
           print "============================================================="
           exit(0)


def progress_bar():

    logging.info('In progress bar function ...')
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)  # defaults to port 27017
    # db = connection.test
    # db.Ndestinationcol.drop()
    # Ndestinationcol = db.Ndestinationcol
    db = connection[db_name]
    scol = bucket_name + "_sourcecol"
    dcol = bucket_name + "_destinationcol"
    Nscol = bucket_name + "_Nsourcecol"
    Ndcol = bucket_name + "_Ndestinationcol"
    Rscol = bucket_name + "_Rsourcecol"
    Rdcol = bucket_name + "_Rdestinationcol"
    Rincrement = bucket_name + "_Rincrement"
    DELsource = bucket_name + "_DELsource"

    db[Ndcol].drop()


    hosts = [Node1, Node2, Node3]

    if(type_of_operation == "copy"):
            pipe = [{'$group': {'_id': None, 'total': {'$sum': '$obj_size'}}}]
            src_db_size = list(db[scol].aggregate(pipe))[0]["total"]
            #print "src_db_size = " , src_db_size
            dst_db_size = 0
            stop_time1 = 0
            #print "Source size = %d MB and Destination size = %d MB" % (src_db_size , dst_db_size )
            #print "Source size = %s and Destination size = %s" % (src_db_size, dst_db_size)
            start_time = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
            while dst_db_size < src_db_size:
                try:
                        #scalar_access_check()
                        network_access_check()
                        #stop_time_old = stop_time1
                        #start_time1 = time.time()
                        #dst_db_size_old = dst_db_size
                        pipe = [{'$group': {'_id': None, 'total': {'$sum': '$obj_size'}}}]
                        dst_db_size = list(db[dcol].aggregate(pipe))[0]["total"]
                        print "dst_db_size = "
                        #st_db_size
                        os.system('clear')
                        print "S3 Migration status ..."
                        print "Bucket Name: ", bucket_name
                        print "%s" % ('=' * 133)
                        print "Source size = %d MB and Destination size = %d MB" % ( src_db_size/1024/1024 , dst_db_size/1024/1024 )
                        print "Percentage of completion : [%-100s] %d%%" % ('#' * ((dst_db_size * 100) / src_db_size), ((dst_db_size * 100) / src_db_size))
                        current_time = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
                        print ("Progress Log: %s   -  %s   - %s " % (start_time,current_time,db[dcol].count()))
                        logging.info('Progress Log:' '%s' '- ' '%s' ' :' '%s', start_time, current_time, db[dcol].count())
                        #stop_time1 = time.time() - start_time1
                        #print "Stoptime1 = ", stop_time1
                        #print "Stoptime_old = ", stop_time_old
                        #print "Dst_db_size = ", dst_db_size
                        #print "Dst_db_size_old = ", dst_db_size_old
                        #print "Throughtput = %s MB/sec" % ((dst_db_size - dst_db_size_old/1024/1024) / (stop_time1 - stop_time_old))
                        print "%s" % ('=' * 133)
                        time.sleep(5)
                except KeyboardInterrupt:
                        print("Caught control-C interruption ....")
                        for hhh in hosts:
                            ssh_kill(hhh, "rcopy.py")
                        print "Terminated the data migration  !!!!!!!!"
                        sys.exit(1)
                except:
                        #print("waiting in except2")
                        time.sleep(0.5)
                        continue
    elif(type_of_operation == "restart"):
            pipe = [{'$group': {'_id': None, 'total': {'$sum': '$obj_size'}}}]
            src_db_size = list(db[scol].aggregate(pipe))[0]["total"]
            pipe1 = [{'$group': {'_id': None, 'total': {'$sum': '$obj_size'}}}]
            dst_db_size = list(db[dcol].aggregate(pipe1))[0]["total"]
            Rdst_db_size = dst_db_size
            # print "Source size = %d MB and Destination size = %d MB" % (src_db_size , dst_db_size )
            # print "Source size = %s and Destination size = %s" % (src_db_size, dst_db_size)
            start_time = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
            while dst_db_size < src_db_size:
                try:
                    pipe = [{'$group': {'_id': None, 'total': {'$sum': '$obj_size'}}}]
                    dst_db_size = list(db[dcol].aggregate(pipe))[0]["total"]
                    # print "dst_db_size = " , dst_db_size
                    os.system('clear')
                    print "S3 Migration status ..."
                    print "Bucket Name: ", bucket_name
                    print "%s" % ('=' * 133)
                    print "Destination size before the restart operation: %d " % (Rdst_db_size / 1024 / 1024)
                    print "Source size = %d MB and Destination size = %d MB" % (
                    src_db_size / 1024 / 1024, dst_db_size / 1024 / 1024)
                    print "Percentage of completion : [%-100s] %d%%" % (
                    '#' * ((dst_db_size * 100) / src_db_size), ((dst_db_size * 100) / src_db_size))
                    current_time = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
                    print ("Progress Log: %s   -  %s   - %s " % (start_time,current_time,db[dcol].count()))
                    logging.info('Progress Log:' '%s' '- ' '%s' ' :' '%s', start_time, current_time, db[dcol].count())
                    print "%s" % ('=' * 133)
                    time.sleep(5)
                except KeyboardInterrupt:
                    print("Caught control-C interruption ....")
                    for hhh in hosts:
                        ssh_kill(hhh, "rcopy.py")
                    print "Terminated the data migration  !!!!!!!!"
                    sys.exit(1)
                except:
                    #print("waiting in except2")
                    time.sleep(0.5)
                    continue
    elif (type_of_operation == "sync"):
        pipe = [{'$group': {'_id': None, 'total': {'$sum': '$obj_size'}}}]
        src_db_size = list(db[Nscol].aggregate(pipe))[0]["total"]
        # print "src_db_size = " , src_db_size
        dst_db_size = 0
        # print "Source size = %d MB and Destination size = %d MB" % (src_db_size , dst_db_size )
        # print "Source size = %s and Destination size = %s" % (src_db_size, dst_db_size)
        while dst_db_size < src_db_size:
            try:
                pipe = [{'$group': {'_id': None, 'total': {'$sum': '$obj_size'}}}]
                dst_db_size = list(db[Ndcol].aggregate(pipe))[0]["total"]
                # print "dst_db_size = " , dst_db_size
                os.system('cls')
                print "S3 Migration status ..."
                print "Bucket Name: ", bucket_name
                print "%s" % ('=' * 133)
                print "Source size = %d MB and Destination size = %d MB" % (
                src_db_size / 1024 / 1024, dst_db_size / 1024 / 1024)
                print "Percentage of completion : [%-100s] %d%%" % (
                '#' * ((dst_db_size * 100) / src_db_size), ((dst_db_size * 100) / src_db_size))
                print "%s" % ('=' * 133)
                time.sleep(2)
            except KeyboardInterrupt:
                print("Caught control-C interruption ....")
                for hhh in hosts:
                    ssh_kill(hhh, "rcopy.py")
                print "Terminated the data migration  !!!!!!!!"
                sys.exit(1)
            except:
                #print("waiting in increment except2")
                time.sleep(2)
                continue
    else:
            print "Wrong operation..Progress bar cannot continue"

def run_increment_migration():
    logging.info('Starting ..run_increment_migration function ..')
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)  # defaults to port 27017
    logging.info('######################################################################')
    db = connection[db_name]
    scol = bucket_name + "_sourcecol"
    dcol = bucket_name + "_destinationcol"
    Nscol = bucket_name + "_Nsourcecol"
    Ndcol = bucket_name + "_Ndestinationcol"
    Rscol = bucket_name + "_Rsourcecol"
    Rdcol = bucket_name + "_Rdestinationcol"
    Rincrement = bucket_name + "_Rincrement"
    DELsource = bucket_name + "_DELsource"

    # db = connection.test
    # logging.info('Mongo database connection established ')
    # Nsourcecol = db.Nsourcecol
    # DELsource = db.DELsource
    # Rincrement = db.Rincrement
    # sourcecol = db.sourcecol

    src_collection_count = db[Nscol].count()
    # print "== The number of objects to be restarted are: ", src_collection_count
    # logging.info('The number of objects to be restarted are:' '%s', src_collection_count)
    # logging.info('Total number of objects in source are :' '%s', src_collection_count)
    # logging.info('S3 Migration started .......')
    # print "S3 Migration started ......  "  # pool = Pool(processes=number_of_process)
    # print "=============================================================="
    # start_time = time.time()
    hosts = [Node1, Node2, Node3]
    iterv = len(hosts)  # Number of nodes to do ssh
    logging.info('Getting the list of hosts and splitting the objects between hosts')
    if (src_collection_count % iterv) == 0:
        no_of_objects_per_node = src_collection_count / iterv
    else:
        no_of_objects_per_node = (src_collection_count / iterv) + 1
    # print "No_of_objects_per_node : ", no_of_objects_per_node

    # Distributing  the objects between the hosts . This code needs to be Dynamic
    # Creating commands to send over ssh . Assuming rcopy and input.
    skip_node1 = 0
    skip_node2 = no_of_objects_per_node + skip_node1
    skip_node3 = no_of_objects_per_node + skip_node2
    logging.info('Split between hosts completed .Generating the rcopy to run')
    cmd1 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (
    bucket_name, no_of_objects_per_node, skip_node1, number_of_process, type_of_operation)
    cmd2 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (
    bucket_name, no_of_objects_per_node, skip_node2, number_of_process, type_of_operation)
    cmd3 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (
    bucket_name, no_of_objects_per_node, skip_node3, number_of_process, type_of_operation)
    dict = {hosts[0]: cmd1, hosts[1]: cmd2, hosts[2]: cmd3}
    #print "cmd1:= " , cmd1
    #print "cmd2:= ",  cmd2
    #print "cmd3:= ",  cmd3
    # dict = { Node1 : cmd1, Node2 : cmd2, Node3 : cmd3 }
    logging.info('rcopy command created . Sending the command to hosts to start migration')
    logging.info('Starting ssh on all the hosts ')
    # sys.exit(0)
    threads = []
    for k, v in dict.items():
        # print "%s corresponds to %s"  % (k,v)
        # logging.info('%s',k , 'Node command is:', '%s',v)
        t = multiprocessing.Process(target=ssh_server, args=(k, v))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    logging.info('Ssh started . Migration started from individual hosts')
    logging.info('============================================================')
    logging.info('starting the progress bar ')
    progress_bar()
    # t.join()
    logging.info('All ssh completed .Migration is completed ')
    logging.info("#########################################################################")
    print "Data Migration has been completed  !!!!! "

def delete_oldsource_objects():
    logging.info('Starting ..delete_oldsource_objects ..')
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)  # defaults to port 27017
    db = connection[db_name]
    Rincrement = bucket_name + "_Rincrement"
    DELsource = bucket_name + "_DELsource"
    scol = bucket_name + "_sourcecol"
    dcol = bucket_name + "_destinationcol"

    DELlist = list(db[DELsource].find({}))
    #print "Keys to be delete are : "
    for ii in DELlist:
        #print "Delete key is : %s" , ii["obj_key"]
        logging.warn('Deleted the key : ''%s',ii["obj_key"])
        destination.delete_object(Bucket=bucket_name, Key=ii["obj_key"])
        db[scol].remove({"obj_key":ii["obj_key"]})
        db[dcol].remove({"obj_key":ii["obj_key"]})
    logging.info('Delete of all old objects completed ')
    if db[DELsource].find().count() != 0:
       print "============================================================="
       print "Objects deleted to be in-sync with current source"
       print "Data migration completed !!!!!"
       sys.exit(0)

def start_migration():
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)  # defaults to port 27017
    logging.info('######################################################################')
    logging.info('Function: START : start_migration ')
    logging.info('MongoDB database connection established ')
    logging.info('====================================================================')
    logging.info('Starting the migration operation')
    db = connection[db_name]
    scol = bucket_name + "_sourcecol"
    dcol = bucket_name  +"_destinationcol"
    db[scol].drop()
    db[dcol].drop()
    logging.info('MongoDB Source and Destination collections created ')
    # db[scol].insert({"bucket": bucket_name, "obj_key": 1, "obj_size": 222, "obj_etag": "abcedef", "obj_status": "False"})
    # db = connection.test
    # db.destinationcol.drop()
    # db.sourcecol.drop()
    # sourcecol = db.sourcecol
    # destinationcol = db.destinationcol
    #logging.info('Source and Destination collection created ')
    # Get the list of objects from source
    print "============================================================="
    print "Inventory of objects:"
    print "== Populating all the object names in the Source database   =="
    logging.info('Populating all the object names in the Source database')
    start_time = time.time()
    for key in source1.get_bucket(bucket_name):
        db[scol].insert({"bucket": bucket_name, "obj_key": key.key, "obj_size": key.size, "obj_etag": key.etag,
                          "obj_status": "False"})
        # print "key etag :" , key.etag ; #print "key name :" , key.key  #print "key size :", key.size
    stop_time = time.time() - start_time
    logging.info('Source collection created with all the objects. Total taken is : ''%s', stop_time)
    print "== Source database with list of all objects completed . Total time taken to get the list is : ", stop_time
    src_collection_count = db[scol].count()
    # src_collection_count = 28
    print "Total number of objects in source are : ", src_collection_count
    print "=============================================================="

    time.sleep(1)
    logging.info('Total number of objects in source are :' '%s', src_collection_count)
    logging.info('S3 Migration started .......')
    print "S3 Migration started ......  "  # pool = Pool(processes=number_of_process)
    print "=============================================================="
    start_time = time.time()
    hosts = [Node1, Node2, Node3] #, Node6]
    #hosts = [Node1, Node2, Node3, Node4, Node5, Node6]
    iterv = len(hosts)  # Number of nodes to do ssh
    logging.info('Getting the list of hosts and splitting the objects between hosts')
    if (src_collection_count % iterv) == 0:
        no_of_objects_per_node = src_collection_count / iterv
    else:
        no_of_objects_per_node = (src_collection_count / iterv) + 1
    # print "No_of_objects_per_node : ", no_of_objects_per_node

    # Distributing  the objects between the hosts . This code needs to be Dynamic
    # Creating commands to send over ssh . Assuming rcopy and input.
    skip_node1 = 0
    skip_node2 = no_of_objects_per_node + skip_node1
    skip_node3 = no_of_objects_per_node + skip_node2
    #skip_node4 = no_of_objects_per_node + skip_node3
    #skip_node5 = no_of_objects_per_node + skip_node4
    #skip_node6 = no_of_objects_per_node + skip_node5

    logging.info('Split between hosts completed .Generating the rcopy to run')
    cmd1 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (bucket_name, no_of_objects_per_node, skip_node1, number_of_process, type_of_operation)
    cmd2 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (bucket_name, no_of_objects_per_node, skip_node2, number_of_process, type_of_operation)
    cmd3 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (bucket_name, no_of_objects_per_node, skip_node3, number_of_process, type_of_operation)
    #cmd4 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (bucket_name, no_of_objects_per_node, skip_node4, number_of_process, type_of_operation)
    #cmd5 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (bucket_name, no_of_objects_per_node, skip_node5, number_of_process, type_of_operation)
    #cmd6 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (bucket_name, no_of_objects_per_node, skip_node6, number_of_process, type_of_operation)
    #dict = {hosts[0]: cmd1, hosts[1]: cmd2, hosts[2]: cmd3, hosts[3]: cmd4, hosts[4]: cmd5, hosts[5]: cmd6}
    dict = {hosts[0]: cmd1, hosts[1]: cmd2, hosts[2]: cmd3} #, hosts[3]: cmd4} #, hosts[4]: cmd5, hosts[5]: cmd6}
    #print "cmd1:= ", cmd1   #     print "cmd2:= ",  cmd2       # print "cmd3:= ",  cmd3
    #print "cmd2:= ", cmd2
    #print "cmd3:= ", cmd3
   #print "cmd4:= ", cmd4
    #print "cmd5:= ", cmd5
    #print "cmd6:= ", cmd6
    # dict = { Node1 : cmd1, Node2 : cmd2, Node3 : cmd3 }
    #print dict
    #exit(0)
    logging.info('rcopy commands created . Sending the command to hosts to start migration')
    logging.info('Function: CALL : ssh_server ')
    logging.info('Starting ssh on all the hosts ')
    threads = []
    for k, v in dict.items():
        # print "%s corresponds to %s"  % (k,v)
        # logging.info('%s',k , 'Node command is:', '%s',v)
        t = multiprocessing.Process(target=ssh_server, args=(k, v))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    logging.info('ssh started . Migration started from individual hosts')
    logging.info('============================================================')
    logging.info('starting the progress bar ')
    progress_bar()
    # t.join()
    logging.info('All ssh completed .Migration is completed ')
    logging.info("#########################################################################")
    stop_time = time.time() - start_time
    print "Data Migration has been completed  !!!!! "
    logging.info('Function: END : start_migration ')
    print "== Total time taken for migration  : ", stop_time

def start_restart_operation():
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)  # defaults to port 27017
    logging.info('######################################################################')
    logging.info('Mongo database connection established ')
    logging.info('====================================================================')
    logging.info('Starting RESTART operation')
    db = connection[db_name]
    scol = bucket_name + "_sourcecol"
    dcol = bucket_name + "_destinationcol"
    Nscol = bucket_name + "_Nsourcecol"
    Ndcol = bucket_name + "_Ndestinationcol"
    Rscol = bucket_name + "_Rsourcecol"
    Rdcol = bucket_name + "_Rdestinationcol"
    db[Rscol].drop()
    db[Rdcol].drop()
    #db[dcol].drop()
    db.Rsourcecol.drop()
    Rsourcecol = db.Rsourcecol

    # db = connection.test
    # db.destinationcol.drop()
    # db.sourcecol.drop()
    # db.Rsourcecol.drop()
    # db.Rdestinationcol.drop()
    # db.destinationcol.drop()
    # sourcecol = db.sourcecol
    # destinationcol = db.destinationcol
    # Rsourcecol = db.Rsourcecol
    # Rdestinationcol = db.Rdestinationcol

    # db.sourcecol.find({"obj_status": "True"}).forEach(function(doc) {db.Rsourcecol.insert(doc);})
    # db.full_set.aggregate([{ $match: {date: "20120105"}}, { $out: "subset"}]);
    logging.info('Using source collection, creating Rsourcecol with list of keys that are not migrated  ')
    start_time = time.time()
    pipe = [{'$match': {"obj_status": "False"}}, {'$out': "Rsourcecol"}]
    db[scol].aggregate(pipe)
    time.sleep(0.1)
    # pipe1 = [{'$match': {"obj_status": "True"}}, {'$out': "destinationcol"}]
    # db.sourcecol.aggregate(pipe1)
    stop_time = time.time() - start_time
    logging.info('Rsourcel created .. starting the migration.Time Taken:' '%s', stop_time)
    # Get the list of objects from source
    print "============================================================="
    print "Inventory:"
    print "== Source Database with list of objects completed . Total time taken to get the list is : ", stop_time
    src_collection_count = db.Rsourcecol.count()
    print "== The number of objects to be restarted are: ", src_collection_count
    logging.info('The number of objects to be restarted are:' '%s', src_collection_count)
    logging.info('Total number of objects in source are :' '%s', src_collection_count)
    logging.info('S3 Migration started .......')
    print "S3 Migration started ......  "  # pool = Pool(processes=number_of_process)
    print "=============================================================="
    #exit(0)
    # start_time = time.time()
    hosts = [Node1, Node2, Node3]
    iterv = len(hosts)  # Number of nodes to do ssh
    logging.info('Getting the list of hosts and splitting the objects between hosts')
    if (src_collection_count % iterv) == 0:
        no_of_objects_per_node = src_collection_count / iterv
    else:
        no_of_objects_per_node = (src_collection_count / iterv) + 1
    # print "No_of_objects_per_node : ", no_of_objects_per_node

    # Distributing  the objects between the hosts . This code needs to be Dynamic
    # Creating commands to send over ssh . Assuming rcopy and input.
    skip_node1 = 0
    skip_node2 = no_of_objects_per_node + skip_node1
    skip_node3 = no_of_objects_per_node + skip_node2
    logging.info('Split between hosts completed .Generating the rcopy to run')
    cmd1 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (
    bucket_name, no_of_objects_per_node, skip_node1, number_of_process, type_of_operation)
    cmd2 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (
    bucket_name, no_of_objects_per_node, skip_node2, number_of_process, type_of_operation)
    cmd3 = "python /root/cmd/rcopy.py %s %s %s %s %s" % (
    bucket_name, no_of_objects_per_node, skip_node3, number_of_process, type_of_operation)
    dict = {hosts[0]: cmd1, hosts[1]: cmd2, hosts[2]: cmd3}
    # print "cmd1:= " , cmd1
    # print "cmd2:= ",  cmd2
    # print "cmd3:= ",  cmd3
    # dict = { Node1 : cmd1, Node2 : cmd2, Node3 : cmd3 }
    logging.info('rcopy command created . Sending the command to hosts to start migration')
    logging.info('Starting ssh on all the hosts ')
    # sys.exit(0)
    threads = []
    for k, v in dict.items():
        # print "%s corresponds to %s"  % (k,v)
        # logging.info('%s',k , 'Node command is:', '%s',v)
        t = multiprocessing.Process(target=ssh_server, args=(k, v))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    logging.info('Ssh started . Migration started from individual hosts')
    logging.info('============================================================')
    logging.info('starting the progress bar ')
    progress_bar()
    # t.join()
    logging.info('All ssh completed .Migration is completed ')
    logging.info("#########################################################################")
    print "Data Migration has been completed  !!!!! "

def start_connectivity():
    print "Checking the connectivity of the scalars nodes and Migration servers : "
    scalar_access_check()
    network_access_check()
    print "Network connectivity -- Success !!!"
    print "Ready for the data migration ...."

if __name__ == "__main__":

   if len(sys.argv) == 1:
        introduction()
        sys.exit(0)
        #pass
   elif sys.argv[1] == "copy":
        type_of_operation = sys.argv[1]
   elif sys.argv[1] == "restart":
        type_of_operation = sys.argv[1]
   elif sys.argv[1] == "sync":
        type_of_operation = sys.argv[1]
   elif sys.argv[1] == "check_connectivity":
       type_of_operation = sys.argv[1]
   else:
        print "The input can be only copy and restart "
        sys.exit(0)
   #print "Type: ", type_of_operation

   if(type_of_operation == "copy"):
      start_migration()
   elif(type_of_operation == "restart"):
       print "This is restart operation !!!"
       start_restart_operation()
   elif(type_of_operation == "check_connectivity"):
       #print "Checking the connectivity of all nodes and systems!!!"
       start_connectivity()
   elif (type_of_operation == "sync"):
       print "Starting increment operation ..."
       logging.info('Starting increment operation !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
       print "============================================================="
       print "Inventory:"
       start_time = time.time()
       list_increment_objects()
       find_increment_objects()
       stop_time = time.time() - start_time
       print "== Time take to find new objects and objects to be deleted: ", stop_time
       print "============================================================="
       print "S3 Migration started ......  "
       print "=============================================================="
       run_increment_migration()
       delete_oldsource_objects()
       #print "Data Migration complete ....! "
   else:
        print "Wrong operation selected.Re-run using following option: "
        print "For Migration, use copy string"
        print "To restart migration, use restart string"
        print "To increment migration, use increment string"




