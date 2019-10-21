__author__ = '33323'
from botocore.client import Config
import boto3
import time
import sys
import os
import utilities
from multiprocessing import Pool
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat
import pymongo
import logging
from datetime import datetime

hname=os.popen('hostname').read().rstrip()
logfile=datetime.now().strftime(hname+"_"+"rcopy.log"+'_%d_%m_%Y.log')
logging.basicConfig(filename=logfile,level=logging.DEBUG)
path = os.path.abspath('./cmd/aa1tox100.yaml')
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

#S3 connection using boto
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

def copy_key(key): #,etag,size):
     logging.info('Function : START : copy_key : ' '%s', key)
     try:
        logging.info('Migration started for key ' '%s', key)
        connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)
        db = connection[db_name]
        scol = bucket_name + "_sourcecol"
        dcol = bucket_name + "_destinationcol"
        Nscol = bucket_name + "_Nsourcecol"
        Ndcol = bucket_name + "_Ndestinationcol"
        Rscol = bucket_name + "_Rsourcecol"
        Rdcol = bucket_name + "_Rdestinationcol"
        Rsourcecol = db.Rsourcecol

        # db[scol].drop()
        # db[dcol].drop()
        # db = connection.test
        # sourcecol = db.sourcecol
        # destinationcol = db.destinationcol
        # Nsourcecol = db.Nsourcecol
        # Ndestinationcol = db.Ndestinationcol

        logging.info('Database connection done from the copy_key fucntion')
        if type_of_operation == "copy":
            list2 = db[scol].find({"obj_key": key }, {"obj_etag": 1, "obj_size": 1, "_id": 0})
        elif type_of_operation == "restart":
            list2 = db.Rsourcecol.find({"obj_key": key}, {"obj_etag": 1, "obj_size": 1, "_id": 0})
        elif type_of_operation == "sync":
            list2 = db[Nscol].find({"obj_key": key}, {"obj_etag": 1, "obj_size": 1, "_id": 0})
        else:
            logging.info('Failed to get the list')
        #print "List:"
        #print list2
        for ii in list2:
            etag = ii["obj_etag"]
            size = ii["obj_size"]
        logging.info('Fetching of source key etag and size completed ')
        print "source %s %s %s %s " % (bucket_name, key, etag, size)
        #exit(0)
        key = str(key)
        response = source.get_object(Bucket=bucket_name, Key=key)
        #copy the object to the destination
        #destination.upload_fileobj(response['Body'], bucket_name, key)
        if response['ContentLength'] > 5000000000:
            #print "Object size is big"
            logging.info('Object size is big')
            logging.debug('start nows')
            config = boto3.s3.transfer.TransferConfig(multipart_threshold= Tmultipart_threshold, multipart_chunksize=Tmultipart_chunksize, io_chunksize=Tio_chunksize,max_concurrency = Tmax_concurrency, use_threads=TUse_Threads)
            destination.upload_fileobj(response['Body'], bucket_name, key, Config=config)
        else:
            destination.upload_fileobj(response['Body'], bucket_name, key)
        logging.info('copy of key completed ')
        #print "destination %s %s %s %s " % (bucket_name, key, response['ETag'],response['ContentLength'])
     except:
         #print "Killing it "
         logging.error('Object didnt start at all...check connectivity')
         sys.exit(0)

     bucket = destination1.get_bucket(bucket_name)
     # bucket = source1.get_bucket(bucket_name)
     detag = bucket.get_key(key).etag
     dsize = bucket.get_key(key).size
     #print ("Src Key etag: ", etag)
     #print ("dst Key etag: ", detag)
     #print ("Src Key size: ", size)
     #print ("dst Key size: ", dsize)

     if type_of_operation == "copy" or type_of_operation == "restart" :
         #if (etag == response['ETag']) and (size == response['ContentLength']):
         if (etag == detag) and (size == dsize):
               #print "  ==> Object copied successfully "
               logging.info('Object copied successfully''%s', key)
               db[dcol].insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'], "obj_etag": response['ETag'],"obj_status": "True"})
               #Rdestinationcol.insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'], "obj_etag": response['ETag'],"obj_status": "True"})
               db[scol].update({"obj_key" : key}, {'$set': {"obj_status" : "True"}})
               logging.info('Object entry updated in destination database with TRUE tag')
               logging.info('================================================================')
               #stop_time = time.time() - start_time
               #print 'Migration of object : %s is in progress .  Time taken is : %s seconds' % (key, stop_time)
               return db[dcol].count()
         else:
               #print " ==> Object copied but has consistency issues  "
               logging.warn('Object copy NOT successfull for key' '%s', key)
               db[dcol].insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'], "obj_etag": response['ETag'], "obj_status": "Flase"})
               logging.info('Object entry updated in destination database with FLASE tag')
               logging.info('================================================================')
     elif type_of_operation == "sync":
         #if (etag == response['ETag']) and (size == response['ContentLength']):
	 if (etag == detag) and (size == dsize):
               #print "  ==> Object copied successfully "
               logging.info('Object copied successfully''%s', key)
               db[Ndcol].insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'], "obj_etag": response['ETag'],"obj_status": "True"})
               #Rdestinationcol.insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'], "obj_etag": response['ETag'],"obj_status": "True"})
               db[Nscol].update({"obj_key" : key}, {'$set': {"obj_status" : "True"}})
               Slist = list(db[scol].find({}))
               Slen = db[scol].find({}).count()
               Rcount=0
               for ii in Slist:
                   if ii["obj_key"] == key:
                       db[scol].update({"obj_key": key}, {'$set': {"obj_etag": etag, "obj_size": size,"obj_status": "True"}})
                       db[dcol].update({"obj_key": key}, {'$set': {"obj_etag": etag, "obj_size": size,"obj_status": "True"}})
                       break
                   else:
                      Rcount+=1
               if Rcount == Slen:
                  db[scol].insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'],"obj_etag": response['ETag'], "obj_status": "True"})
                  db[dcol].insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'],"obj_etag": response['ETag'], "obj_status": "True"})
               logging.info('Object entry updated in destination database with TRUE tag')
               logging.info('================================================================')
               #stop_time = time.time() - start_time
               #print 'Migration of object : %s is in progress .  Time taken is : %s seconds' % (key, stop_time)
               return db[dcol].count()
         else:
               #print " ==> Object copied but has consistency issues  "
               logging.warn('Object copy NOT successfull for key' '%s', key)
               db[Ndcol].insert({"bucket": bucket_name, "obj_key": key, "obj_size": response['ContentLength'], "obj_etag": response['ETag'], "obj_status": "Flase"})
               logging.info('Object entry updated in destination database with FLASE tag')
               logging.info('================================================================')
     else:
         logging.warn('Something wrong in copy keys function..type of operation error')
     logging.info('Function : END  : copy_key : ' '%s', key)

def get_list():
    logging.info('Function : START : get_list')
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)
    db = connection[db_name]
    scol = bucket_name + "_sourcecol"
    list1 = db[scol].find({}, {"obj_key": 1, "_id": 0}).limit(no_of_objects).skip(skip_limit)
    key_list = []
    for ii in list1:
        key_list.append(ii["obj_key"])
    return(key_list)
    logging.info('Function : END : get_list ')

def Rget_list():
    logging.info('Function : START : Rget_list ')
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)
    db = connection[db_name]
    Rscol = bucket_name + "_Rsourcecol"
    Rsourcecol = db.Rsourcecol
    list1 = db.Rsourcecol.find({}, {"obj_key": 1, "_id": 0}).limit(no_of_objects).skip(skip_limit)
    key_list = []
    for ii in list1:
        key_list.append(ii["obj_key"])
    return(key_list)
    logging.info('Function : END :  Rget_list ')

def Iget_list():
    logging.info('Function : START : Iget_list')
    connection = pymongo.MongoClient("mongodb://" + db_user + ":" + db_password + "@" + db_ip + "/" + db_name)
    db = connection[db_name]
    Nscol = bucket_name + "_Nsourcecol"
    list1 = db[Nscol].find({}, {"obj_key": 1, "_id": 0}).limit(no_of_objects).skip(skip_limit)
    key_list = []
    for ii in list1:
        key_list.append(ii["obj_key"])
    return(key_list)
    logging.info('Function : END : Iget_list')


if __name__ == "__main__":
   logging.info('Function : START : Main ')
   logging.info('##########################################################################')
   logging.info('ssh connection established going to next stage')
   logging.info('##########################################################################')
   logging.info('Connected to the Database ')
   bucket_name = str(sys.argv[1])
   no_of_objects = int(sys.argv[2])
   skip_limit= int(sys.argv[3])
   number_of_process = int(sys.argv[4])
   type_of_operation = str(sys.argv[5])
   if type_of_operation == "copy":
      key_list = get_list()
      logging.info(' Its copy operation ...!!!')
   elif type_of_operation == "restart":
       key_list = Rget_list()
       logging.info(' Its restart operation ...!!!')
   elif type_of_operation == "sync":
       key_list = Iget_list()
       logging.info(' Its increment operation ...!!!')
   else:
       print "wrong operation selected..choose copy or restart"
       logging.info('wrong operation selected..choose copy or restart')
   #print "Key list : " , key_list
   logging.info('Source object list created ')
   pool = Pool(processes=number_of_process)
   start_time = time.time()
   logging.info('Migration started ..calling the pool.map function')
   logging.info('Function : CALL : pool.map ')
   pool.map(copy_key, key_list)
   logging.info('Function : RETURN : pool.map ')
   logging.info("All keys copied . Closing ssh connection .Returing to the Main program ")
   logging.info('##########################################################################')
   logging.info('Function : END : Main ')

