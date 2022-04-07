import paramiko
from paramiko import AuthenticationException
from paramiko.ssh_exception import NoValidConnectionsError
import os
import json
from botocore.retries import bucket
import boto3
import botocore
from aws_automation.config import *
from scp import SCPClient
import io
from botocore.client import Config


class WrongAttribute(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value
    


#This method is used to remove files

def remove_file(ssh_client, path, file_name):
    remove_command = f'cd {path};rm {file_name}'
    stdin, stdout, stderror = ssh_client.exec_command(remove_command)
    exit_status = stdout.channel.recv_exit_status()


#This function will check ssh connection is valid or not

def check_ssh(host, ssh_port, username, password, path) -> (bool,str):
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh_client.connect(hostname=host, port=ssh_port, username=username, password=password)

        file_dir, file_name = os.path.split(path)

        if bool(file_name):
            if not check_exist(ssh_client, file_dir, file_name, False):
                return False, "Path not found"
            else: 
                return True,"connection okay for file"
        else:
            if not check_exist(ssh_client, file_dir, None, True):
                return False,'path not found'
            else:
                return True,"connection okay"

        ssh_client.close()

    except TimeoutError:
        return False,'Ip address is incorrect or server down'

    except AuthenticationException:
        return False,'Username or password invalid'

    except NoValidConnectionsError:
        return False,'Port number is invalid'


        
def check_exist(ssh_client, path, file_name, is_directory) -> bool:
    if is_directory:
        checking_command = f'if test -d {path}; then echo "file exist"; fi'
    else:
        checking_command = f'cd {path}; if test -f {file_name} ; then echo "file exist"; fi'
    stdin, stdout, stderror = ssh_client.exec_command(checking_command)
    return stdout.read().decode('utf-8').__contains__("file exist")


def archive_file(ssh_client, file_name, path):
    file_dir, filename = os.path.split(path)

    if filename:
        archiving_command = f"cd {file_dir}; tar -czf {file_name} {filename}"
        stdin, stdout, stderror = ssh_client.exec_command(archiving_command)
        exit_status = stdout.channel.recv_exit_status()
    else:
        archiving_command = f"cd {file_dir}; tar -czf {file_name} *"
        stdin, stdout, stderror = ssh_client.exec_command(archiving_command)
        exit_status = stdout.channel.recv_exit_status()

    error = stderror.read()
    if exit_status != 0 or error:
        return False
    
    if check_exist(ssh_client, path, file_name,None):
        return True

    return False

def extract_file(ssh_client, file_name, path):
    file_dir,filename = os.path.split(path)
    archiving_command = f"cd {file_dir}; tar -xf {filename}"
    stdin, stdout, stderror = ssh_client.exec_command(archiving_command)
    exit_status = stdout.channel.recv_exit_status()
    error = stderror.read()
    if exit_status != 0 or error:
        return False
    
    if check_exist(ssh_client, path, file_name,None):
        return True

    return False

def ssh(client,cmd):
    out = []
    stdin, stdout, stderror = client.exec_command(cmd)
    exit_status = stdout.channel.recv_exit_status()

    if stderror or exit_status != 0:
        return False
    else:
        return True

def dumpdb(client,db_user,db_password,db_name,path,filename):
    savefile=path+"/"+filename
    dump = ssh(f'mysqldump -u {db_user} -p{db_password} {db_name}> {savefile}')
    if ssh(client,dump):
        return True
    else:
        return False

def importdb(client,db_user,db_password,db_name,path):
    file_dir,filename = os.path.split(path)
    dump = ssh(f'mysql -u {db_user} -p{db_password} {db_name} < {path}')
    if ssh(client,dump):
        remove_file(client,file_dir,filename)
        return True
    else:
        return False

def create_bucket(bucketName):
    try:
        print(bucketName)
        aws_s3_client = boto3.client('s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        location = {
            'LocationConstraint':loc
        }
        response=aws_s3_client.create_bucket(
            Bucket=bucketName,
            CreateBucketConfiguration=location
        )
        return response

    except Exception as e:
        print(e)
        return False

def open_ftp_connection(ftp_host, ftp_port, ftp_username, ftp_password): 

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
    try: 
        transport = paramiko.Transport((ftp_host, ftp_port)) 
    except Exception as e: 
        return 'conn_error' 
    try: 
        response=transport.connect(None,ftp_username, ftp_password)
    except Exception as identifier: 
        return 'auth_error' 
    ftp_connection = paramiko.SFTPClient.from_transport(transport)
    return ftp_connection

def upload_data(bucketname, ftp_file_path, ftp_host, ftp_port, ftp_username, ftp_password):

    try:
        file_dir,filename = os.path.split(ftp_file_path)
        ftp_connection = open_ftp_connection(ftp_host, int(ftp_port), ftp_username, ftp_password)
        ftp_file = ftp_connection.file(ftp_file_path, 'r') 
        s3_connection =  boto3.client('s3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
        ) 
        ftp_file_size = ftp_file._get_size()
        ftp_file_data = ftp_file.read() 
        ftp_file_data_bytes = io.BytesIO(ftp_file_data)
        response=s3_connection.upload_fileobj(ftp_file_data_bytes, bucketname, filename,
                                          ExtraArgs={'ACL': 'public-read'})
        
        if response == None:
            return True
    except Exception as e:
        print(e)
        return False 

    finally:
        ftp_file.close()

def download(bucketname,bucketfilename):
    try:
        aws_s3_client = boto3.client('s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=loc,
            endpoint_url=f"https://s3.{loc}.amazonaws.com",
            config=Config(signature_version='s3v4')
      
        )
        BUCKET_NAME = bucketname
        BUCKET_FILE_NAME = bucketfilename

        url = aws_s3_client.generate_presigned_url(ClientMethod='get_object',
                                               Params={'Bucket': BUCKET_NAME,
                                                       'Key': BUCKET_FILE_NAME
                                                       },ExpiresIn=3600)
        return {"status":"success","url":url}

    except Exception as e:
        print(e)
        return {"status":"error", "message":e}
    


def restore(bucketname,bucketfilename,path,localfilename,host,port,username,password):
    try:
        s3_connection =  boto3.client('s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        source_response = s3_connection.get_object(Bucket=bucketname,Key=bucketfilename)
        transport = paramiko.Transport((host,int(port)))
        transport.connect(None,username,password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        with sftp.open(f'{path}{localfilename}', 'wb', 32768) as f:
            s3_connection.download_fileobj(bucketname, bucketfilename, f)

        return True

    except Exception as e:
        print(e)
        return False

    finally:
        # Closes the connection
        sftp.close()

def getfilesize(sshclient,path,filename):
    getfilesize_command = f'cd {path}; stat --format="%s" {filename}'
    stdin, stdout, stderror = sshclient.exec_command(getfilesize_command)
    return int(stdout)

def checkfile(sshclient,path):
    file_dir,filename = os.path.split(path)
    command = f'if test -f {path} ; then echo "1" ; else echo "2";fi '
    if command == 1:
        return True
    else:
        return False
