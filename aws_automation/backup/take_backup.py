
from botocore.retryhandler import ExceptionRaiser
from flask import Blueprint,jsonify,request,redirect
from flask.wrappers import Response
from werkzeug.wrappers import response
from .functions import *
from aws_automation.config import *

import json


bp = Blueprint("take_backup", __name__)

@bp.route('/home')
def hello():
	return jsonify(status="success",message="hello world")

@bp.route('/api/user/create_bucket',methods=['PUT'])
def create_buckets():
    _json = request.get_json()
    _bucketName=_json['bucketName']
    cur = db.get_db().cursor()

    try:
        cur.execute("SELECT * from buckets where bucket_name=%s",[_bucketName])
        rows=cur.fetchall()

        if not rows: 
            if request.method=="PUT" and _bucketName:
                response = create_bucket(_bucketName)
                
                
                time=response["ResponseMetadata"]["HTTPHeaders"]["date"]
                print(time)
                cur.execute("INSERT into buckets (bucket_name,create_date) values (%s,%s)",[_bucketName,time])
                db.get_db().commit()
                return jsonify(status="success",bucket_name=_bucketName,time=time)
            else:
                return jsonify(status="failed", error="Invalid request method or empty bucket name"),409

        else:
            return jsonify(status="failed",error="Bucket name is already existed"),409


    except botocore.exceptions.ClientError as error: 
        return jsonify(status="error",error=error),500

    finally:
        cur.close()

@bp.route('/api/user/upload',methods=['PUT'])
def upload():
    _json = request.get_json()
    _bucketName =_json['bucketName']
    _ftphost=_json['host']
    _ftpport=_json['port']
    _ftpusername=_json['username']
    _ftppassword=_json['password']
    _ftppath = _json['path']
    _filename=_json['filename']
    _isdb = _json['isdb']
    _databaseuser = _json['dbuser']
    _databasepassword = _json['dbpass']
    _databasename = _json['databasename']
    _databasefilename = _json['dbfilename']
    _userstorageleft=_json['storageleft']
    cur = db.get_db().cursor()

    try:
        if request.method == "PUT":
            if _bucketName and _ftphost and _filename and _ftppassword and _ftpusername and _ftpport and _ftppath and _isdb:
                
                if check_ssh(_ftphost, _ftpport, _ftpusername, _ftppassword, _ftppath):
                    
                    ssh_client = paramiko.SSHClient()
                    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                    ssh_client.connect(hostname=_ftphost, port=int(_ftpport), username=_ftpusername, password=_ftppassword)
                    file_dir, file_name = os.path.split(_ftppath)
                    if _isdb: 
                        dbfile=f'{file_dir}/{_databasefilename}'
                        if dumpdb(ssh_client,_databaseuser,_databasepassword,_databasename,dbfile):
                            pass
                        else:
                            return jsonify(status="error",message="Database backup failed. check credentials"),403
                    
                    if archive_file(ssh_client,_filename,_ftppath):
                        pass
                    else:
                        return jsonify(status="error",message="Archiving file error. check credentials"),403

                    size=getfilesize(ssh_client,file_dir,_filename)
                    if _userstorageleft > size:
                        response = upload_data(_bucketName,file_dir,_ftphost,_ftpport,_ftpusername,_ftppassword)
                        if response:
                            cur.execute("Select backetid from buckets where bucketname=%s",[_bucketName])
                            rows=cur.fetchone()            
                            output=jsonify(rows)
                            data=output.get_json()
                            bid=data['bucketid']

                            cur.execute("INSERT into backupdetail (bucket_name,filename,ftphost,bucketid) values (%s,%s,%s,%s,%s)",[_bucketName,_filename,_ftphost,bid])
                            db.get_db().commit()
                            return jsonify(status="success",filesize=size,bucketname=_bucketName)
                        else:
                            return jsonify(status="error",message="upload could not possible. try again"),403
                    else:
                        return jsonify(status="error",message="You dont have sufficient storage. upgrade your space"),403

                else:
                    return jsonify(status="error",message="ssh connection is not established. check details"),403

            else:
                return jsonify(status="error",message="input fields must not be empty"),403
        else:
            return jsonify(status="error",message="invalid request method"),409

    except Exception as e:
        print(e)
        return jsonify(status="error",message="Something went wrong"),500

    finally:
        ssh_client.close()
        cur.close()


    

@bp.route('/api/user/download_Link',methods=['GET'])
def downloads():
    _json = request.get_json()
    _bucketName =_json['bucketName']
    _filename=_json['filename']

    try:
        if request.method == 'GET':
            if _bucketName and _filename:
                response= download(_bucketName,_filename)
                if response['status']=="success":
                    return redirect(response["url"],code=302)

                else:
                    return jsonify(status="error",message=response["message"])
            else:
                return jsonify(status="error",message="input fields must not be empty")
        else:
            return jsonify(status="error",message="Invalid request method")
    
    except Exception as e:
        print(e)
        return jsonify(status="error",message="Something went wrong")



@bp.route('/api/user/restore',methods=['PUT'])
def restore():
    _json = request.get_json()
    _bucketName =_json['bucketName']
    _filename=_json['filename']
    _ftppath=_json['path']
    _ftphost=_json['host']
    _ftpport=_json['port']
    _ftpusername=_json['username']
    _ftppassword=_json['password']
    _isdb = _json['isdb']
    _databaseuser = _json['dbuser']
    _databasepassword = _json['dbpass']
    _databasename = _json['databasename']
    _databasefilename = _json['dbfilename']

    try:
        if request.method=="PUT":
            if _bucketName and _ftphost and _filename and _ftppassword and _ftpusername and _ftpport and _ftppath and _isdb:

                if check_ssh(_ftphost, _ftpport, _ftpusername, _ftppassword, _ftppath):
                    
                        ssh_client = paramiko.SSHClient()
                        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                        ssh_client.connect(hostname=_ftphost, port=int(_ftpport), username=_ftpusername, password=_ftppassword)
                        file_dir, file_name = os.path.split(_ftppath)

                        response = restore(_bucketName,_filename,_ftppath,_filename,_ftphost,_ftpport,_ftpusername,_ftppassword)
                        if response:
                           if extract_file(ssh_client, _filename, file_dir):
                              remove_file(ssh_client,file_dir,_filename)
                              return jsonify(status="success",message="Restoration completed"),403
                           else:
                                return jsonify(status="error",message="Invalid path for extraction"),403

                        else: 
                            return jsonify(status="error" , message="restoration halted"),403
                else:
                        return jsonify(status="error",message="ssh connection is not established. check details"),403

            else:
                return jsonify(status="error",message="input fields must not be empty"),403
        else:
            return jsonify(status="error",message="invalid request method"),409 

    except Exception as e :
        return jsonify(status="error",message="something went wrong"),409  

    finally:
        ssh_client.close()
        

                           

    

    
