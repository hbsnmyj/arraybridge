#!/usr/bin/python
import subprocess
import sys
import os
import re
import stat
import random
import string

def get_scidb_env():
    d = {
        'install_path':os.environ.get('SCIDB_INSTALL_PATH',''),
        'scidb_host':os.environ.get('IQUERY_HOST','127.0.0.1'),
        'scidb_port':os.environ.get('IQUERY_PORT','1239'),
        'cluster_name':os.environ.get('SCIDB_CLUSTER_NAME',''),
        }
    return d

def stop_scidb():
    scidb_env = get_scidb_env()
    assert scidb_env['install_path'] != ''

    scidb_py = os.path.join(scidb_env['install_path'],'bin','scidb.py')
    cmd = [scidb_py, 'stopall', scidb_env['cluster_name']]

    proc = subprocess.Popen(' '.join(cmd),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    out_text,err_text = proc.communicate()

    return proc.returncode,out_text,err_text

def start_scidb(config_ini=None, user_name=None, user_password=None, max_attempts=None):
    scidb_env = get_scidb_env()
    assert scidb_env['install_path'] != ''

    scidb_py = os.path.join(scidb_env['install_path'],'bin','scidb.py')
    cmd = [scidb_py, 'startall', scidb_env['cluster_name']]
    if (config_ini != None):
        cmd.append(config_ini)

    if user_name and user_password:
        cmd.extend(['--user_name', user_name, '--user_password', user_password])
    else:
        # If user_name is specified then user_password MUST be specified.
        # Likewise, if user_password is specified then user_name MUST be
        # specified.  It is okay to have both or neither specified.
        assert (user_name==None) and (user_password==None)

    if max_attempts:
        cmd.extend(['--max_attempts', str(max_attempts)])

    print "start_scidb cmd=" + str(cmd)

    proc = subprocess.Popen(
        ' '.join(cmd),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    out_text,err_text = proc.communicate()

    return proc.returncode,out_text,err_text

def init_scidb():
    scidb_env = get_scidb_env()
    assert scidb_env['install_path'] != ''

    scidb_py = os.path.join(scidb_env['install_path'],'bin','scidb.py')
    cmd = [scidb_py, 'init-all', scidb_env['cluster_name']]

    proc = subprocess.Popen(' '.join(cmd),shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    out_text,err_text = proc.communicate('y\n')

    return proc.returncode,out_text,err_text

def restart_scidb(config_ini=None, user_name=None, user_password=None, max_attempts=None):
    ret,text_out,text_err = stop_scidb()
    assert ret == 0, text_err

    ret,text_out,text_err = start_scidb(
        config_ini,
        user_name=user_name,
        user_password=user_password,
        max_attempts=max_attempts)
    assert ret == 0, "err=" + text_err + " ini=" + config_ini

def change_security_mode(mode,config_ini):
    if (mode not in ['trust','password']):
        msg = 'Warning: unsupported security mode ({0}) requested!\n'.format(mode)
        sys.stderr.write(msg)

    config_ini_contents = ''

    with open(config_ini,'r') as fd:
        config_ini_contents = fd.read()

    m = re.search('security=[^\n]+',config_ini_contents)
    if (m is not None):
        config_ini_contents = config_ini_contents.replace(m.group(),'security={0}'.format(mode))
    else:
        config_ini_contents = '\n'.join([config_ini_contents,'security={0}'.format(mode)])

    with open(config_ini,'w') as fd:
        fd.write(config_ini_contents)

def emit_login_file(username,p_word,file_name=None):
    contents = [
        '{',
        '\"user-name\":\"{0}\",'.format(username),
        '\"user-password\":\"{0}\"'.format(p_word),
        '}'
        ]

    login_file_name = file_name
    if (not login_file_name):

        login_file_name = os.path.join(
            '/',
            'tmp','login_{0}'.format(''.join(random.sample(string.letters,10)))
            )

    with open(login_file_name,'w') as fd:
        fd.write('\n'.join(contents))

    os.chmod(login_file_name,stat.S_IRUSR | stat.S_IWUSR)

    return login_file_name
