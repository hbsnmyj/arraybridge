#!/usr/bin/python

import ConfigParser
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
    if scidb_env['install_path'] == '':
        raise Exception("Invalid environment variable 'install_path'")
        

    scidb_py = os.path.join(scidb_env['install_path'],'bin','scidb.py')
    cmd = [scidb_py, 'stopall', scidb_env['cluster_name']]

    proc = subprocess.Popen(' '.join(cmd),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    out_text,err_text = proc.communicate()

    return proc.returncode,out_text,err_text

def start_scidb(config_ini=None, authentication_file=None, max_attempts=None):
    scidb_env = get_scidb_env()
    if scidb_env['install_path'] == '':
        raise Exception("Invalid environment variable 'install_path'")

    scidb_py = os.path.join(scidb_env['install_path'],'bin','scidb.py')
    cmd = [scidb_py, 'startall', scidb_env['cluster_name']]
    if (config_ini != None):
        cmd.append(config_ini)

    if authentication_file:
        cmd.extend(['--auth-file', authentication_file])

    if max_attempts:
        cmd.extend(['--max-attempts', str(max_attempts)])

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
    if scidb_env['install_path'] == '':
        raise Exception("Invalid environment variable 'install_path'")

    scidb_py = os.path.join(scidb_env['install_path'],'bin','scidb.py')
    cmd = [scidb_py, 'init-all', scidb_env['cluster_name']]

    proc = subprocess.Popen(' '.join(cmd),shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    out_text,err_text = proc.communicate('y\n')

    return proc.returncode,out_text,err_text

def restart_scidb(config_ini=None, authentication_file=None, max_attempts=None):
    ret,text_out,text_err = stop_scidb()
    assert ret == 0, text_err
    if ret != 0:
        raise Exception("Unable to stop scidb\nerr={0}\nini={1}\nauth={2}".format(
            text_err, config_ini, authentication_file))

    ret,text_out,text_err = start_scidb(
        config_ini,
        authentication_file=authentication_file,
        max_attempts=max_attempts)
    if ret != 0:
        raise Exception("Unable to start scidb\nerr={0}\nini={1}\nauth={2}".format(
            text_err, config_ini, authentication_file))

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

def get_ini_value(config_ini, section, key):
    value=""
    with open(config_ini,'r') as fd:
        config_ini_contents = fd.read()

        m = re.search('{0}(.*)=(.*)[^\n]+'.format(key),config_ini_contents)
        if (m is not None):
            print "security=", str(m.group())
            value = m.group().split("=")
            if len(value) == 2:
                value = value[1].strip()
    return value

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

class ConfigParserWithComments(ConfigParser.ConfigParser):
    # http://stackoverflow.com/questions/8533797/adding-comment-with-configparser

    #def optionxform(self, optionstr):
    #    return optionstr

    def add_comment(self, section, comment):
        """Write a comment to the section specified

        @param section The section to write to.  Or, '' if writing to a non-section area
        @param comment The comment to write to the section
        """
        if len(comment) == 0:
            self.set(section, ';', None)
        else:
            self.set(section, '; %s' % (comment,), None)

    def write(self, fp):
        """Write an .ini-format representation of the configuration state.

        @param fp Pointer to the file to be written to
        """
        if self._defaults:
            fp.write("[%s]\n" % ConfigParser.DEFAULTSECT)
            for (key, value) in self._defaults.items():
                self._write_item(fp, key, value)
            fp.write("\n")
        for section in self._sections:
            fp.write("[%s]\n" % section)
            for (key, value) in self._sections[section].items():
                self._write_item(fp, key, value)
            fp.write("\n")

    def _write_item(self, fp, key, value):
        """Write a key=value pair item to a file

        @param fp Pointer to the file to be written to
        @param key The 'key' portion of the key=value pair
        @param value The 'value' portion of the key=value pair
        """
        if key.startswith(';') and value is None:
            fp.write("%s\n" % (key,))
        else:
            fp.write("%s = %s\n" % (key, str(value).replace('\n', '\n\t')))


def allowable_auth_letters():
    """Returns the set of letters allowable in authentication names"""
    return "A-Za-z0-9,-_./"

def init_auth_file(user_name, user_password, auth_file_name):
    """Initialize the authentication file with the appropriate information

    @param user_name The user's name
    @param user_password The user's password (in plaintext format)
    @param auth_file_name The name of the file to initialize
    """
    pattern = re.compile("[{0}]+".format(allowable_auth_letters()))
    if not pattern.match(user_name):
        raise Exception("Invalid user name")

    if not pattern.match(user_password):
        raise Exception("Invalid user name")

    config = ConfigParserWithComments()
    config.optionxform = str  # Make the parser preserve case

    config.add_comment("", "https://en.wikipedia.org/wiki/ini_file")
    config.add_comment("", "")

    config.add_comment("", "INI file format")
    config.add_comment("", "---------------")
    config.add_comment("", "- The file is composed of comments, sections and parameters")
    config.add_comment("", "- Comments start with a semi-colon(:) at the beginning of a line")
    config.add_comment("", "- Section names are defined between square brackets([ and ]) and start at the beginning of a line")
    config.add_comment("", "- Parameters are defined as a name, followed by an equal sign followed by a value.  There can be")
    config.add_comment("", "   spaces before and after the equal sign.  Spaces at the end of the value are trimmed.")
    config.add_comment("", "- Empty lines are legal and skipped silently")
    config.add_comment("", "- If a value has a quote around it the quotes will be considered to be part of the value and not")
    config.add_comment("", "   removed")
    config.add_comment("", "")

    config.add_comment("", "Authentication file format")
    config.add_comment("", "---------------------------")
    config.add_comment("", "- The Authentication file must conform to the INI file format described above")
    config.add_comment("", "- The authentication file has a section called 'section_<name>' where name matches the value")
    config.add_comment("", "   specified for the security parameter in the config.ini file")
    config.add_comment("", "- For security_password there are two variables:  user-name and user-password")
    config.add_comment("", "- The user-name variable specifies the user's name, is not quoted, and must not have spaces")
    config.add_comment("", "   within the the value")
    config.add_comment("", "- The user-password variable specifies the user's password, is not quoted, and must not have spaces")
    config.add_comment("", "   within the the value")
    config.add_comment("", "- The user-name and user-password must conform to the following regular expression:")
    config.add_comment("", "   '[{0}]+'".format(allowable_auth_letters()))
    config.add_comment("", "- security_<name>, user-name, and user-password are case sensitive")
    config.add_comment("", "")

    config.add_section("security_password")
    config.set("security_password", "user-name",        user_name)
    config.set("security_password", "user-password",    user_password)

    # Write the configuration file to 'auth_file_name'
    with open(auth_file_name, 'wb') as configfile:
        config.write(configfile)

def create_auth_file(user_name, user_password, auth_file_name=None):
    auth_filename = auth_file_name
    if (not auth_filename):

        auth_filename = os.path.join(
            '/',
            'tmp','auth_{0}'.format(''.join(random.sample(string.letters,10)))
            )

    init_auth_file(user_name, user_password, auth_filename)
    os.chmod(auth_filename, stat.S_IRUSR | stat.S_IWUSR)
    return auth_filename
