"""
Common functionalities for breeze can be defined/written here
Any functions that is being called from "automation/ddc/testsuites/Regression/Breeze" can be defined/writen here
"""
import re
import datetime
import logging
import sys
import os
import json
import imp
import subprocess
import random
import time
import math
import pandas as pd
import xml.etree.ElementTree as et
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join('lib')))
from robot.api.deco import keyword
from DNA_Tools import DNACommonTools, CommandError
run_cmd = DNACommonTools()
exe_cmd = DNACommonTools()
from robot.api import logger
from robot.api.deco import keyword
sys.path.append(os.getcwd())
from util.ssh_client import SSHClient
ssh_client = SSHClient()
P4PATH = os.path.join(Path.home(), "workspace", "projects", "qa", "automation", "tools")  # jenkins
#P4PATH = os.path.join(Path.home(), "perforce", "projects", "qa", "automation", "tools")  # local
copy_perforce_file = imp.load_source('copy_netconfig_file', 'lib/perforce_lib.py')


class QueryTableError(Exception):
    pass


@keyword('CHECK BREEZE QUERY TABLES')
def get_query_table(host, table_name):
    flag = False
    console = logging.StreamHandler()
    logging.getLogger().addHandler(console)

    # command = /a/bin/tbl-fetch 0 tableprov bzsmoosh_pipeline_stream_stats_1hr
    table_check_command = "/a/bin/tbl-fetch 0 tableprov " + table_name
    table_check_output = run_cmd.remoteExecCommand(host , table_check_command)[ host ]
    if ("Table rows: 0" in table_check_output):
        logging.warn("Data is not present in " + table_name)
        logging.info("Full Table Content : " + table_check_output)
        flag = True
    elif ("Table rows:"):
        logging.info("Data is present in " + table_name)
        logging.info("Full Table Content : " + table_check_output)
        flag = True

    return flag

@keyword('VALIDATE COLUMN DATA MISMATCH')
def validate(host, table_name, component):
    """Get the table given with following query.
            Query:select * from bzprocgrp_pipeline_process_stats_1hr where nrecords_written>0 and rec_size<=0;
            Result: No data expected 
    
    """
    flag=False
    if component == "bzproc":
        command = f"ssh -o StrictHostKeyChecking=no root@{host} '/a/bin/sql2 -q agg-ddc.shared.qa.akamai.com \"SELECT * FROM {table_name} WHERE nrecords_written>0 and rec_size<=0 AND pipeline_name not in (\'bz_lds_ax_etp_dns_4\', \'bz_lds_ax_dns_4\');\"'"
    elif component == "bzsmoosh":
        command = f"ssh -o StrictHostKeyChecking=no root@{host} '/a/bin/sql2 -q agg-ddc.shared.qa.akamai.com \"select * from {table_name} where num_records_processed > 0 and bytes_processed<=0;\"'"
    logging.info(f" The query ran is ==> {command}")
    result = subprocess.run(command,shell=True,text=True,capture_output=True)
    output = result.stdout
    logging.info(f"The Tableis ==> {output}")
    if output.split()[-3] == '0':
        logging.info(f"Table is validated and no discrepancy found in the Written records and processed bytes")
        flag=True
    else:
        logging.info(f"Discrepancy found. The table should return 0 rows but it is returning this ==> {output.split()[-3]}")
        flag=False
    return flag

@keyword("GET TABLE")
def get_table_from_agg(table_name: str, host: str, aggregator: str, timeout=20) -> pd:
    """Get the content from given query table.
            Args:
                table_name : table to be retrieved
                host       : machine to be used for running the query
                aggregator : aggregator to provide the data
                timeout    : timeout for command execution

            Returns:
                content of query table (pandas package)
    """
    sql_query = f"\'SELECT * FROM {table_name}\'"
    table = f"{table_name}.csv"
    command = f"ssh root@{host} \"/a/bin/sql2 --csv -q {aggregator} {sql_query}\" > {table}"
    pipe = subprocess.Popen(command.encode(), shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    pipe.wait(timeout)
    pd_tb = pd.read_csv(table)
    variables = list(pd_tb.iloc[0])
    if "time?" in variables:
        logger.console(variables.index("time?"))
        time_column = variables.index("time?")
        time_column_name = pd_tb.columns[time_column]
        pd_tb[time_column_name][1:] = pd.to_datetime(pd_tb[time_column_name][1:], unit="s")
    logger.console(pd_tb)
    return pd_tb


@keyword("VALIDATE TABLE CONTENT")
def validate_table(table: pd):
    """Validate content from given table.
            Args:
                table : table to be verified [pandas package]

            Returns:
                raises QueryTableError on failure
    """
    if not check_date(table=table):
        raise QueryTableError(f"Validation not successful for table: {table}")


def check_date(table: pd):
    """Check data from given table.
            Args:
                table : table to be verified [pandas package]

            Returns:
                True if there is data matching the requirements, False otherwise
    """
    variables = list(table.iloc[0])
    ip_column = variables.index("ip?")
    ip_column_name = table.columns[ip_column]
    table.drop(0, inplace=True)
    if "time?" in variables:
        time_column = variables.index("time?")
        time_column_name = table.columns[time_column]
        table.sort_values([ip_column_name, time_column_name], ascending=[True, False], inplace=True)
        if time_column_name == "bucket":
            filtered_bucket = table[table[time_column_name] >= datetime.datetime.utcnow() - datetime.timedelta(minutes=75)]
            print(f"Len of filtered dataframe: {len(filtered_bucket)}")
            return len(filtered_bucket) >= 1
        else:
            filtered_time = table[table[time_column_name] >= datetime.datetime.utcnow() - datetime.timedelta(minutes=15)]
            print(f"Len of filtered dataframe: {len(filtered_time)}")
            return len(filtered_time) >= 1
    else:
        table.sort_values([ip_column_name], ascending=[True], inplace=True)
        print(f"Len of dataframe: {len(table)}")
        return len(table) >= 1

@keyword('VERIFY_DATA_IN_SMOOSHBOX_DIRECTORY')
def verify_latest_files_exist(host, num_previous_days):
    """Verifies if there is data matching date requirement in smooshdata directory
            Args:
                host              : machine ip to be used in verification
                num_previous_days : number of days from which data should be included

            Returns:
                True on success, False on failure
    """

    # example command : "cd /ghostcache/smooshdata/bzflud/0; find . -size +0c -mtime -4 -ls | tail -5"
    latest_data = False

    run_cmd.remoteExecCommand(host, 'cd')
    command_output = run_cmd.remoteExecCommand(host, 'cd /ghostcache/smooshdata/bzflud/0; find . -size +0c -mtime -'+str(num_previous_days) + " -ls | tail -5")[host]
    logging.info(command_output)
    ### IF there is not data, then this below "if" wont be executed
    if command_output:
        return True

    run_cmd.remoteExecCommand(host, 'cd')
    command_output = run_cmd.remoteExecCommand(host, 'cd /ghostcache/smooshdata/bzflud/1; find . -size +0c -mtime -'+str(num_previous_days) + " -ls | tail -5")[host]
    logging.info(command_output)
    if command_output:
        return True

    run_cmd.remoteExecCommand(host, 'cd')
    command_output = run_cmd.remoteExecCommand(host, 'cd /ghostcache/smooshdata/bzflud/upload; find . -size +0c -mtime -'+str(num_previous_days) + " -ls | tail -5")
    logging.info(command_output)
    if command_output:
        return True

    return latest_data


@keyword('DETERMINE QP SPLITTER IP')
def determine_qp_splitter_ip(ghost):
    """To determine the qpsplit/bzproc machine on which ghost sends data.
            Args:
                ghost : ip of ghost machine

            Returns:
                qpsplit machine ip on success, None otherwise
    """
    cmd = 'netstat -anp | grep ESTABLISHED | grep bzc | head -n 1'
    qp_ip = run_cmd.remoteExecCommand(ghost, cmd)[ghost]
    match = re.match('(.+)([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)(.+ +)([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)(.+)',
                     qp_ip)
    if match:
        logger.info('BZproc machine is '+match.group(4), also_console=True)
        return match.group(4)
    else:
        logger.error('Unable to identify BZproc machine IP')
        return None


@keyword('DETERMINE BZSMOOSH IP')
def determine_bzsmoosh_ip(qp_ip):
    """To determine the bzsmoosh machine on which qpsplit/bzproc machine sends data.
                    It takes below arguments:
                    1. QPsplit/BZproc ip
                    """
    # this command fetches the established connection from bzprocgrp to bzsmoosh running on port 11377 on another machine
    cmd = "netstat -anp | awk \"\\$5 ~ /:11377\\$/ && /ESTABLISHED/ && \\$5 !~ /^127\.0\.0\.1:/\" | head -n 1"

    bzsmoosh_ip = run_cmd.remoteExecCommand(qp_ip, cmd)[qp_ip]
    match = re.match('(.+)([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)(.+ +)([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)(.+)',
                     bzsmoosh_ip)
    if match:
            logger.info('BZsmoosh machine is ' + match.group(4), also_console=True)
            return match.group(4)
    else:
        logger.error('Unable to identify BZsmoosh machine IP')
        return None

@keyword('OBTAIN STREAM DEFINITION')
def obtain_stream_definition(bzsmoosh_ip,stream_id):
    """To get the content of requested stream json and extract useful info out of that.
                It takes below arguments:
                1. bzsmoosh_ip
                2. stream_id
    """
    cmd = 'cat /a/share/logger/mdt/ddcdir.bzflud.streams.conf/'+stream_id+'.json'
    logger.info('Obtaining stream definition from the file /a/share/logger/mdt/ddcdir.bzflud.streams.conf/{0}.json \
using BZsmoosh {1} machine'.format(stream_id,bzsmoosh_ip),also_console=True)
    stream_definition_as_string = run_cmd.remoteExecCommand(bzsmoosh_ip, cmd)[bzsmoosh_ip]
    logger.info('\n\nStream definition is :\n'+stream_definition_as_string+'\n\n')
    definition = json.loads(stream_definition_as_string)
    return definition


@keyword('GET STREAM DEFINITION')
def get_stream_definition(bzsmoosh_ip, stream_id, network='ddc_commercial'):
    """Get the content of requested stream_id.json and extract useful info out of that.
            Args:
                bzsmoosh_ip : ip of bzsmoosh machine
                stream_id   : id of json file
                network     : tested network

            Returns:
                content of json file (dictionary), name of field containing Arl ID on success, False, None on failure
    """
    if network == 'ddc_commercial':
        command = 'cat /a/bzflud/metadata/ddcdir.bzflud.streams.conf/'+stream_id+'.json'
    elif network == 'govddc':
        command = 'cat /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/' + stream_id + '.json'
    #logger.info('Obtaining stream definition from the file /a/share/logger/mdt/ddcdir.bzflud.sqa2_streams.conf/{0}.json \
#using BZsmoosh {1} machine'.format(stream_id,bzsmoosh_ip),also_console=True)
    output, return_code = run_cmd.remote_exec_command_code(bzsmoosh_ip, command)
    logging.info("Output: " + output)
    if output == "" or "No such file or directory" in output:
        logging.info("Json file content not found")
        return False, None
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False, None
    if "objects" in output:
        field_name = "objects"
    else:
        field_name = "properties"
    definition = json.loads(output)
    return definition, field_name


@keyword('DECRYPT SECRET')
def decrypt_secret(ip_address, index, key):
    """To get the encoded keys which will be used to connect with cloud destination.
                It takes below arguments:
                1. ip_address : ip of bzsmoosh machine
                2. index : index of certs to be used.
                3. key : keys to be encrypted.
    """
    decryption_cmd = "echo "+str(key)+" | base64 -d | openssl pkeyutl -decrypt -inkey /bzflud/secrets/bzflud.crt."+str(index)+".private_key -pkeyopt rsa_padding_mode:oaep -pkeyopt rsa_oaep_md:sha256"
    logger.info('key decryption command is : \n' + decryption_cmd+'\n\n', also_console=True)
    decrypted_key, return_code = run_cmd.remote_exec_command_code(ip_address, decryption_cmd)
    logger.info('Decrypted key is : \n' + decrypted_key+'\n\n', also_console=True)
    if 'error' in decrypted_key.lower() or decrypted_key == '' or "No such file or directory" in decrypted_key:
        logging.info('Error while decrypting secrets')
        return False, None
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False, None
    return True, decrypted_key


@keyword('GENERATE TRAFFIC USING NIKKO')
def generate_traffic_using_nikko(ghost, requests_count, url_object, arl_host, pool=100, ulimit=0, timeout=20) -> tuple:
    """ Generates the traffic from ghost which will be uploaded on the cloud destination.

               Args:
                   ghost          : ghost machine IP (str)
                   requests_count : number of requests to be generated by nikko (str)
                   url_object      : url object to be used by nikko (str)
                   arl_host       : ARL hostname (str)

               Returns: True, number of requests sent on success, False, None on failure

    """
    nikko_out_file = '/tmp/nikko_out'
    cmd = './nikko -url "http://localhost' + url_object + '" -arlhost ' + arl_host + ' -number ' + requests_count + f' -pool {pool} -ulimit {ulimit} > ' + nikko_out_file
    logging.info('Generating traffic using nikko command:\n'+cmd)
    output, return_code = run_cmd.remote_exec_command_code(host=ghost, command=cmd, timeout=timeout)
    logging.info("Nikko output: \n" + output)
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False, "None"

    cmd = 'grep \"Target-info:\" ' + nikko_out_file
    output, return_code = run_cmd.remote_exec_command_code(ghost, cmd)
    logging.info("Nikko results - stats: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("Retrieving information regarding nikko traffic generation unsuccessful")
        return False, "None"
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False, "None"
    output = output.replace(',', '')
    requests_sent = output.split()[-2].split("=")[-1]
    status = output.split()[-1]
    logging.info("Requests sent: " + requests_sent)
    logging.info("Operation status: " + status)
    if requests_sent == requests_count and status == ("200-OK=" + requests_count):
        logging.info("Traffic generation operation successful")
        return True, requests_sent
    else:
        logger.error('Traffic generation operation unsuccessful')
        return False, requests_sent


@keyword("ENSURE THAT NIKKO IS PRESENT")
def check_nikko_present(host: str) -> bool:
    """
    Function checks if nikko is present on ghost machine and if not, copies it over from P4 repo.
    :param host: ghost machine IP
    :return: None
    """
    filename, local_path, remote_path, perforce_filepath = "filename", "local_path", "remote_path", "perforce_filepath"
    expected_files = [
        {
            filename: "nikko",
            local_path: P4PATH,
            remote_path: "/root/",
            perforce_filepath: "//projects/qa/automation/tools/nikko"
        }
    ]
    for file in expected_files:
        command = f"ls -l {file[remote_path]}"
        output = run_cmd.remote_exec_command_error(host=host, command=command)
        if file[filename] not in output:
            nikkoTool_filepath = copy_perforce_file.copy_netconfig_file(file[perforce_filepath])
            logger.info(f"File: {file[filename]} not found on {host} in {file[remote_path]}.\n"
                        f"Copying from {nikkoTool_filepath}.")
            copy_command = f"scp {nikkoTool_filepath} root@{host}:{file[remote_path]}"
            pipe = subprocess.Popen(copy_command.encode(), shell=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
            pipe.wait(20)
            if pipe.returncode != 0:
                raise CommandError(f"scp command returned error code: {pipe.returncode} with the following"
                                   f"stderr: {pipe.stdout}")
        else:
            logger.info(f"File {file[filename]} is present on machine: {host}. Continuing.")
            return True


@keyword("ENSURE THAT BZFLUD CERT IS PRESENT")
def check_bzflud_cert_present(host: str, index: str, streamid: str):
    """
    Function checks if required bzflud certificate is present on bzsmoosh machine.
    :param host: bzsmoosh machine IP
    :param index: certificate index
    :param streamid: stream_id
    :return: True on success, False on failure
    """
    filename, remote_path = "filename", "remote_path"
    expected_files = [
        {
            filename: f"bzflud.crt.{index}.private_key",
            remote_path: "/bzflud/secrets/"
        },
        {
            filename: f"bzflud.crt.{index}.certificate",
            remote_path: "/bzflud/secrets/"
        },
        {
            filename: f"bzflud.crt.{index}.chain_cert",
            remote_path: "/bzflud/secrets/"
        }
    ]
    for file in expected_files:
        command = f"ls -l {file[remote_path]}"
        output, return_code = run_cmd.remote_exec_command_code(host, command)
        logging.info("Bzflud secrets present on bzsmoosh: \n" + output)
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        if file[filename] not in output:
            logging.info(f"File: {file[filename]} not found on {host} in {file[remote_path]}. "
                         f"No private_key file on bzsmoosh machine matching kmi_key_idx={index} from {streamid}.json file\n")
            return False
        else:
            logger.info(f"File {file[filename]} is present on machine: {host}. Continuing.")
            return True


@keyword('GET CURRENT TIME')
def get_current_time():
    """Gets current time

            Returns:
                current time
    """
    return datetime.datetime.now()


@keyword('LATENCY')
def latency(start, end):
    """To get the difference in minutes between two timestamps."""
    time_difference = round((end-start).total_seconds()/60.0,2)
    logger.info('Time difference is :'+str(time_difference)+' minutes')
    return time_difference


@keyword('CHECK IF BZS IS PRESENT')
def check_if_bzs_is_running(host):
    check_bzs_command = "/a/sbin/akamai_run check akamai | grep bzs"
    check_bzs_output =run_cmd.remoteExecCommand(host, check_bzs_command)[host]
    if(check_bzs_output == ''):
        return False
    else:
        return True


@keyword('CHECK IF LOGS ARE POPULTED WITH LATEST DATE')
def check_if_logs_are_populated_with_latest_date(host, logs_list):
    logs_list = logs_list.split(",")
    flag = True
    for log in logs_list:
            command = "cat /a/logs/" + log +  " | wc -l"
            words =run_cmd.remoteExecCommand(host, command)[host]
            if("No such file or directory" in words):
                flag = False
                logging.error(log + " file is not present on qpsplit machine")
            elif (int(words) >= 10):
                logging.info("No of entries in " + log + "  is " + words)
            else:
                flag = False
                logging.error("No of entries in " + log + "  is " + words)
    return flag

@keyword('CHECK IF LOGS HAVE ERRORS')
def check_if_logs_have_errors(host , logs_list):
    logs_list = logs_list.split(",")
    flag = False
    for log in logs_list:
        command = "cat /a/logs/" + log +  " | grep -i error"
        error_check =run_cmd.remoteExecCommand(host, command)[host]
        if(error_check != ''):
            logging.error(log + " contains errors")
            flag = True
        else:
            logging.info(log + " does not contain error")
    return flag


@keyword('CHECK IF DIRECTORY IS EMPTY')
def check_if_directory_is_empty(host, dir_to_check):
    """ Checks whether specified directory is empty

               Args:
                   host         : machine IP (str)
                   dir_to_check : directory to be checked (str)

               Returns: True on success, False on failure

    """
    command = "cd " + dir_to_check
    #contents_output = run_cmd.remoteExecCommand(host, command)[host]
    contents_output, return_code = run_cmd.remote_exec_command_code(host, command)
    logging.info(contents_output)
    if contents_output == "":
        logging.info(dir_to_check + " is empty")
        return True
    elif return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return True
    return False


@keyword('CHECK IF A FILE IS PRESENT UNDER DIRECTORY')
def check_if_a_file_is_present_under_directory(host, dir_to_check, file_to_check):
    """ Checks whether specified file is present in specified directory

               Args:
                   host          : machine IP (str)
                   dir_to_check  : directory to be checked (str)
                   file_to_check : file to be checked (str)

               Returns: True on success, False on failure

    """
    command = "cd " + dir_to_check + " ; ls -ltr | grep " + file_to_check
    #check_output =run_cmd.remoteExecCommand(host, command)[host]
    contents_output, return_code = run_cmd.remote_exec_command_code(host, command)
    logging.info(contents_output)
    if contents_output == "":
        logging.info(dir_to_check + " does not contain the file: " + file_to_check)
        return False
    elif return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    return True


@keyword('EXECUTE COMMAND')
def execute_cmd(host, command):
    check_output = run_cmd.remoteExecCommand(host, command)[host]
    if check_output:
        return True
    else:
        return False


@keyword('MODIFY BZSMOOSH CONFIG')
def modify_bzsmoosh_config(host, text, replaceTo):
    modify_config = "cd /a/share/logger/mdt/ddcdir.bzsmoosh.conf; sed -i ':a;N;$!ba;s/"+text+"/"+replaceTo+"/2' ddc.bzsmoosh.tunnel.xml"
    check_config = run_cmd.remoteExecCommand(host, modify_config)[host]
    verify_config = "cd /a/share/logger/mdt/ddcdir.bzsmoosh.conf; cat ddc.bzsmoosh.tunnel.xml"
    check_config_output = run_cmd.remoteExecCommand(host, verify_config)[host]
    return check_config_output


@keyword("EXTRACT PID")
def get_pid(remote_ip, proc, user="logger"):
    """
    Function to get count of procgrps running on QP
    Args: 1. remote_ip :  remote IP address
    Args: 2. proc: process name
    Returns: PID
    """
    logger.info("Checking process id of %s on %s" %(proc,remote_ip),also_console=True)
    cmd = "pgrep -u "+user+ " -f "+proc
    pid =ssh_client.execute_command(cmd=cmd, hosts=remote_ip)[remote_ip]
    logger.info("pid of proc %s is  %s" %(proc,pid),also_console=True)
    return pid


@keyword("KILL PROC")
def kill_proc(remote_ip, pid, arg=""):
    """
    Function to kill process on remote ip
    Args: 1. remote_ip :  remote IP address
    Args: 2. proc: process id
    Returns: None
    """
    logger.info("killing process id  %s on %s" %(pid,remote_ip),also_console=True)
    cmd = "kill "+arg+ " "+pid
    logger.info("kill cmd %s" %(cmd),also_console=True)
    result =ssh_client.execute_command(cmd=cmd, hosts=remote_ip)[remote_ip]
    if result:
        return 1
    else:
        return 0


@keyword("Modify Bzproc Configs")
def modify_bzmanager_config(host, text, replace_to, procg_name='procg_bzflud', network='commercial'):
    """Modify config file bzmanager.xml
            Args:
                host       : machine ip to be used for command execution
                text       : text to be replaced
                replace_to : replacement text
                procg_name : name of procgrp
                network    : network that machines is a part of

            Returns:
                command output
    """
    if network == 'govddc':
        command = f'cd /a/share/govlogger/mdt/gov.ddcdir.bzproc.conf; cat bzmanager.xml | grep {procg_name}'
    else:
        command = f'cd /a/share/logger/mdt/ddcdir.bzproc.conf; cat bzmanager.xml | grep {procg_name}'
    output = run_cmd.remoteExecCommand(host, command)[host]
    output = output.split('"')
    spaces = 0
    for i in range(len(output[2])):
        if output[2][i] == ' ':
            spaces += 1
    separator = ' ' * spaces
    if network == 'govddc':
        modify_config = f'cd /a/share/govlogger/mdt/gov.ddcdir.bzproc.conf; sed -i "s/{procg_name}\\"{separator}enabled={text}/{procg_name}\\"{separator}enabled={replace_to}/g" bzmanager.xml'
    else:
        modify_config = f'cd /a/share/logger/mdt/ddcdir.bzproc.conf; sed -i "s/{procg_name}\\"{separator}enabled={text}/{procg_name}\\"{separator}enabled={replace_to}/g" bzmanager.xml'
    check_config = run_cmd.remoteExecCommand(host, modify_config)[host]
    return check_config


@keyword('Check Service')
def check_service(host, command, procg_name='procg_bzflud'):
    """Check if process related to given procgrp is not running
            Args:
                host       : machine ip to be used for command execution
                command    : command to be executed
                procg_name : name of procgrp

            Returns:
                True on success, False otherwise
    """
    output = run_cmd.remoteExecCommand(host, command)[host]
    logging.info(output)
    flag = True
    if f" {procg_name} " in output:
        flag = False
    return flag


@keyword("GET PROCGRP COUNT")
def get_procgrp_count(remote_ip):
    """
    Function to get count of procgrps running on QP
    Args: 1. remote_ip :  remote IP address
    Returns: Count
    """
    logger.info("Checking procgrp count on %s" %(remote_ip),also_console=True)
    cmd = "ps -ef | grep /a/bin/bzprocgrp | grep -v grep | wc -l"
    out = run_cmd.remoteExecCommand(remote_ip, cmd)[remote_ip]
    logger.info("Checking procgrp count %s" %(out),also_console=True)
    return int(out)


def ssh_run(server, command,also_console=True):
    """
    Description:
        Runs the specified command on the server using ssh

    Args:
        1. Machine IP
        2. Command

    Return:
        Returns the output of the command execution.
    """
    remote_command = "ssh -o \"StrictHostKeyChecking no\" -2A root@" + server + " " + command
    output = subprocess.Popen(remote_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read().strip()
    output.wait
    logger.info("\nMACHINE:  {}\nCOMMAND:  {}\nOUTPUT:\n{}".format(server, command, output.decode()), also_console=also_console)
    return output.decode().strip()


def check_status_code(also_console=True):
    """ Checks status code of bash command

               Args:

               Returns: status code

    """
    remote_command = "echo $?"
    output = subprocess.Popen(remote_command, shell=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT).stdout.read().strip()
    logger.info("\nOUTPUT:\n{}".format(output.decode()),
                also_console=also_console)
    return output.decode().strip()


@keyword("Get BZS from GHOST")
def get_bzs_from_ghost(ghostip):
    command = " netstat -anp|grep bzc|awk \'{print $5}\'|sed \'s/:.*//g\'|sort -u "
    output = ssh_run(ghostip,command)
    return output.split('\n')


@keyword("Get BZS IP From GHOST")
def get_bzs_ip_from_ghost(ghost_ip):
    """ Gets a BZS machine IP that specified ghost sends logs to from ghost

               Args:
                   ghost_ip    : ghost machine IP (str)

               Returns: BZS ip (str)

    """
    command = "ls /a/etc/bzc/work | grep ghost.ddc.log | head -1"
    file_name, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Ghost DDC log file name: " + file_name)
    if file_name == "" or "No such file or directory" in file_name:
        logging.info("Ghost DDC log file not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    command = "cat /a/etc/bzc/work/" + file_name[:-1]
    contents_output, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Ghost DDC log file content:\n" + contents_output)
    if contents_output == "" or "No such file or directory" in contents_output:
        logging.info("Ghost DDC log file is empty")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    bzs = contents_output.split()[-1]
    logging.info("BZS = " + bzs)
    return bzs


@keyword("Get BZSMOOSH IP From BZS")
def get_bzsmoosh_ip_from_bzs(bzs_ip):
    """ Gets a bzsmoosh machine IP from bzs

               Args:
                   bzs_ip    : bzs machine IP (str)

               Returns: bzsmoosh ip (str)

    """
    command = "netstat -anp | grep ESTABLISHED | grep 11377 | head -1"
    bzsmoosh_info, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("BZSMOOSH ip information: " + bzsmoosh_info)
    if bzsmoosh_info == "":
        logging.info("No BZSMOOSH found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    bzsmoosh_ip = bzsmoosh_info.split()[4].split(':')[0]
    return bzsmoosh_ip


@keyword("Get BZSMOOSH IP List From BZS")
def get_bzsmoosh_ip_list_from_bzs(bzs_ip, arl_id):
    """ Gets a bzsmoosh machine IPs from bzs

               Args:
                   bzs_ip    : bzs machine IP (str)
                   arl_id    : ARL ID (str)

               Returns: bzsmoosh ip list (List[str]) on success, False on failure

    """

    command = "cat /usr/local/akamai/etc/ddc_localization/state/current_locale"
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Current locale of bzs machine: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("No information found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    current_locale = output.rstrip()
    command = "cat /a/share/logger/mdt/ddcdir.bzplanner.shufflemap/bzflud." + current_locale + ".json"
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Shufflemap bzflud json file content: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("No information found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    output = output.rstrip().replace('\n', '').replace(' ', '').split("\"")
    dns = ''
    host_index = []
    if arl_id in output:
        for i in range(len(output)):
            if ":[" in output[i]:
                host_index.append(i - 1)
        host_index.reverse()
        arl_index = output.index(arl_id)
        for i in range(len(host_index)):
            if host_index[i] < arl_index:
                dns = output[host_index[i]]
                logging.info("Shard with ARL ID found, using shard dns: " + dns)
                break
    if dns == '':
        dns = output[output.index("default") + 2]
        logging.info("Shard with ARL ID not found, using default dns: " + dns)

    command = "host " + dns
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("List of bzsmoosh machines: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("No information found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    output = output.rstrip().split("\n")
    bzsmoosh_ip = []
    for i in range(len(output)):
        ip = output[i].split()[-1]
        if ip.replace('.', '').isnumeric():
            bzsmoosh_ip.append(ip)
    return bzsmoosh_ip


@keyword("GovDDC Get BZSMOOSH IP List From BZS")
def govddc_get_bzsmoosh_ip_list_from_bzs(bzs_ip):
    """ Gets a bzsmoosh machine IPs from bzs in GovDDC network

               Args:
                   bzs_ip    : bzs machine IP (str)

               Returns: bzsmoosh ip list (List[str]) on success, False on failure

    """
    command = "cat /a/share/govlogger/mdt/gov.ddcdir.bzproc.conf/ulfe.procg_bzflud.manifest.xml | grep host="
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Output: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("No information found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    output = output.rstrip().split("\"")
    dns = output[-2]
    command = "host " + dns
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("List of bzsmoosh machines: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("No information found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    output = output.rstrip().split("\n")
    bzsmoosh_ip = []
    for i in range(len(output)):
        ip = output[i].split()[-1]
        if ip.replace('.', '').isnumeric():
            bzsmoosh_ip.append(ip)
    return bzsmoosh_ip


@keyword("Get BZS IP And Log Information From GHOST")
def get_bzs_ip_log_information_from_ghost(ghost_ip):
    """ Gets a BZS machine IP that specified ghost sends logs to as well as epoch ID and epoch number from ghost
    perspective (extracted from log file name)

               Args:
                   ghost_ip    : ghost machine IP (str)

               Returns: BZS ip (str), Ghost epoch ID (str), Ghost epoch number (str)

    """
    command = "ls /a/etc/bzc/work | grep ghost.ddc.log | head -1"
    file_name, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Ghost DDC log file name: " + file_name)
    if file_name == "" or "No such file or directory" in file_name:
        logging.info("Ghost DDC log file not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    command = "cat /a/etc/bzc/work/" + file_name[:-1]
    contents_output, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Ghost DDC log file content:\n" + contents_output)
    if contents_output == "" or "No such file or directory" in contents_output:
        logging.info("Ghost DDC log file is empty")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    bzs = contents_output.split()[-1]
    epoch_id = file_name.split('_')[1]
    epoch_number = file_name.split('_')[2]
    logging.info("BZS = " + bzs)
    logging.info("Epoch ID = " + epoch_id)
    logging.info("Epoch number = " + epoch_number)
    return bzs, epoch_id, epoch_number


@keyword("Get Log Information From GHOST")
def get_log_information_from_ghost(ghost_ip):
    """ Gets a log information from GHOST machine

               Args:
                   ghost_ip    : ghost machine IP (str)

               Returns: Ghost epoch ID (str), Ghost epoch number (str), Ghost log size (str) on success, False on failure

    """
    command = "ls -ltr /a/logs/ghost.ddc.log.old | grep ghost.ddc.log | tail -1"
    log_info, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Latest Ghost DDC log file from ghost.ddc.log.old: " + log_info)
    if log_info == "" or "No such file or directory" in log_info:
        logging.info("Latest Ghost DDC log file from ghost.ddc.log.old not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    ghost_epoch_id = log_info.split()[-1].split(".")[3]
    ghost_epoch_number = log_info.split()[-1].split(".")[4]
    ghost_log_size = log_info.split()[4]
    logging.info("Ghost epoch ID = " + ghost_epoch_id)
    logging.info("Ghost epoch number = " + ghost_epoch_number)
    logging.info("Ghost log size = " + ghost_log_size)
    return ghost_epoch_id, ghost_epoch_number, ghost_log_size


@keyword("Get Log File Size From GHOST")
def get_log_file_size_from_ghost(ghost_ip):
    """ Gets a size of a log file from GHOST

               Args:
                   ghost_ip    : ghost machine IP (str)

               Returns: Ghost log size (str)

    """
    command = "ls -ltr /a/logs/ghost.ddc.log.old | grep ghost.ddc.log | tail -1"
    log_info, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Latest Ghost DDC log file from ghost.ddc.log.old: " + log_info)
    if log_info == "" or "No such file or directory" in log_info:
        logging.info("Latest Ghost DDC log file from ghost.ddc.log.old not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    log_info = log_info.split(' ')
    log_info = [i for i in log_info if i]
    ghost_log_size = log_info[4]
    logging.info("Ghost log size: " + ghost_log_size)
    return ghost_log_size


@keyword("Get Log Information From BZS")
def get_log_information_from_bzs(bzs_ip, ghost_ip, ghost_epoch_id, ghost_epoch_number):
    """ Gets a log information from BZS machine

               Args:
                   bzs_ip             : BZS machine IP (str)
                   ghost_ip           : ghost machine IP (str)
                   ghost_epoch_id     : epoch ID from Ghost (str)
                   ghost_epoch_number : epoch number from Ghost (str)

               Returns: BZS epoch ID (str), BZS epoch number (str), BZS log size (str) on success, False on failure

    """
    # command = "ls -ltr /pull/qlayer/archived/ffbak | tail -1"
    # last_daily_directory, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    # logging.info("Latest daily directory from BZS: " + last_daily_directory)
    # if last_daily_directory == "" or "No such file or directory" in last_daily_directory:
    #     logging.info("Latest daily directory from BZS not found")
    #     return False
    # if return_code != 0:
    #     logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
    #     return False
    # last_daily_directory = (last_daily_directory.split(' ')[-1])[:-1]
    # ghost_directory = ghost_ip.split('.')
    # ghost_directory = '.'.join(ghost_directory[:-1])
    # ghost_directory = ghost_directory + '.0'
    # command = ("ls /pull/qlayer/archived/ffbak/" + last_daily_directory + "/" + ghost_directory + " | grep "
    #            + ghost_epoch_id + "-" + ghost_epoch_number + " | head -1")
    # bzc_log, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    # logging.info("BZS log directory: " + bzc_log)
    # if bzc_log == "" or "No such file or directory" in bzc_log:
    #     logging.info("Log directory not found")
    #     return False
    # if return_code != 0:
    #     logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
    #     return False
    # bzc_log = bzc_log[:-1]
    command = (f"find /pull*/qlayer/archived/ffbak/ | grep {ghost_epoch_number} | grep {ghost_epoch_id} | "
               f"grep {ghost_ip} | grep headers.done$ | tail -1")
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Path to headers.done file: " + output)
    # command = ("cat /pull/qlayer/archived/ffbak/" + last_daily_directory + "/" + ghost_directory + "/" + bzc_log
    #            + "/headers.done")
    command = f"cat {output}"
    bzc_log_info, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("BZS log information: " + bzc_log_info)
    if bzc_log_info == "" or "No such file or directory" in bzc_log_info:
        logging.info("Log information file not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    bzc_log_info = bzc_log_info.split('\n')
    bzc_epoch_id, bzc_epoch_number, bzc_log_size = '', '', ''
    for i in bzc_log_info:
        if 'X-Akamai-Sequence' in i:
            bzc_epoch_id = i.split(' ')[2]
            bzc_epoch_number = i.split(' ')[3]
        elif 'X-Akamai-Local-Filesize' in i:
            bzc_log_size = i.split(' ')[1]
    if bzc_epoch_id and bzc_epoch_number and bzc_log_size:
        return bzc_epoch_id, bzc_epoch_number, bzc_log_size
    else:
        return False


@keyword("Rotate Logs Manually")
def rotate_logs_manually(ghost_ip):
    """ Rotates logs manually

               Args:
                   ghost_ip    : ghost machine IP (str)

               Returns: True on success, otherwise False

     """
    command = "/a/sbin/arrotate ghost.ddc.log"
    output, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Manual log rotation output: " + output)
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    return True


@keyword("Get Cpcode From Ghost Logs")
def get_cpcode_from_ghost_logs(ghost_ip):
    """ Gets cpcode from ghost.ddc.log file on ghost machine

               Args:
                   ghost_ip    : ghost machine IP (str)

               Returns: cpcode on success, False on failure

     """
    command = "zstdcat /a/logs/ghost.ddc.log.gz | grep \"^r\" | grep \" 200 \" | grep -v \"ERR\" | tail -10 |grep \"GET\" "
    output, return_code = run_cmd.remote_exec_command_code(ghost_ip, command)
    logging.info("Output: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("Specified request logs not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    output = output.rstrip().split('\n')
    for i in range(len(output)):
        # Skip lines that don't start with 'r ' (error messages, etc.)
        if not output[i].startswith('r '):
            continue
        temp = output[i].split(' ')
        if len(temp) > 11 and temp[10] == "GET" and temp[11].startswith('/'):
            cpcode = temp[11].split('/')[3]
            if cpcode.isnumeric():
                logging.info("cpcode found: " + cpcode)
                return cpcode
    logging.info("cpcode not found within logs")
    return False


@keyword("Get Summary File")
def get_summary_file(host, zm, ghost):
    output = ''
    for h in host:
        command = "\" ls -ltr /var/tmp/" + zm + "*txt |tail -2 |awk \'{print $NF}\'|while read a; do cat $a|grep " + ghost + ";done \""
        output += ssh_run(h, command) + '\n'
    return output.split('\n')


@keyword("Cross-check on GHOST")
def crosscheck_Ghost(ghostip, ddclog, zm):
    total_count=0
    correct=0
    incorrect=0
    logger.info("Checking on Ghost...",also_console=True)
    command = "\" ls -ltr /var/tmp/" + zm + "*txt |tail -1 |awk \'{print $NF}\'|while read a; do cat $a ;done \""
    output = ssh_run(ghostip,command,also_console=False)
    for x in ddclog:
          try:
             y = x.split()[0].split('-')
             rotid = y[2] + '-' + y[3] + '-' + y[4] + '-' + y[5] + '-' + y[6] + '.' + y[7]
             zcat = x.split()[1] + ' ' + x.split()[2] + ' ' + x.split()[3]
             found = 0
             offending = []
             for j in output.split('\n'):
                 if rotid in j:
                    total_count+=1
                    #if zcat in j:
                    if set(zcat.split()) == set(j.split()[-3:]):
                       #found = 1
                       correct+=1
                       logger.info(j+' OK',also_console=True)
                    else:
                       incorrect+=1
                       logger.info(j+' != '+zcat,also_console=True)
                       offending.append(set(zcat.split())+'!='+set(j.split()[-3:]))
                       #found = 0

             print(offending)
             #assert found == 1
          except:
             pass
    if(correct >= 1):
        flag = True
    else:
        flag = False
    return [rotid, flag]


@keyword("Run DDCProcessLog")
def run_process_log(qp,donefiles):
    #qp = qp[0]
    if len(qp[0]) > 1:
        qp = qp[0]
    for donefile in donefiles:
        if len(donefile) == 0:
           continue
        donef = str(donefile.split()[0])
        cmd = 'rm -rf /tmp/r1 2>/dev/null; sed -i \'/rjavier_8724/d\' ~logger/conf/spinner.cp_codes; echo "8724  rjavier_8724  2021.04.30  06:44" >> ~logger/conf/spinner.cp_codes ; su - logger -c " process-log.pl -b -l {} -q -o -R /tmp/r1; find /tmp/r1 -type f |while read a; do cat \$a|b16todecimal > /tmp/p.out ; if [[ -s /tmp/p.out ]] ; then (echo \\"*** \$a ****\\";cat /tmp/p.out|tail -1);fi; done"'.format(donef)
        logger.info("\nMACHINE:  {}\nCOMMAND: {}".format(qp, cmd), also_console=True)
        #output = run_cmd.remoteExecCommand(qp,cmd)[qp].decode().strip()
        output = run_cmd.remoteExecCommand(qp,cmd)[qp].strip()
        logger.info("OUTPUT:\n{}".format(output), also_console=True)
        return output


@keyword("Check Processed LGA")
def check_for_LGA(qp,cpcode,rotid):
    if len(qp[0]) > 1:
        qp = qp[0]
    r = rotid.split('.')[0]
    cmd = 'echo "1849   rjavier_1849   2021.04.30  06:44" >> ~logger/conf/spinner.cp_codes ; su - logger -c "cd /ghostcache/spin3-archive/tmp;  lgatool -l *' + r + '*lga.tmp"'   # ; rm /ghostcache/spin-archive/tmp/*"'
    #output = run_cmd.remoteExecCommand(qp,cmd)[qp].decode().strip()
    output = run_cmd.remoteExecCommand(qp,cmd)[qp].strip()
    #logger.info("OUTPUT:\n{}".format(output), also_console=True)
    logger.info("\nMACHINE:  {}\nCOMMAND:  {}\nOUTPUT:\n{}".format(qp, cmd, output), also_console=True)
    if(cpcode in output):
        return True
    else:
        return False
    #return output


@keyword("Get Last E2E Processed")
def get_last_e2e_processed(das_ip):
    """ Gets last e2e processed

               Args:
                   das_ip : das machine IP (str)

               Returns: last e2e processed on success (List[Str]), False on failure

    """
    r = 0
    retry = 0
    while True:
        cmd = 'cat /var/tmp/last_rotid_processed.lst; echo $?'
        output, return_code = run_cmd.remote_exec_command_code(das_ip, cmd)
        logging.info("Output: " + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Data on DAS machine not found")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        r = random.randrange(len(output.split('\n')) - 1)
        rotid = output.split('\n')[r].split(' ')[0]
        seqid = output.split('\n')[r].split(' ')[2]
        # cmd = '\'find /ghostcache9/ghostcache/das/htp_stat_5mi -type f -mtime 0 2>/dev/null|xargs zgrep -a GHOST|grep -a ' + rotid +'|grep -a \"SEQ '+seqid+'\" |head -1\''
        cmd = '\'find /ghostcache9/ghostcache/das/htp_stat_5mi -type f 2>/dev/null|xargs zgrep -a GHOST|grep -a ' + rotid + '|grep -a \"SEQ ' + seqid + ' \" |head -1; echo $?\''
        output, return_code = run_cmd.remote_exec_command_code(das_ip, cmd)
        logging.info("Output: " + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Data not found")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        output = '\n'.join(output.split('\n')[:-1])
        if len(output) > 0:
            break
        retry += 1
        if retry >= 10:
            return False
    return output.split(' ')


@keyword("Run Log_filter")
def check_Log_filter(qp,cpcode,donefiles):
    if len(qp[0]) > 1:
        qp = qp[0]
    for donefile in donefiles:
        if len(donefile.split()[0]) > 1:
           donef = str(donefile.split()[0])
        else:
           donef = donefiles
        cmd = '\'cd {} ; ls *_*done|sort |head -1|tee /tmp/crap |xargs zgrep \"^r \"|head -1|cut -d\" \" -f2|cut -d\".\" -f1\''.format(donef)
        r1 = ssh_run(qp,cmd,also_console=True)
        cmd = '\'cd {} ; ls *_*done|sort |head -1|tee /tmp/crap2 |xargs zgrep \"^r \"|tail -1|cut -d\" \" -f2|cut -d\".\" -f1\''.format(donef)
        r2 = ssh_run(qp,cmd,also_console=True)
        cmd = 'su - logger -c "cat {}/*_*done | log_filter -c /a/etc/netmgmt.log_filter.conf -f ddc --prepend_ip --stdin_filename {} \\"unix_date >= {} and unix_date <= {} and cpcode in ({})\\"|tail"'.format(donef,qp,r1,r2,cpcode)
        logger.info("\nMACHINE:  {}\nCOMMAND: {}".format(qp, cmd), also_console=True)
        #output = run_cmd.remoteExecCommand(qp,cmd)[qp].decode().strip()
        output = run_cmd.remoteExecCommand(qp,cmd)[qp].strip()
        logger.info("OUTPUT:\n{}".format(output), also_console=True)
        return output


@keyword("Log Filter Check")
def Log_filter_check(bzs_ip, cpcode, donefiles, user):
    """ Gets a log information from BZS machine

               Args:
                   bzs_ip    : BZS machine IP (str)
                   cpcode    : cpcode to be checked with log_filter (str)
                   donefiles : list of .done log files from bzs (last 2 days) (str)
                   user      : user used for log filter (str)

               Returns: True on successful log verification, False on failure

    """
    command = 'cd {} ; ls *_*done|sort |head -1|tee /tmp/crap |xargs zstdcat | grep \"^r \"|head -1|cut -d\" \" -f2|cut -d\".\" -f1'.format(donefiles[-1])
    r1, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    if "terminated by signal 13" in r1:
        r1 = r1.split()[0]
    r1 = r1.rstrip()
    print("r1 ="+r1)
    logging.info("First timestamp from log file: " + r1)
    if r1 == "" or "No such file or directory" in r1:
        logging.info("Log not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    command = 'cd {} ; ls *_*done|sort |head -1|tee /tmp/crap2 |xargs zstdcat | grep \"^r \"|tail -1|cut -d\" \" -f2|cut -d\".\" -f1'.format(donefiles[-1])
    r2, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    if "terminated by signal 13" in r2:
        r2 = r2.split()[0]
    r2 = r2.rstrip()
    logging.info("Last timestamp from log file: " + r2)
    if r2 == "" or "No such file or directory" in r2:
        logging.info("Log not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    command = 'su - {} -c "cat {}/*_*done | log_filter -c /a/etc/netmgmt.log_filter.conf -f ddc --prepend_ip --stdin_filename {} \\"unix_date >= {} and unix_date <= {} and cpcode in ({})\\"|tail"'.format(user, donefiles[-1], bzs_ip, r1, r2, cpcode)
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Output: " + output)
    if output == "" or "No such file or directory" in output or "command not found" in output:
        logging.info("Logs not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    command = "rm /tmp/crap; rm /tmp/crap2"
    output, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    return True


@keyword("Get Done Files From BZS")
def get_done_files_from_bzs(bzs_ip, ghost_ip):
    """ Gets a list of .done log file names from BZS machine from last two days

               Args:
                   bzs_ip             : BZS machine IP (str)
                   ghost_ip           : ghost machine IP (str)

               Returns: list of done log file names (List[str])

    """
    command = "ls -1 /pull/qlayer/archived/ffbak | sort -n | tail -2"
    last_daily_directories, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Two latest daily directory from BZS: " + last_daily_directories)
    if last_daily_directories == "" or "No such file or directory" in last_daily_directories:
        logging.info("Latest daily directory from BZS not found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    last_daily_directories = last_daily_directories.split('\n')
    yesterday_directory = last_daily_directories[0].strip()
    today_directory = last_daily_directories[1].strip()
    ghost_directory = ghost_ip.split('.')
    ghost_directory = '.'.join(ghost_directory[:-1])
    ghost_directory = ghost_directory + '.0'
    command = ("ls /pull/qlayer/archived/ffbak/" + yesterday_directory + "/" + ghost_directory + " | grep "
               + ghost_ip + " | grep -v progress")
    yesterday_files, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Yesterdays's .done log files:\n" + yesterday_files)
    if "No such file or directory" in yesterday_directory:
        logging.info("Yesterday's daily directory from BZS not found")
        return False
    yesterday_files = yesterday_files.split('\n')
    for i in range(len(yesterday_files)):
        yesterday_files[i] = "/pull/qlayer/archived/ffbak/" + yesterday_directory + "/" + ghost_directory + "/" + yesterday_files[i]
    command = ("ls /pull/qlayer/archived/ffbak/" + today_directory + "/" + ghost_directory + " | grep "
               + ghost_ip + " | grep -v progress")
    today_files, return_code = run_cmd.remote_exec_command_code(bzs_ip, command)
    logging.info("Today's .done log files:\n" + today_files)
    if "No such file or directory" in today_directory:
        logging.info("Today's daily directory from BZS not found")
        return False
    today_files = today_files.split('\n')
    for i in range(len(today_files)):
        today_files[i] = "/pull/qlayer/archived/ffbak/" + today_directory + "/" + ghost_directory + "/" + today_files[i]
    done_files = []
    for i in yesterday_files:
        if "ghost.ddc.log" in i:
            done_files.append(i)
    for i in today_files:
        if "ghost.ddc.log" in i:
            done_files.append(i)
    logging.info(".done log files:\n" + str(done_files))
    if not done_files:
        logging.info(".done log files from BZS not found")
        return False
    return done_files


@keyword("RUN THE COMMAND")
def run_the_command(host, command):
    output =run_cmd.remoteExecCommand(host, command)[host]
    return output.strip()


@keyword('DISABLE ENABLE PROCG BZFLUD')
def modify_bzmanager_xml(host, text, replaceTo):
    modify_command = "cd /a/share/logger/mdt/ddcdir.bzproc.conf/; sed -i 's/"+text+"/"+replaceTo+"/g' bzmanager.xml"
    check_output1 = run_cmd.remoteExecCommand(host, modify_command)[host]
    verify_change = "cd /a/share/logger/mdt/ddcdir.bzproc.conf/; cat bzmanager.xml ;"
    check_output2 = run_cmd.remoteExecCommand(host, verify_change)[host]
    return check_output2


@keyword("COPY JSON FILE")
def copy_json_file(host, source, destination):
    """ Creates a copy a file on specified machine

           Args:
               host        : machine IP (str)
               source      : source path (str)
               destination : destination path (str)

           Returns: True on success, False on failure

    """
    copy_cmd= "cp {} {}".format(source,destination)
    backup_json_file =run_cmd.remoteExecCommand(host, copy_cmd)[host]
    if backup_json_file != "":
        return False
    else:
        return True


@keyword("COPY JSON FILES")
def copy_json_files(hosts, source, destination):
    """ Creates a copy a file on specified machines

           Args:
               hosts        : machines IPs (List[str])
               source      : source path (str)
               destination : destination path (str)

           Returns: True on success, False on failure

    """
    for i in hosts:
        copy_cmd = "cp {} {}".format(source, destination)
        backup_json_file = run_cmd.remoteExecCommand(i, copy_cmd)[i]
        if backup_json_file != "":
            return False
    return True


@keyword('Remove Backup File')
def remove_backup_file(host, backup_file):
    """ Removes backup files on specified machine

           Args:
               host        : machine IP (str)
               backup_file : backup_file to be removed (str)

           Returns:

    """
    remove_command = "cd" +" ;" + "rm {}".format(backup_file)
    remove_backup_file = run_cmd.remoteExecCommand(host, remove_command)[host]


@keyword('Remove Backup Files')
def remove_backup_files(hosts, backup_file):
    """ Removes backup files on specified machines

           Args:
               hosts       : machine IPs (List[str])
               backup_file : backup_file to be removed (str)

           Returns:

    """
    for i in hosts:
        remove_command = "cd" +" ;" + "rm {}".format(backup_file)
        remove_backup_file = run_cmd.remoteExecCommand(i, remove_command)[i]


@keyword('Modify Fields in JSON File')
def modify_Json_fields(host, stream_id):
    """ Modify json file of specified stream on specified machine

               Args:
                   host      : machine IP (str)
                   stream_id : ID of stream (str)

               Returns: False on failure

    """
    command='cat /a/share/logger/mdt/ddcdir.bzflud.streams.conf/{}.json'.format(stream_id)
    json_file =run_cmd.remoteExecCommand(host, command)[host]
    json_definition= json.loads(json_file)
    json_definition["data-set"]["file-prefix"]="arlid200058021test"
    json_definition["data-set"]["file-suffix"]="13057test"
    json_definition["data-set"]["opted-fields"]= [1002, 1100, 1005, 1006, 1008, 1009, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1101, 1019, 1023, 1031, 1032, 1037, 1033, 1066, 1068, 1102, 1103]
    json_definition=json.dumps(json_definition)
    echo_command= "echo '{}' | jq '.' > /a/share/logger/mdt/ddcdir.bzflud.streams.conf/{}.json".format(json_definition, stream_id)
    update_json_file =ssh_client.execute_command(cmd=echo_command, hosts=host)[host]
    modified_json_file=run_cmd.remoteExecCommand(host, 'cat /a/share/logger/mdt/ddcdir.bzflud.streams.conf/{}.json'.format(stream_id))[host]
    if "arlid200058021test" and "13057test" not in modified_json_file:
        return False


@keyword('Modify Fields in JSON Files')
def modify_Json_fields_in_files(hosts,stream_id):
    """ Modify json file of specified stream on specified machines

           Args:
               hosts     : machine IPs (List[str])
               stream_id : ID of stream (str)

           Returns: False on failure

    """
    for i in hosts:
        command='cat /a/bzflud/metadata/ddcdir.bzflud.streams.conf/{}.json'.format(stream_id)
        json_file =run_cmd.remoteExecCommand(i, command)[i]
        json_definition= json.loads(json_file)
        json_definition["data-set"]["file-prefix"]="arlid200058021test"
        json_definition["data-set"]["file-suffix"]="13057test"
        json_definition["data-set"]["opted-fields"]= [1002, 1100, 1005, 1006, 1008, 1009, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1101, 1019, 1023, 1031, 1032, 1037, 1033, 1066, 1068, 1102, 1103]
        json_definition=json.dumps(json_definition)
        echo_command= "echo '{}' | jq '.' > /a/bzflud/metadata/ddcdir.bzflud.streams.conf/{}.json".format(json_definition, stream_id)
        update_json_file =ssh_client.execute_command(cmd=echo_command, hosts=i)[i]
        modified_json_file=run_cmd.remoteExecCommand(i, 'cat /a/bzflud/metadata/ddcdir.bzflud.streams.conf/{}.json'.format(stream_id))[i]
        if "arlid200058021test" and "13057test" not in modified_json_file:
            return False


@keyword('GovDDC Modify Fields in JSON Files')
def govddc_modify_Json_fields_in_files(hosts,stream_id):
    """ Modify json file of specified stream on specified machines in GovDDC network

           Args:
               hosts     : machine IPs (List[str])
               stream_id : ID of stream (str)

           Returns: False on failure

    """
    for i in hosts:
        command='cat /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/{}.json'.format(stream_id)
        json_file =run_cmd.remoteExecCommand(i, command)[i]
        json_definition= json.loads(json_file)
        json_definition["data-set"]["file-prefix"]="arlid200058021test"
        json_definition["data-set"]["file-suffix"]="13057test"
        json_definition["data-set"]["opted-fields"]= [1002, 1100, 1005, 1006, 1008, 1009, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1101, 1019, 1023, 1031, 1032, 1037, 1033, 1066, 1068, 1102, 1103]
        json_definition=json.dumps(json_definition)
        echo_command= "echo '{}' | jq '.' > /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/{}.json".format(json_definition, stream_id)
        update_json_file =ssh_client.execute_command(cmd=echo_command, hosts=i)[i]
        modified_json_file=run_cmd.remoteExecCommand(i, 'cat /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/{}.json'.format(stream_id))[i]
        if "arlid200058021test" and "13057test" not in modified_json_file:
            return False


@keyword('Run Curl command for Json Files')
def run_curl_commands_for_json_files(hosts, stream_id):
    """ Run curl command to refresh stream config file on specified machines

           Args:
               hosts     : machine IPs (List[str])
               stream_id : ID of stream (str)

           Returns: True on success, False on failure

    """
    for i in hosts:
        command = ('cp /a/bzflud/metadata/ddcdir.bzflud.streams.conf/{}.json '
                   '/usr/local/akamai/bzflud/metadata/updates/ddcdir.bzflud.streams.conf/{}.json').format(
            stream_id, stream_id)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Copy json file: \n" + output)
        if output != "":
            logging.info("Copy operation failed")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = ('cp /a/bzflud/metadata/ddcdir.bzflud.streams.conf/ds-arlindex.json '
                   '/usr/local/akamai/bzflud/metadata/updates/ddcdir.bzflud.streams.conf/ds-arlindex.json')
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Copy arlindex file: \n" + output)
        if output != "":
            logging.info("Copy operation failed")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = 'sha256sum /a/share/logger/mdt/ddcdir.bzflud.streams.conf/{}.json'.format(stream_id)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("No information found")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        curl_input = output.split()[0]
        command = 'sha256sum /a/share/logger/mdt/ddcdir.bzflud.streams.conf/ds-arlindex.json'
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("No information found")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        curl_input_arl = output.split()[0]
        command = ('curl -v -H "X-Akamai-Update-File-Sha-256:{}" -X AKAMAI_META_UPDATE '
                   '0.0.0.0:9487/7/4/550/ddcdir.bzflud.streams.conf/stager.akamai.com/usr/local/akamai/bzflud'
                   '/metadata/updates/ddcdir.bzflud.streams.conf/{}.json').format(
            curl_input, stream_id)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Curl command output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Curl operation failed")
            return False
        elif "200 OK" not in output:
            logging.info("Curl operation did not return 200 OK code")
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = ('curl -v -H "X-Akamai-Update-File-Sha-256:{}" -X AKAMAI_META_UPDATE '
                   '0.0.0.0:9487/7/4/550/ddcdir.bzflud.streams.conf/stager.akamai.com/usr/local/akamai/bzflud'
                   '/metadata/updates/ddcdir.bzflud.streams.conf/ds-arlindex.json').format(
            curl_input_arl)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Curl command output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Copy operation failed")
            return False
        elif "200 OK" not in output:
            logging.info("Curl operation did not return 200 OK code")
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = ('curl -v -X AKAMAI_META_UPDATE 0.0.0.0:9487/7/4/550/DONE/stager.akamai.com/usr/local/akamai/bzflud'
                   '/metadata/updates/ddcdir.bzflud.streams.conf')
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Curl command output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Copy operation failed")
            return False
        elif "200 OK" not in output:
            logging.info("Curl operation did not return 200 OK code")
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
    return True


@keyword('GovDDC Run Curl command for Json Files')
def govddc_run_curl_commands_for_json_files(hosts, stream_id):
    """ Run curl command to refresh stream config file on specified machines in GovDDC network

           Args:
               hosts     : machine IPs (List[str])
               stream_id : ID of stream (str)

           Returns: True on success, False on failure

    """
    for i in hosts:
        command = ('cp /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/{}.json '
                   '/usr/local/akamai/share/govlogger/mdt/updates/gov.ddcdir.bzflud.streams.conf/{}.json').format(
            stream_id, stream_id)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Copy json file: \n" + output)
        if output != "":
            logging.info("Copy operation failed")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = ('cp /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/ds-arlindex.json '
                   '/usr/local/akamai/share/govlogger/mdt/updates/gov.ddcdir.bzflud.streams.conf/ds-arlindex.json')
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Copy arlindex file: \n" + output)
        if output != "":
            logging.info("Copy operation failed")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = 'sha256sum /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/{}.json'.format(stream_id)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("No information found")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        curl_input = output.split()[0]
        command = 'sha256sum /a/share/govlogger/mdt/gov.ddcdir.bzflud.streams.conf/ds-arlindex.json'
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("No information found")
            return False
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        curl_input_arl = output.split()[0]
        command = ('curl -v -H "X-Akamai-Update-File-Sha-256:{}" -X AKAMAI_META_UPDATE '
                   '0.0.0.0:9487/7/4/550/gov.ddcdir.bzflud.streams.conf/stager.akamai.com/usr/local/akamai/share/'
                   'govlogger/mdt/updates/gov.ddcdir.bzflud.streams.conf/{}.json').format(curl_input, stream_id)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Curl command output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Curl operation failed")
            return False
        elif "200 OK" not in output:
            logging.info("Curl operation did not return 200 OK code")
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = ('curl -v -H "X-Akamai-Update-File-Sha-256:{}" -X AKAMAI_META_UPDATE '
                   '0.0.0.0:9487/7/4/550/gov.ddcdir.bzflud.streams.conf/stager.akamai.com/usr/local/akamai/share/'
                   'govlogger/mdt/updates/gov.ddcdir.bzflud.streams.conf/ds-arlindex.json').format(curl_input_arl)
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Curl command output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Copy operation failed")
            return False
        elif "200 OK" not in output:
            logging.info("Curl operation did not return 200 OK code")
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
        command = ('curl -v -X AKAMAI_META_UPDATE 0.0.0.0:9487/7/4/550/DONE/stager.akamai.com/usr/local/akamai/share/'
                   'govlogger/mdt/updates/gov.ddcdir.bzflud.streams.conf')
        output, return_code = run_cmd.remote_exec_command_code(i, command)
        logging.info("Curl command output: \n" + output)
        if output == "" or "No such file or directory" in output:
            logging.info("Copy operation failed")
            return False
        elif "200 OK" not in output:
            logging.info("Curl operation did not return 200 OK code")
        if return_code != 0:
            logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
            return False
    return True


@keyword('Get Arlid from Hostname')
def get_arlid_from_hostname(host, arl_hostname):
    """ Get Arl ID base on Arl hostname from ghost machine

           Args:
               host         : ghost machine IP (str)
               arl_hostname : Aarl hostname (str)

           Returns: Arl ID on success, False on failure

    """
    command = 'grep -B 10 "{}" /a/etc/ghost/arl.data/arlindex.xml'.format(arl_hostname)
    output, return_code = run_cmd.remote_exec_command_code(host, command)
    logging.info("Output: \n" + output)
    if output == "" or "No such file or directory" in output:
        logging.info("No information found on arl found")
        return False
    if return_code != 0:
        logging.info(("Bash return code: " + str(return_code) + " is different than 0 - operation failed"))
        return False
    output = output.split("\n")
    output.reverse()
    for i in range(1, len(output)):
        if "<akamai:include" in output[i]:
            arlid = output[i].split()[1].replace('"', '').split("=")[-1]
            return arlid
    return False


@keyword("VERIFY UPLOAD VALUES")
def verify_upload_values(host, command, value):
    info_exists =run_cmd.remoteExecCommand(host, command)[host]
    logger.debug("schema value is " +info_exists)
    if value in info_exists:
        logging.debug("PASS")
        return '1'
    else:
        return '0'


@keyword('Disable Stream_id')
def disable_field(host,stream_id):
    disable_command= 'cd /a/share/logger/mdt/ddcdir.bzflud.sqa2_streams.conf; jq "\'"."enabled" = false"\'" {}.json > temp && mv temp {}.json'.format(stream_id,stream_id)
    command_output =run_cmd.remoteExecCommand(host, disable_command)[host]
    if command_output!='':
        return False
    return True


@keyword('CHECK DISK SPACE')
def check_disk_space(host):
    """ Check whether increased load decreases over time

           Args:
               host : machine IP (str)

           Returns: True on success, False on failure

    """
    command = 'cd /a/logs ; df -h .;'
    output = run_cmd.remoteExecCommand(host,command)[host]
    logging.info(output)
    available_space = int(float(output.split()[10][:-1]))
    logging.info("Available space : {}".format(available_space))
    if available_space > 2:
        allocate_space = str(available_space - 2) + 'G'
        logging.info("Allocate space : {}".format(allocate_space))
        command = 'cd /a/logs ; fallocate -xl ' + allocate_space + ' ddc_sqa_test; df -h . ;'
        output = run_cmd.remoteExecCommand(host, command)[host]

    status, result, retry, i = True, True, 0, 1
    while status:
        command = 'cd /a/logs; df -h . ;'
        output = run_cmd.remoteExecCommand(host, command)[host]
        use_percentage = int(output.split()[11][:-1])
        logging.info("Use Percentage : {}".format(use_percentage))
        if use_percentage > 92 and use_percentage <= 98:
            command = 'ls -lth /a/logs/ghost.ddc.log.old/|wc -l;date; cd /a/logs/; df -h .'
            output = run_cmd.remoteExecCommand(host, command)[host]
            max_number_of_files = int(output.split()[0])
            logging.info("Max number of files: {}".format(max_number_of_files))
            time.sleep(30)
            status = False
        elif use_percentage > 98:
            command = 'cd /a/logs ; rm ddc_sqa_test; fallocate -xl ' + str((int(allocate_space[:-1]) - 0.5)) + 'G ddc_sqa_test ;  '
            output = run_cmd.remoteExecCommand(host, command)[host]
        else:
            if i > 5:
                logging.info("Fallocate command do not raise Use% efficiently")
                logging.info("Failed")
                command = 'cd /a/logs ; rm ddc_sqa_tes* ;'
                output = run_cmd.remoteExecCommand(host, command)[host]
                return False
            command = 'cd /a/logs ; fallocate -xl 0.5G ddc_sqa_test_' + str(i) +' ;'
            output = run_cmd.remoteExecCommand(host, command)[host]
            i = i + 1

    #command = 'tc qdisc replace dev eth0 root handle 1: prio priomap 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2; tc filter add dev eth0 parent 1:0 protocol ip prio 1 u32 flowid 1:1 match ip dport 9483 0xffff; tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 300ms;'
    #output = run_cmd.remoteExecCommand(host, command)[host]
    while result and retry <= 5:
        logging.info("Result : {0} , Retry count : {1}".format(result,retry))
        #command = 'ls -lth /a/logs/ghost.ddc.log.old/|wc -l;date;cd /a/logs/ ; df -h .'
        command = 'cd /a/logs; df -h . ;'
        output = run_cmd.remoteExecCommand(host,command)[host]
        #disk_space_percentage = int(output.split()[0])
        disk_space_percentage = int(output.split()[11][:-1])
        logging.info("Disk Space Percentage: {}".format(disk_space_percentage))
        #logging.info("Comparison result : ")
        #logging.info(int(max_number_of_files) > int(min_number_of_files))
        #if int(max_number_of_files) > int(min_number_of_files):
        if int(disk_space_percentage) <= 92:
            logging.info("Number of files is reduced. Akarotate is working as expected.")
            result = False
        else:
            logging.info("Retrying after 30 secs")
            time.sleep(30)
            retry = retry + 1

    if result is False:
        logging.info("Passed")
        command = 'cd /a/logs ; rm ddc_sqa_tes* ;'
        output = run_cmd.remoteExecCommand(host,command)[host]
        return True
    else:
        logging.info("Failed")
        command = 'cd /a/logs ; rm ddc_sqa_tes* ;'
        output = run_cmd.remoteExecCommand(host,command)[host]
        return False


@keyword('DD ALLOCATION CHECK DISK SPACE')
def dd_allocation_check_disk_space(host):
    """ Check whether load increased  with dd command decreases over time

           Args:
               host : machine IP (str)

           Returns: True on success, False on failure

    """
    command = 'cd /a/logs ; df -h .;'
    output = run_cmd.remoteExecCommand(host, command)[host]
    logging.info(output)
    available_space = int(float(output.split()[10][:-1]))
    logging.info("Available space : {}".format(available_space))
    if available_space > 2:
        allocate_space = str((available_space - 2) * 1024)
        logging.info("Allocate space : {}G".format(str(available_space - 2)))
        command = 'cd /a/logs ; dd if=/dev/zero of=ddc_sqa_test bs=1MB count=' + allocate_space
        output = run_cmd.remoteExecCommand(host, command)[host]

    status, result, retry, i = True, True, 0, 1
    while status:
        command = 'cd /a/logs; df -h . ;'
        output = run_cmd.remoteExecCommand(host, command)[host]
        use_percentage = int(output.split()[11][:-1])
        logging.info("Use Percentage : {}".format(use_percentage))
        if 92 < use_percentage <= 98:
            command = 'ls -lth /a/logs/ghost.ddc.log.old/|wc -l;date; cd /a/logs/; df -h .'
            output = run_cmd.remoteExecCommand(host, command)[host]
            max_number_of_files = int(output.split()[0])
            logging.info("Max number of files: {}".format(max_number_of_files))
            time.sleep(30)
            status = False
        elif use_percentage > 98:
            command = 'cd /a/logs; rm ddc_sqa_test; dd if=/dev/zero of=ddc_sqa_test bs=1MB count=' + str((int(allocate_space) - 512))
            output = run_cmd.remoteExecCommand(host, command)[host]
        else:
            if i > 5:
                logging.info("Fallocate command do not raise Use% efficiently")
                logging.info("Failed")
                command = 'cd /a/logs ; rm ddc_sqa_tes* ;'
                output = run_cmd.remoteExecCommand(host, command)[host]
                return False
            command = 'cd /a/logs; dd if=/dev/zero of=ddc_sqa_test_' + str(i) + ' bs=1MB count=512'
            output = run_cmd.remoteExecCommand(host, command)[host]
            i = i + 1

    while result and retry <= 5:
        logging.info("Result : {0} , Retry count : {1}".format(result,retry))
        command = 'cd /a/logs; df -h . ;'
        output = run_cmd.remoteExecCommand(host,command)[host]
        disk_space_percentage = int(output.split()[11][:-1])
        logging.info("Disk Space Percentage: {}".format(disk_space_percentage))
        if int(disk_space_percentage) <= 92:
            logging.info("Number of files is reduced. Akarotate is working as expected.")
            result = False
        else:
            logging.info("Retrying after 30 secs")
            time.sleep(30)
            retry = retry + 1

    if result is False:
        logging.info("Passed")
        command = 'cd /a/logs ; rm ddc_sqa_tes* ;'
        output = run_cmd.remoteExecCommand(host,command)[host]
        return True
    else:
        logging.info("Failed")
        command = 'cd /a/logs ; rm ddc_sqa_tes* ;'
        output = run_cmd.remoteExecCommand(host,command)[host]
        return False


@keyword('VERIFY DS2 LOG MESSAGES')
def verify_ds2_log_messages(host, streamid, ds2_enabled):
    msg_format = "Failed to upload bundle for bzflud/"+streamid
    ds2_enabled_attempts = 0
    if ds2_enabled == True:
        ds2_enabled_attempts = 14
    else:
        ds2_enabled_attempts = 10

    msg_format_ds2_retry = ("\"" + msg_format + " attempt: .*\/" + str(ds2_enabled_attempts) +
                           " duration: .* retryMaxDuration: .* ttlMinutes: .*" + "\"")

    command = "grep " + msg_format_ds2_retry + " /a/logs/bzflud/bzflud.upload.log"
    logging.info(command)
    output = run_cmd.remoteExecCommand(host, command)[host]
    if msg_format in output:
        return True
    else:
        return False


@keyword('GOVDDC VERIFY DS2 LOG MESSAGES')
def govddc_verify_ds2_log_messages(host, streamid, ds2_enabled):
    msg_format = "Failed to upload bundle for gov_bzflud/"+streamid
    if ds2_enabled:
        ds2_enabled_attempts = 14
    else:
        ds2_enabled_attempts = 10

    msg_format_ds2_retry = ("\"" + msg_format + " attempt: .*\/" + str(ds2_enabled_attempts) +
                           " duration: .* retryMaxDuration: .* ttlMinutes: .*" + "\"")

    command = "grep " + msg_format_ds2_retry + " /a/logs/bzflud/bzflud.upload.log"
    logging.info(command)
    output = run_cmd.remoteExecCommand(host, command)[host]
    if msg_format in output:
        return True
    else:
        return False

@keyword("QUERY BZFLUD ERRORS TABLE FOR PIPELINE")
def query_bzflud_errors_table_for_pipeline(host, aggregator, pipelinename, errorcode, errorcodeflag=True) -> pd:
    """ Query bzflud errors table for a given pipeline

               Args:
                   host : machine IP (str)
                   aggregator : SQA aggregator to use
                   pipelinename : name of the CSI breeze pipeline
                   errorcode : errorcode to match with
                   errorcodeflag :  to match for the presence of absence of the errorcode
                                    True:presence of errorcode, False:absense of errorcode

               Returns: True on success, False otherwise

    """
    if errorcodeflag == True:
        errorcheck = "errcode = "
    else:
        errorcheck = "errorcode != "
    sql_query = f"\'SELECT * FROM bzflud_errors_5mi where {errorcheck} {errorcode} \'"
    table = f"bzflud_errors_5mi.csv"
    command = f"ssh root@{host} \"/a/bin/sql2 --csv -q {aggregator} {sql_query}\" > {table}"
    pipe = subprocess.Popen(command.encode(), shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    pipe.wait(20)
    pd_tb = pd.read_csv(table)
    return "pipeline" in pd_tb.columns and pipelinename in pd_tb['pipeline'].values

@keyword("QUERY BANZAI SERVER TABLE FOR SOURCEIP")
def query_banzai_server_table_for_sourceip(host, aggregator, sourceip) -> pd:
    """ Query banzai server table for any source IP

               Args:
                   host : machine IP (str)
                   aggregator : SQA aggregator to use

               Returns: True on success, False otherwise

    """
    sql_query = f"\'SELECT * FROM banzai_srvr_xfer_sum_1hr_v2 where logname like \\\"%cust%\\\";\'"
    table = f"banzai_srvr_xfer_sum_1hr_v2.csv"
    command = f"ssh root@{host} \"/a/bin/sql2 --csv -q {aggregator} {sql_query}\" > {table}"
    pipe = subprocess.Popen(command.encode(), shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    pipe.wait(20)
    pd_tb = pd.read_csv(table)
    print(pd_tb)
    return "source_ip" in pd_tb.columns and sourceip in pd_tb['source_ip'].values

@keyword("TABLE FETCH BZC METRICS IN SOURCE IP")
def table_fetch_bzc_metrics_in_sourceip(sourceip) -> pd:
    """ Query banzai client table for given source IP

               Args:
                   sourceip : machine IP (str)

               Returns: True on success, False otherwise

    """
    table = f"bz_client_metrics_counts.csv"
    command = f"ssh root@{sourceip} \"/a/bin/tbl-fetch -cuq 0 tableprov bz_clnt_metric_counts\" > {table}"
    pipe = subprocess.Popen(command.encode(), shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    pipe.wait(20)
    pd_tb = pd.read_csv(table, sep='\s+', skiprows=[1])

    pd_new = pd_tb[(pd_tb['num_bad'] == 0) & (pd_tb['num_read'] > 0) & (pd_tb['num_flight'] > 0) & (pd_tb['num_done'] > 0) & (pd_tb['num_read'] == pd_tb['num_flight'])]

    return pd_tb.equals(pd_new)


def custom_round(value):
    if value - math.floor(value) < 0.5:
        return math.floor(value)
    else:
        return math.ceil(value)

@keyword("VERIFY RESOURCE AWARE POLICY FOR BZPROCGRPS")
def verify_resource_aware_policy_for_bzprocgrps(remote_ip):
    """
    Function to verify Resource Aware Policy for Bzprocgrps
    Args: 1. remote_ip :  remote source IP where the query will be running.
    Returns:  True/False
    """
    ret = False

    logger.info('checking cpu count\n',also_console=True)
    ncpu = exe_cmd.remoteExecCommand(remote_ip," nproc --all")[remote_ip].split('\n')[0]
    xml_file_path = "/a/share/logger/mdt/ddcdir.bzproc.conf/bzmanager.xml"
    logger.info('checking number of additional threads from bzmanager.xml \n',also_console=True)
    additional_threads = exe_cmd.remoteExecCommand(remote_ip,"grep -E \"core count = \\\""+ ncpu + "\" /a/share/logger/mdt/ddcdir.bzproc.conf/bzmanager.xml |sed -e \"s/.*additional_threads =//g\" ")[remote_ip].split('\n')[0]
    additional_threads = additional_threads.replace('"','').replace('/', '').replace('>','').replace(' ','')
    additional_threads = int(additional_threads)
    logger.info(xml_file_path+'\n',also_console=True)
    ssh_client.get_files(remote_ip,xml_file_path,"local_xml_file")
    # Parse the XML file into an ElementTree object
    tree=et.parse("local_xml_file")

    root = tree.getroot()
    query = "/a/bin/sql2  \"select procgrp_name,worker_threads from _local_bzprocgrp_pid_map where procgrp_name in ("
    # Initialize an empty dictionary to hold the data
    weight_data = {}
    expected_threads = {}
    result = {}

    # Traverse the XML tree and populate the dictionary
    for child in root:
        if child.tag == 'procgroups':
            for subchild in child:
                if subchild.get('enabled') == 'true' and 'weight' in subchild.keys():
                    procgrp_name = subchild.get('name')
                    weight =  subchild.get('weight')
                    weight = int(weight)
                    weight_data[procgrp_name] = weight
    total_weight = sum(weight_data.values())
    for key, value in weight_data.items():
#        expected_threads[key] = -(additional_threads *value//-total_weight)+1
        expected_threads[key] = custom_round(additional_threads *value/total_weight)+1
        query = query + '\\"'+ key + '\\",'
    
    query = query[:-1] + ")\""
    logger.info('sql Query is : \n' + query)
    result = exe_cmd.remoteExecCommand(remote_ip, query)[remote_ip].split('\n')
    result = result[2:len(result)-3]
    result_dict = {}

    # Process each string in the list
    for item in result:
        # Split the string into key and value, stripping whitespace
        key_value = item.strip().rsplit(' ', 1)
        if len(key_value) == 2:
            key = key_value[0].strip()  # Get the key, remove extra spaces
            value = int(key_value[1].strip())  # Get the value, convert to int
            result_dict[key] = int(value)  # Add to dictionary

    print(result_dict)
    print(expected_threads)

    if result_dict == expected_threads:
        ret = True
    return ret

@keyword("GET FILE CONTENTS")
def get_file_contents(host_ip,file_path):
    """
    Function to get contents of a file
    Args: 1. host_ip :  remote source IP where the command will be run.
    Args: 2. file_path : absolute path of the file to read contents from
    Returns: contents of the given file
    """
    command = "cat " + file_path
    check_output =run_cmd.remoteExecCommand(host_ip, command)[host_ip].split("\n")
    smoosh_ip_list = ' '.join(check_output).split()
    return smoosh_ip_list[0]

@keyword("VALIDATE OLDEST FILE TIMESTAMP")
def validate_oldest_file_timestamp(host_ip, directory_to_check):
    """
    Function to get contents of a file
    Args: 1. host_ip :  remote source IP where the command will be run.
    Args: 2. directory_to_check : absolute path of the directory to validate the file's timestamp
    Returns: True, if the oldest file is not older than 24 hours
    """
    command = "find " + directory_to_check + " -type f -printf \"%A@ %p\n\" | sort -n | head -n2 "
    check_output =run_cmd.remoteExecCommand(host_ip, command)[host_ip].split("\n")
    timestamp_output = ' '.join(check_output).split()
    time_delta = int(time.time()) - int(float(timestamp_output[0]))
    if (int(time_delta/3600) <=24):
        return True
    else:
        return False


@keyword("CHECK IF A DIRECTORY IS EMPTY")
def check_if_a_directory_is_empty(host_ip, directory_to_check):
    """
    Function to get contents of a file
    Args: 1. host_ip :  remote source IP where the command will be run.
    Args: 2. directory_to_check : absolute path of the directory to validate the file's timestamp
    Returns: True, if the directory is empty
    """
    command = "find " + directory_to_check + " -mindepth 1 -maxdepth 1 | wc -l"
    check_output =run_cmd.remoteExecCommand(host_ip, command)[host_ip].split("\n")
    file_count = ' '.join(check_output).split()
    if int(file_count[0]) == 0:
        return True
    else:
        return False
      
def read_log_files(remote_ip,base_path, state_file):
    """
    Function to stop/start the specified service
    Args: 1. remote_ip :  remote IP address
          2. base_path: base_path to find dir
          3. name_pattern: pattern of the sub dirs
     Returns: list of files
    """
    state_file_path = base_path+state_file
    all_log_files = []
    logger.info("checking state file on %s" %(remote_ip),also_console=True)
    command = "cat " + state_file_path
    state_file_content = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")
    for files in state_file_content:
        all_log_files.append(files)
    return all_log_files

def ls_files_in_dir(remote_ip,base_path):
    """
    Function to stop/start the specified service
    Args: 1. remote_ip :  remote IP address
          2. base_path: base_path to find files
    Returns: list of files
    """
    logger.info("listing data files on %s" %(remote_ip),also_console=True)
    command = "ls " + base_path + "|grep -Ev \"state|headers|total\""
    log_files = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")
    return log_files

@keyword("VERIFY REMOTE STATE RETRIEVAL")
def verify_remote_state_retrieval(remote_ip,base_path, name_pattern, mtime=0):
    """
    Function to stop/start the specified service
    Args: 1. remote_ip :  remote IP address
          2. base_path: base_path to find dir
          3. name_pattern: pattern of the sub dirs
          4. mtime: mtime option of find command
    Returns: True or False
    """
    ret = False
    logger.info("finding latest log files on %s" %(remote_ip),also_console=True)
    #command = f"find {base_path} -type d -name '{name_pattern}' -mtime -{days}"
    command = f"find {base_path} -type d -name '{name_pattern}' -mtime {mtime}"
    latest_dir = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")[0]
    #archived_dir = "/pull/qlayer/archived/"
    log_file = latest_dir[24:87]
    #log_file_path = archived_dir + log_file
    #log_file_pat = log_file + "*"
    #command = f"find {archived_dir} -name '{log_file_pat}'|grep -v progress"
    #archive_dir = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")[0]
    #archive_dir = archive_dir + "/"
    logger.info("get data files list from %s" %(latest_dir),also_console=True)
    bzarchived_log_files = ls_files_in_dir (remote_ip,latest_dir)
    progress_file_pat = log_file+"*progress.done"
    command = f"find /pull/qlayer/archived/ -name '{progress_file_pat}'"
    progress_dir = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")[0]
    progress_dir = progress_dir + "/"
    logger.info("reading syncstate.done",also_console=True)
    syncstate_content = read_log_files(remote_ip, progress_dir, "syncstate.done")
    #bz_log_files = ls_files_in_dir (remote_ip,log_file_path)
    logger.info("check if data files are as per syncstate" ,also_console=True)
    if len(bzarchived_log_files) - len(syncstate_content) <= 1:
        ret = True

    return ret

@keyword("VERIFY REMOTE STATE RETRIEVAL RESUME CASE")
def verify_remote_state_retrieval_resume_case(remote_ip,base_path, name_pattern, mtime=0):
    """
    Function to stop/start the specified service
    Args: 1. remote_ip :  remote IP address
          2. base_path: base_path to find dir
          3. name_pattern: pattern of the sub dirs
          4. mtime: mtime option of find command
    Returns: True or False
    """
    ret = False
    logger.info("finding latest log files on %s" %(remote_ip),also_console=True)
    #command = f"find {base_path} -type d -name '{name_pattern}' -ctime -{days}"
    command = f"find {base_path} -type d -name '{name_pattern}' -mtime {mtime}"
    latest_dir = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")[0]
    archived_dir = "/pull/qlayer/archived/"
    log_file = latest_dir[25:88]
    log_file_pat = log_file + "*"
    command = f"find {archived_dir} -name '{log_file_pat}'|grep -v progress"
    logger.info("find log file dir  %s in archive dir " %(log_file),also_console=True)
    archive_dir = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")[0]
    archive_dir = archive_dir + "/"
    logger.info("get data files list from %s" %(archive_dir),also_console=True)
    archived_log_files = ls_files_in_dir (remote_ip,archive_dir)
    progress_file_pat = log_file+"*progress.done"
    command = f"find /pull/qlayer/archived/ -name '{progress_file_pat}'"
    logger.info("get progess dir",also_console=True)
    progress_dir = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].split("\n")[0]
    progress_dir = progress_dir + "/"
    logger.info("reading syncstate.done",also_console=True)
    syncstate_content = read_log_files(remote_ip, progress_dir, "syncstate.done")
    bz_log_files = ls_files_in_dir (remote_ip,archive_dir)
    sync_state_file = progress_dir + "syncstate.done"
    resume_case_ip_command = "head -1 " + sync_state_file
    logger.info("get remote ip",also_console=True)
    resume_case_ip = run_cmd.remoteExecCommand(remote_ip, resume_case_ip_command)[remote_ip].split("\n")[0].split(" ")[-1]
    logger.info("check if data files are as per syncstate" ,also_console=True)
    if check_if_a_directory_is_empty(resume_case_ip,progress_dir) == False and len(bz_log_files) == len(syncstate_content):
        ret = True

    return ret


@keyword('GET RETENTION DAYS')
def get_retention_days(remote_ip, log_type, retention_config_file, default_retention=15):
    """
    Get the retention days for a specific log type from the retention config file.
    
    Args:
        remote_ip: IP address of the remote machine to execute the command
        log_type: The log type to search for (e.g., 'ffbak', 'fpbak', etc.)
        retention_config_file: Path to the retention config file (e.g., /a/share/logger/mdt/ddcdir.cloud.conf/qlayer-clean-archived.conf)
        default_retention: Default retention days to use if log type is not found (default: 15)
    
    Returns:
        int: The retention days value for the specified log type, or default_retention if not found
    
    Example config format:
        <log type="ffbak"          retention="15"  tar="0" />
    """
    try:
        # Read the retention config file and grep for the log type
        command = f"cat {retention_config_file} | grep '{log_type}'"
        logger.info(f"Executing command: {command}")
        
        result = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].strip()
        logger.info(f"Command output: {result}")
        
        if not result:
            # Only use default retention if the config file is for storage
            if 'storage' in retention_config_file.lower():
                logger.warn(f"No retention configuration found for log type: {log_type}. Using default retention: {default_retention} days")
                return default_retention
            else:
                logger.error(f"No retention configuration found for log type: {log_type}")
                return None
        
        # Parse the XML-style config to extract retention days
        # Expected format: <log type="ffbak" retention="15" tar="0" />
        lines = result.strip().splitlines()
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            # Extract type attribute value using regex
            type_match = re.search(r'type\s*=\s*["\']([^"\']+)["\']', line)
            if type_match:
                config_log_type = type_match.group(1).strip()
                # Match exact log type (e.g., "ffbak" should not match "cncffbak")
                if config_log_type == log_type:
                    # Extract retention attribute value
                    retention_match = re.search(r'retention\s*=\s*["\'](\d+)["\']', line)
                    if retention_match:
                        retention_days = int(retention_match.group(1))
                        logger.info(f"Retention days for {log_type}: {retention_days}")
                        return retention_days
        
        # Only use default retention if the config file is for storage
        if 'storage' in retention_config_file.lower():
            logger.warn(f"Could not find exact match for log type: {log_type}. Using default retention: {default_retention} days")
            return default_retention
        else:
            logger.error(f"Could not find exact match for log type: {log_type}")
            return None
        
    except Exception as e:
        # Only use default retention if the config file is for storage
        if 'storage' in retention_config_file.lower():
            logger.error(f"Error getting retention days for {log_type}: {str(e)}. Using default retention: {default_retention} days")
            return default_retention
        else:
            logger.error(f"Error getting retention days for {log_type}: {str(e)}")
            return None


@keyword('VERIFY THE DATA CLEANUP AS PER RETENTION DAYS')
def verify_data_cleanup_as_per_retention_days(remote_ip, log_type, retention_days, archiver_path="/pull/qlayer/archived/"):
    """
    Verify that files older than retention days are not present in the archived folder.
    
    Args:
        remote_ip: IP address of the remote machine to execute the command
        log_type: The log type folder to check (e.g., 'ffbak', 'fpbak', etc.)
        retention_days: Number of days for retention policy
        archiver_path: Base path for archived data (default: /pull/qlayer/archived/)
    
    Returns:
        bool: True if no files older than retention days are found (cleanup is working), 
              False if old files are found (cleanup is not working)
    """
    try:
        full_path = f"{archiver_path}{log_type}"
        
        # Find files/directories older than retention days
        command = f"find {full_path} -maxdepth 1 -mtime +{retention_days}"
        logger.info(f"Executing command on {remote_ip}: {command}", also_console=True)
        
        result = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].strip()
        logger.info(f"Command output: {result}")
        
        if not result:
            logger.info(f"No files/directories older than {retention_days} days found in {full_path} on {remote_ip}. Data cleanup is working correctly.", also_console=True)
            return True
        
        # Check if result contains actual files (not just empty lines)
        old_files = [f for f in result.splitlines() if f.strip()]
        
        if len(old_files) == 0:
            logger.info(f"No files/directories older than {retention_days} days found in {full_path} on {remote_ip}. Data cleanup is working correctly.", also_console=True)
            return True
        
        # Files older than retention days found - this is a failure
        logger.error(f"Found {len(old_files)} files/directories older than {retention_days} days in {full_path} on {remote_ip}:", also_console=True)
        for old_file in old_files:
            logger.error(f"  - {old_file}", also_console=True)
        
        logger.error(f"Data cleanup is NOT working correctly on {remote_ip}. Files older than retention period still exist.", also_console=True)
        return False
        
    except Exception as e:
        logger.error(f"Error verifying data cleanup on {remote_ip}: {str(e)}")
        return False


@keyword('CHECK IF DIRECTORY EXISTS')
def check_if_directory_exists(remote_ip, directory_path):
    """
    Check if a directory exists on a remote machine.
    
    Args:
        remote_ip: IP address of the remote machine
        directory_path: Path to the directory to check (e.g., /pull/qlayer/archived/ffbak)
    
    Returns:
        bool: True if directory exists, False otherwise
    """
    try:
        command = f"test -d {directory_path} && echo 'EXISTS' || echo 'NOT_EXISTS'"
        logger.info(f"Checking if directory exists on {remote_ip}: {directory_path}")
        
        result = run_cmd.remoteExecCommand(remote_ip, command)[remote_ip].strip()
        logger.info(f"Directory check result: {result}")

        if 'NOT_EXISTS' in result:
            logger.info(f"Directory {directory_path} does NOT exist on {remote_ip}")
            return False
        else:
            logger.warn(f"Directory {directory_path} exists on {remote_ip}")
            return True
            
    except Exception as e:
        logger.error(f"Error checking directory existence on {remote_ip}: {str(e)}")
        return False

