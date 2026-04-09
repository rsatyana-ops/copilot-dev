#Importing Libraries/Resources
*** Settings ***
Library  BuiltIn
Library  String
Library  Collections
Library  ${EXECDIR}/lib/ZephyrLink.py  cycle_name=${cycle_name}
Library  ${EXECDIR}/ddc/util/ddc_util/breeze_util.py
Library   ${EXECDIR}/ddc/util/common_utilities.py
Suite Setup     SUITE SETUP COMPOSITE 

*** Variables ***
${QP_US_HOST_NAME}              freeflow.us.bzlogs.shared.qa.akamai.com
${QP_EU_HOST_NAME}              freeflow.eu.bzlogs.shared.qa.akamai.com
${BZARCHIVER_US_HOST_NAME}      ffbak.us.bzarchiver.shared.qa.akamai.com
${BZARCHIVER_EU_HOST_NAME}      ffbak.eu.bzarchiver.shared.qa.akamai.com
${log_type}                     ffbak
${qp_retention_config_file}        /a/share/logger/mdt/ddcdir.cloud.conf/qlayer-clean-archived.conf
${storage_retention_config_file}    /a/share/logger/mdt/ddcdir.storage.conf/ddc.archive_retention.xml
${ltdb_ip}                      198.18.148.71

*** Keywords ***
SUITE SETUP COMPOSITE

    # QP US Host
    @{QP_US_IP_LIST}=   GET IP FROM DNS   ${QP_US_HOST_NAME}      ${ltdb_ip}
    ${QP_US_HOST}=    Find Machine With Latest Release And Contains Archived Directory   ${QP_US_IP_LIST}    ${cycle_name}    ${log_type}   
    Run Keyword If    '${QP_US_HOST}' == 'None'    Fail    No QP US machine has the latest release installed
    Set Suite Variable    ${QP_US_HOST}
    Log To Console    Chosen QP US machine: ${QP_US_HOST}

    # QP EU Host
    @{QP_EU_IP_LIST}=   GET IP FROM DNS   ${QP_EU_HOST_NAME}      ${ltdb_ip}
    ${QP_EU_HOST}=    Find Machine With Latest Release And Contains Archived Directory    ${QP_EU_IP_LIST}    ${cycle_name}    ${log_type}
    Run Keyword If    '${QP_EU_HOST}' == 'None'    Fail    No QP EU machine has the latest release installed
    Set Suite Variable    ${QP_EU_HOST}
    Log To Console    Chosen QP EU machine: ${QP_EU_HOST}

    # BZARCHIVER US Host
    @{BZARCHIVER_US_IP_LIST}=   GET IP FROM DNS   ${BZARCHIVER_US_HOST_NAME}      ${ltdb_ip}
    ${BZARCHIVER_US_HOST}=    Find Machine With Latest Release And Contains Archived Directory    ${BZARCHIVER_US_IP_LIST}    ${cycle_name}    ${log_type}
    Run Keyword If    '${BZARCHIVER_US_HOST}' == 'None'    Fail    No BZARCHIVER US machine has the latest release installed
    Set Suite Variable    ${BZARCHIVER_US_HOST}
    Log To Console    Chosen BZARCHIVER US machine: ${BZARCHIVER_US_HOST}

    # BZARCHIVER EU Host
    @{BZARCHIVER_EU_IP_LIST}=   GET IP FROM DNS   ${BZARCHIVER_EU_HOST_NAME}      ${ltdb_ip}
    ${BZARCHIVER_EU_HOST}=    Find Machine With Latest Release And Contains Archived Directory    ${BZARCHIVER_EU_IP_LIST}    ${cycle_name}    ${log_type}
    Run Keyword If    '${BZARCHIVER_EU_HOST}' == 'None'    Fail    No BZARCHIVER EU machine has the latest release installed
    Set Suite Variable    ${BZARCHIVER_EU_HOST}
    Log To Console    Chosen BZARCHIVER EU machine: ${BZARCHIVER_EU_HOST}

    # Get Retention Days for specific logtype in QP
    ${qp_retention_days}=   GET RETENTION DAYS     ${ltdb_ip}      ${log_type}     ${qp_retention_config_file}
    Run Keyword If    '${qp_retention_days}' == 'None'    Fail    Could not get retention days for log type: ${log_type}
    Set Suite Variable    ${qp_retention_days}
    Log To Console    Retention days for ${log_type}: ${qp_retention_days}

        # Get Retention Days for specific logtype in Storage
    ${storage_retention_days}=   GET RETENTION DAYS     ${ltdb_ip}      ${log_type}     ${storage_retention_config_file}
    Run Keyword If    '${storage_retention_days}' == 'None'    Fail    Could not get retention days for log type: ${log_type}
    Set Suite Variable    ${storage_retention_days}
    Log To Console    Retention days for ${log_type}: ${storage_retention_days}

Find Machine With Latest Release And Contains Archived Directory
    [Arguments]    ${ip_list}    ${cycle_name}      ${log_type}
    [Documentation]    Iterates through IP list and returns the first machine with latest release and contains archived directory. Returns None if no machine found.
    # Shuffle the list to randomize the order
    ${shuffled_list}=    Evaluate    random.sample(${ip_list}, len(${ip_list}))    modules=random
    ${archived_path}=    Set Variable    /pull/qlayer/archived/${log_type}
    FOR    ${ip}    IN    @{shuffled_list}
        ${release_result}=    CHECK RELEASE NUMBER IN MACHINES BASED ON CYCLE_NAME    ${cycle_name}    ${ip}
        Run Keyword If    not ${release_result}    Continue For Loop
        # Check if archived directory exists
        ${dir_exists}=    CHECK IF DIRECTORY EXISTS    ${ip}    ${archived_path}
        Run Keyword If    ${release_result} and ${dir_exists}    Return From Keyword    ${ip}
        Log To Console    Machine ${ip} has latest release but archived directory ${archived_path} not found. Trying next machine...
    END
    Return From Keyword    None


*** Test Cases ***
Verify Data Retention In QP Machine And Bzarchiver/Rebalancing Machine
    [Tags]  ZEDDCSQA-6144

    Log To Console  Verify Data Retention for ${log_type} logtype in QP US Machine
    ${result_1}=    VERIFY THE DATA CLEANUP AS PER RETENTION DAYS   ${QP_US_HOST}   ${log_type}   ${qp_retention_days}
    Run Keyword If  '${result_1}' == 'False'  Fail  Retention validation failed for QP US Machine: ${QP_US_HOST}

    Log To Console  Verify Data Retention for ${log_type} logtype in QP EU Machine
    ${result_2}=    VERIFY THE DATA CLEANUP AS PER RETENTION DAYS   ${QP_EU_HOST}   ${log_type}   ${qp_retention_days}
    Run Keyword If  '${result_2}' == 'False'  Fail  Retention validation failed for QP EU Machine: ${QP_EU_HOST}

    Log To Console  Verify Data Retention for ${log_type} logtype in BZARCHIVER US Machine
    ${result_3}=    VERIFY THE DATA CLEANUP AS PER RETENTION DAYS   ${BZARCHIVER_US_HOST}   ${log_type}   ${storage_retention_days}
    Run Keyword If  '${result_3}' == 'False'  Fail  Retention validation failed for BZARCHIVER US Machine: ${BZARCHIVER_US_HOST}

    Log To Console  Verify Data Retention for ${log_type} logtype in BZARCHIVER EU Machine
    ${result_4}=    VERIFY THE DATA CLEANUP AS PER RETENTION DAYS   ${BZARCHIVER_EU_HOST}   ${log_type}   ${storage_retention_days}
    Run Keyword If  '${result_4}' == 'False'  Fail  Retention validation failed for BZARCHIVER EU Machine: ${BZARCHIVER_EU_HOST}

    Log To Console  Data Retention validation passed for all machines

    Log To Console  Verify Data Retention for storebak logtype in BZARCHIVER MACHINE
    ${storebak_retention_days}=     GET RETENTION DAYS     ${ltdb_ip}      storebak     ${storage_retention_config_file}
    Run Keyword If    '${storebak_retention_days}' == 'None'    Fail    Could not get retention days for log type: storebak
    Log To Console    Retention days for storebak: ${storebak_retention_days}
    ${result}=   VERIFY THE DATA CLEANUP AS PER RETENTION DAYS   ${BZARCHIVER_US_HOST}   storebak   ${storebak_retention_days}
    Run Keyword If  '${result}' == 'False'  Fail  Retention validation failed for BZARCHIVER Machine: ${BZARCHIVER_US_HOST}
