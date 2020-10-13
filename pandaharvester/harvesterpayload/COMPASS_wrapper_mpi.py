#!/usr/bin/env python

import sys
import os
import pwd
import shutil
import time
import json
from socket import gethostname
import subprocess
import logging
from mpi4py import MPI
import random
from datetime import datetime
from glob import glob
import tarfile
import re
import collections

from jobdescription import JobDescription
from PilotErrors import PilotErrors             # Error codes

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
max_rank = comm.Get_size()

logger = logging.getLogger('Rank {0}' . format(rank))
logger.setLevel(logging.DEBUG)
debug_h = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
debug_h.setFormatter(formatter)
debug_h.setLevel(logging.DEBUG)
error_h = logging.StreamHandler(stream=sys.stderr)
error_h.setFormatter(formatter)
error_h.setLevel(logging.ERROR)
logger.addHandler(error_h)
logger.addHandler(debug_h)


def grep(patterns, file_name):
    """ Search for the patterns in the given list in a file """
    # Example:
    # grep(["St9bad_alloc", "FATAL"], "athena_stdout.txt")
    # -> [list containing the lines below]
    #   CaloTrkMuIdAlg2.sysExecute()            ERROR St9bad_alloc
    #   AthAlgSeq.sysExecute()                   FATAL  Standard std::exception is caught

    matched_lines = []
    p = []
    for pattern in patterns:
        p.append(re.compile(pattern))

    try:
        f = open(file_name, "r")
    except IOError, e:
        logger.warning('{0}' . format (e))
    else:
        while True:
            # get the next line in the file
            line = f.readline()
            if not line:
                break

            # can the search pattern be found
            for cp in p:
                if re.search(cp, line):
                    matched_lines.append(line)
        f.close()
    return matched_lines

def read_json(filename):
    """
    Read a dictionary with unicode to utf-8 conversion
    :param filename:
    :raises PilotException: FileHandlingFailure, ConversionFailure
    :return: json dictionary
    """

    dictionary = None
    f = open(filename, 'r')
    if f:
        try:
            dictionary = json.load(f)
        except Exception as e:
            logger.warning('exception caught: {0}'.format(e))
            #raise FileHandlingFailure(str(e))
        else:
            f.close()

            # Try to convert the dictionary from unicode to utf-8
            if dictionary != {}:
                try:
                    dictionary = convert(dictionary)
                except Exception as e:
                    logger.warning('exception caught: {0}'.format(e))
                    #raise ConversionFailure(e)

    return dictionary

def convert(data):
    """
    Convert unicode data to utf-8.
    Usage examples:
    1. Dictionary:
      data = {u'Max': {u'maxRSS': 3664, u'maxSwap': 0, u'maxVMEM': 142260, u'maxPSS': 1288}, u'Avg':
             {u'avgVMEM': 94840, u'avgPSS': 850, u'avgRSS': 2430, u'avgSwap': 0}}
    convert(data)
      {'Max': {'maxRSS': 3664, 'maxSwap': 0, 'maxVMEM': 142260, 'maxPSS': 1288}, 'Avg': {'avgVMEM': 94840,
       'avgPSS': 850, 'avgRSS': 2430, 'avgSwap': 0}}
    2. String:
      data = u'hello'
    convert(data)
      'hello'
    3. List:
      data = [u'1',u'2','3']
    convert(data)
      ['1', '2', '3']
    :param data: unicode object to be converted to utf-8
    :return: converted data to utf-8
    """

    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data
def getNumberOfEvents(payload_stdout):
    """ Returns the number of events """

    nEventsRead = 0
    nEventsWritten = 0
    nEvents_str = ""
    N = 0
    
    logger.debug("Looking for number of processed events (pass 2: Resorting to brute force grepping of payload stdout)")
    
    logger.debug("Processing stdout of normal job")
    matched_lines = grep(["events had been written to miniDST"], payload_stdout)
    if len(matched_lines) > 0:
        N = int(re.match('^(\d+) events had been written to miniDST.*', matched_lines[-1]).group(1))
    
    logger.debug("Processing stdout file of merging")
    matched_lines = grep(["Number of events saved to output"], payload_stdout)
    if len(matched_lines) > 0:
        N = int(re.match('^Number of events saved to output\s+:\s+(\d+)', matched_lines[-1]).group(1))

    if len(nEvents_str) == 0:
        nEvents_str = str(N)
    nEventsRead += N

#    return nEventsRead, nEventsWritten, nEvents_str
    return nEventsRead

def timestamp():
    """ return ISO-8601 compliant date/time format. Should be migrated to Pilot 2"""
    tmptz = time.timezone
    sign_str = '+'
    if tmptz > 0:
        sign_str = '-'
    tmptz_hours = int(tmptz / 3600)

    return str("%s%s%02d:%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), sign_str, abs(tmptz_hours),
                                  int(tmptz / 60 - tmptz_hours * 60)))

def main_exit(exit_code, work_report=None, workerAttributesFile="worker_attributes.json"):
    if work_report:
        publish_work_report(work_report, workerAttributesFile)
    sys.exit(exit_code)

def publish_work_report(work_report=None, workerAttributesFile="worker_attributes.json"):
    """Publishing of work report to file"""
    if work_report:
        if work_report.has_key("outputfiles"):
            del (work_report["outputfiles"])
        with open(workerAttributesFile, 'w') as outputfile:
            work_report['timestamp'] = timestamp()
            json.dump(work_report, outputfile)
        logger.debug("Work report published: {0}" . format(work_report))
    return 0

def removeRedundantFiles(workdir, outputfiles=[]):
    """ Remove redundant files and directories. Should be migrated to Pilot2 """

    logger.info("Removing redundant files prior to log creation")

    workdir = os.path.abspath(workdir)

    dir_list = ["AtlasProduction*",
                "AtlasPoint1",
                "AtlasTier0",
                "buildJob*",
                "CDRelease*",
                "csc*.log",
                "DBRelease*",
                "EvgenJobOptions",
                "external",
                "fort.*",
                "geant4",
                "geomDB",
                "geomDB_sqlite",
                "home",
                "o..pacman..o",
                "pacman-*",
                "python",
                "runAthena*",
                "share",
                "sources.*",
                "sqlite*",
                "sw",
                "tcf_*",
                "triggerDB",
                "trusted.caches",
                "workdir",
                "*.data*",
                "*.events",
                "*.py",
                "*.pyc",
                "*.root*",
                "JEM",
                "tmp*",
                "*.tmp",
                "*.TMP",
                "MC11JobOptions",
                "scratch",
                "jobState-*-test.pickle",
                "*.writing",
                "pwg*",
                "pwhg*",
                "*PROC*",
                "madevent",
                "HPC",
                "objectstore*.json",
                "saga",
                "radical",
                "ckpt*"]

    # explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command (--dereference option)
    matches = []
    import fnmatch
    for root, dirnames, filenames in os.walk(workdir):
        for filename in fnmatch.filter(filenames, '*.a'):
            matches.append(os.path.join(root, filename))
    for root, dirnames, filenames in os.walk(os.path.dirname(workdir)):
        for filename in fnmatch.filter(filenames, 'EventService_premerge_*.tar'):
            matches.append(os.path.join(root, filename))
    if matches != []:
        for f in matches:
            remove(f)
    # else:
    #    print("Found no archive files")

    # note: these should be partitial file/dir names, not containing any wildcards
    exceptions_list = ["runargs", "runwrapper", "jobReport", "log."]

    to_delete = []
    for _dir in dir_list:
        files = glob(os.path.join(workdir, _dir))
        exclude = []

        if files:
            for exc in exceptions_list:
                for f in files:
                    if exc in f:
                        exclude.append(os.path.abspath(f))

            _files = []
            for f in files:
                if not f in exclude:
                    _files.append(os.path.abspath(f))
            to_delete += _files

    exclude_files = []
    for of in outputfiles:
        exclude_files.append(os.path.join(workdir, of))
    for f in to_delete:
        if not f in exclude_files:
            remove(f)

    # run a second pass to clean up any broken links
    broken = []
    for root, dirs, files in os.walk(workdir):
        for filename in files:
            path = os.path.join(root, filename)
            if os.path.islink(path):
                target_path = os.readlink(path)
                # Resolve relative symlinks
                if not os.path.isabs(target_path):
                    target_path = os.path.join(os.path.dirname(path), target_path)
                if not os.path.exists(target_path):
                    broken.append(path)
            else:
                # If it's not a symlink we're not interested.
                continue

    if broken:
        for p in broken:
            remove(p)

    return 0

def remove(path):
    "Common function for removing of file. Should migrate to Pilo2"
    try:
        os.unlink(path)
    except OSError as e:
        logger.error("Problem with deletion: %s : %s" % (e.errno, e.strerror))
        return -1
    return 0


def packlogs(wkdir, excludedfiles, logfile_name, attempt=0):
    # logfile_size = 0
    to_pack = []
    pack_start = time.time()
    for path, subdir, files in os.walk(wkdir):
        for file in files:
            if not file in excludedfiles:
                relDir = os.path.relpath(path, wkdir)
                file_rel_path = os.path.join(relDir, file)
                file_path = os.path.join(path, file)
                to_pack.append((file_path, file_rel_path))
    if to_pack:
        try:
            logfile_name = os.path.join(wkdir, logfile_name)
            log_pack = tarfile.open(logfile_name, 'w:gz')
            for f in to_pack:
                log_pack.add(f[0], arcname=f[1])
            log_pack.close()
            # logfile_size = os.path.getsize(logfile_name)
        except IOError as e:
            if attempt == 0:
                safe_delay = 15
                logger.info('I/O error. Will retry in {0} sec.'.format(safe_delay))
                time.sleep(safe_delay)
                packlogs(wkdir, excludedfiles, logfile_name, attempt=1)
            else:
                logger.info("Continues I/O error during packing of logs. Job will be failed")
                return 1

    for f in to_pack:
        remove(f[0])

    del_empty_dirs(wkdir)
    pack_time = time.time() - pack_start
    logger.debug("Pack of logs took: {0} sec.".format(pack_time))
    return 0

def del_empty_dirs(src_dir):
    "Common function for removing of empty directories. Should migrate to Pilo2"

    for dirpath, subdirs, files in os.walk(src_dir, topdown=False):
        if dirpath == src_dir:
            break
        try:
            os.rmdir(dirpath)
        except OSError as ex:
            pass
    return 0

def isSomethingInStd(**kwargs):
    """ Check if job output contains message """
        
    test = False
    
    what = kwargs.get('what', None)
    where = kwargs.get('where', None)

    logger.info("Checking {0} in {1}" . format(what, where))
    
    if os.path.exists(where):
        logger.info("Processing file: {0}" . format(where))
        matched_lines = grep([what], where)
        if len(matched_lines) > 0:
            logger.info("Identified a '{0}' in {1}:" . format(what, where))
            for line in matched_lines:
                logger.info(line)
            test = True
    else:
        logger.warn("File {0} does not exist" . format(where))

    return test

def interpretPayloadStds(job, payload_stdout_file, payload_stderr_file):
    """ Payload error interpretation and handling """
    
    work_attributes = {}
    
    # these are default values for job metrics
    core_count = 1
    work_attributes["nEvents"] = getNumberOfEvents(payload_stdout_file)
    work_attributes["dbTime"] = ""
    work_attributes["dbData"] = ""
    
    work_attributes['jobMetrics'] = 'coreCount=%s nEvents=%s dbTime=%s dbData=%s' % \
                                    (core_count,
                                     work_attributes["nEvents"],
                                     work_attributes["dbTime"],
                                     work_attributes["dbData"])
    
    del (work_attributes["dbData"])
    del (work_attributes["dbTime"])
    
    error = PilotErrors()

    if job.script == 'test production' or job.script == 'merging mdst' or job.script == 'mass production' or job.script == 'technical production':
        is_end_of_job = isSomethingInStd(what="End of Job", where=payload_stdout_file)
    elif job.script == 'DDD filtering':
        is_end_of_job = isSomethingInStd(what="End of the decoding!", where=payload_stdout_file)
    else:
        is_end_of_job = True
    
    a_fatal_error_appeared = isSomethingInStd(what="A FATAL ERROR APPEARED", where=payload_stdout_file)
    core_dumped = isSomethingInStd(what="\(core dumped\) \$CORAL/../phast/coral/coral.exe", where=payload_stderr_file)
    no_events = isSomethingInStd(what="char\* exception: DaqEventsManager::GetEvent\(\): no event", where=payload_stderr_file)
    abnormal_job_termination = isSomethingInStd(what="Abnormal job termination. Exception #2 had been caught", where=payload_stdout_file)
    aborted = isSomethingInStd(what="Aborted ", where=payload_stderr_file)
    cannot_allocate = isSomethingInStd(what="TLattice::TLattice: Cannot Allocate [0-9]+ Bytes in Memory!", where=payload_stderr_file)
    empty_string = isSomethingInStd(what="_LEDinSpills empty string from calib file", where=payload_stderr_file)
    cant_connect_to_cdb = isSomethingInStd(what="can't connect to CDB database", where=payload_stderr_file)
    read_calib_bad_line = isSomethingInStd(what="Exception in readCalibration(): EC02P1__: InputTiSdepCorr EC02P1__ bad line", where=payload_stderr_file)
    killed = isSomethingInStd(what="Killed\* \$CORAL/../phast/coral/coral.exe", where=payload_stderr_file)
    exiting_with_code3 = isSomethingInStd(what="CORAL exiting with return code -3", where=payload_stdout_file)
    error_loading_lib_posix = isSomethingInStd(what="error while loading shared libraries: libXrdPosix.so", where=payload_stderr_file)
    lost_conn_to_mysql = isSomethingInStd(what="Lost connection to MySQL server", where=payload_stderr_file)
    can_not_open_file = isSomethingInStd(what="DaqEventsManager::NextDataSource(): Can not open file", where=payload_stderr_file)
    zero_events = isSomethingInStd(what="^0 events had been written to miniDST.", where=payload_stdout_file)
    error_reading_bytes = isSomethingInStd(what="Error in <TFile::ReadBuffer>: error reading all requested bytes from file", where=payload_stderr_file)
    chip_readmaps = isSomethingInStd(what="Chip::ReadMaps: std::exception", where=payload_stderr_file)
    
    failed = False
    is_no_end_of_job = False
    if not is_end_of_job:
        failed = True
        is_no_end_of_job = True
    if a_fatal_error_appeared or core_dumped or no_events or abnormal_job_termination or aborted or cannot_allocate or \
        empty_string or cant_connect_to_cdb or read_calib_bad_line or killed or exiting_with_code3 or error_loading_lib_posix or \
        lost_conn_to_mysql or can_not_open_file or zero_events or error_reading_bytes or chip_readmaps:
        failed = True
    
    # handle non-zero failed job return code but do not set pilot error codes to all payload errors
    if failed:
        work_attributes["jobStatus"] = "failed"
        if core_dumped:
            work_attributes["pilotErrorCode"] = error.ERR_COREDUMPED
            work_attributes["pilotErrorDiag"] = error.pilotError[error.ERR_COREDUMPED]
        elif no_events:
            work_attributes["pilotErrorDiag"] = "char* exception: DaqEventsManager::GetEvent(): no event"
            work_attributes["pilotErrorCode"] = error.ERR_NOEVENTS
        elif abnormal_job_termination:
            work_attributes["pilotErrorDiag"] = "Abnormal job termination. Exception #2 had been caught"
            work_attributes["pilotErrorCode"] = error.ERR_ABNORMALJOBTERMINATION
        elif aborted:
            work_attributes["pilotErrorDiag"] = "Aborted "
            work_attributes["pilotErrorCode"] = error.ERR_ABORTED
        elif cannot_allocate:
            work_attributes["pilotErrorDiag"] = "TLattice::TLattice: Cannot Allocate N Bytes in Memory!"
            work_attributes["pilotErrorCode"] = error.ERR_CANNOTALLOCATE
#            elif events_skipped:
#                job.pilotErrorDiag = "Event skipped due to decoding troubles"
#                job.result[2] = error.ERR_EVENTSSKIPPED
        elif empty_string:
            work_attributes["pilotErrorDiag"] = "_LEDinSpills empty string from calib file"
            work_attributes["pilotErrorCode"] = error.ERR_EMPTYSTRING
        elif cant_connect_to_cdb:
            work_attributes["pilotErrorDiag"] = "can't connect to CDB database"
            work_attributes["pilotErrorCode"] = error.ERR_CANTCONNECTTOCDB
        elif read_calib_bad_line:
            work_attributes["pilotErrorDiag"] = "Exception in readCalibration(): EC02P1__: InputTiSdepCorr EC02P1__ bad line"
            work_attributes["pilotErrorCode"] = error.ERR_READCALIBBADLINE
        elif killed:
            work_attributes["pilotErrorDiag"] = "Killed $CORAL/../phast/coral/coral.exe"
            work_attributes["pilotErrorCode"] = error.ERR_KILLED
        elif error_loading_lib_posix:
            work_attributes["pilotErrorDiag"] = "error while loading shared libraries: libXrdPosix.so"
            work_attributes["pilotErrorCode"] = error.ERR_LOADING_ERR_POSIX
        elif lost_conn_to_mysql:
            work_attributes["pilotErrorDiag"] = "Lost connection to MySQL server"
            work_attributes["pilotErrorCode"] = error.ERR_LOST_CONN_TO_MYSQL
        elif can_not_open_file:
            work_attributes["pilotErrorDiag"] = "DaqEventsManager::NextDataSource(): Can not open file"
            work_attributes["pilotErrorCode"] = error.ERR_CAN_NOT_OPEN_FILE
        elif is_no_end_of_job:
            work_attributes["pilotErrorDiag"] = "No End of Job message found in stdout"
            work_attributes["pilotErrorCode"] = error.ERR_NOENDOFJOB
        elif a_fatal_error_appeared:
            work_attributes["pilotErrorDiag"] = "A fatal error appeared"
            work_attributes["pilotErrorCode"] = error.ERR_AFATALERRORAPPEARED
        elif zero_events:
            work_attributes["pilotErrorDiag"] = "0 events had been written to miniDST."
            work_attributes["pilotErrorCode"] = error.ERR_ZERO_EVENTS
        elif error_reading_bytes:
            work_attributes["pilotErrorDiag"] = "Error in <TFile::ReadBuffer>: error reading all requested bytes from file"
            work_attributes["pilotErrorCode"] = error.ERR_READING_BYTES
        elif chip_readmaps:
            work_attributes["pilotErrorDiag"] = "Chip::ReadMaps: std::exception"
            work_attributes["pilotErrorCode"] = error.ERR_CHIP_READMAPS
        elif exiting_with_code3:
            work_attributes["pilotErrorDiag"] = "CORAL exiting with return code -3"
            work_attributes["pilotErrorCode"] = error.ERR_EXITED_WITH_CODE3
        else:
            work_attributes["pilotErrorDiag"] = "Payload failed due to unknown reason (check payload stdout)"
            work_attributes["pilotErrorCode"] = error.ERR_UNKNOWN
        logger.error(work_attributes["pilotErrorDiag"])

    return work_attributes

def copy_jobreport(job_working_dir, worker_communication_point, payload_report_file, workerattributesfile):
    src_file = os.path.join(job_working_dir, payload_report_file)
    dst_file = os.path.join(worker_communication_point, payload_report_file)

    try:
        logger.info(
            "Copy of payload report [{0}] to access point: {1}".format(payload_report_file, worker_communication_point))
        cp_start = time.time()
        # shrink jobReport
        job_report = read_json(src_file)
        if 'executor' in job_report:
            for executor in job_report['executor']:
                if 'logfileReport' in executor:
                    executor['logfileReport'] = {}

        with open(dst_file, 'w') as job_report_outfile:
            json.dump(job_report, job_report_outfile)
        cp_time = time.time() - cp_start
        logger.info("Copy of payload report file took: {0} sec.".format(cp_time))
    except:
        logger.error("Job report copy failed, execution terminated':  \n %s " % (sys.exc_info()[1]))
        work_report = dict()
        work_report["jobStatus"] = "failed"
        work_report["pilotErrorCode"] = 1103  # Should be changed to Pilot2 errors
        work_report["exitMsg"] = str(sys.exc_info()[1])
        main_exit(1103, work_report, workerattributesfile)

def frontera_prepare_wd(scratch_path, trans_job_workdir, worker_communication_point, job, workerAttributesFile):
    # Copy files to tmp to cope high IO. Move execution to RAM disk

    copy_start = time.time()
    if os.path.exists(scratch_path):
        try:
            if not os.path.exists(trans_job_workdir):
                logger.info('Creating {0}' . format(trans_job_workdir))
                os.makedirs(trans_job_workdir)
            time.sleep(2)
        except IOError as e:
            copy_time = time.time() - copy_start
            logger.info('Special Frontera setup failed after: {0}' . format(copy_time))
            logger.error("Copy to scratch failed, execution terminated':  \n %s " % (sys.exc_info()[1]))
            work_report = dict()
            work_report["jobStatus"] = "failed"
            work_report["pilotErrorCode"] = 1103  # Should be changed to Pilot2 errors
            work_report["exitMsg"] = str(sys.exc_info()[1])
            main_exit(1103, work_report, workerAttributesFile)
        except:
            pass
    else:
        logger.info('Scratch directory (%s) does not exist' % scratch_path)
        return worker_communication_point

    os.chdir(trans_job_workdir)
    logger.debug("Current directory: {0}" . format(os.getcwd()))
    copy_time = time.time() - copy_start
    logger.info('Special Frontera setup took: {0}' . format(copy_time))

    return trans_job_workdir

def main():
    workerAttributesFile = "worker_attributes.json"
    StageOutnFile = "event_status.dump.json"
    payload_report_file = 'jobReport.json'
    payload_stdout_file = "payload_stdout.txt"
    payload_stderr_file = "payload_stderr.txt"

    start_g = time.time()
    start_g_str = time.asctime(time.localtime(start_g))
    hostname = gethostname()
    logger.info("Script started at {0} on {1}" . format(start_g_str, hostname))
    
    starting_point = os.getcwd()
    scratch_path = '/tmp/'
#    scratch_path = os.path.join(scratch_path, str(pwd.getpwuid( os.getuid() ).pw_uid))
#    logger.info('Scratch path: {0}' . format(scratch_path))
    
    
    my_pid = str(os.getpid())
#    logger.info('My pid is {0}' . format(my_pid))
    
    work_report = {}
    work_report["jobStatus"] = "starting"
    work_report["messageLevel"] = logging.getLevelName(logger.getEffectiveLevel())
    work_report['cpuConversionFactor'] = 1.0
    work_report['node'] = hostname
    
    # Get a file name with job descriptions
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = 'worker_pandaids.json'
    try:
        in_file = open(input_file)
        panda_ids = json.load(in_file)
        in_file.close()
    except IOError as (errno, strerror):
        logger.critical("I/O error({0}): {1}" . format(errno, strerror))
        logger.critical("Exit from rank")
        main_exit(errno)

#    logger.debug("Collected list of jobs {0}" . format(panda_ids))
    # PandaID of the job for the command
    try:
        job_id = panda_ids[rank]
    except ValueError:
        logger.critical("Pilot have no job: rank {0}" . format(rank))
        logger.critical("Exit pilot")
        main_exit(1)

    logger.debug("Job [{0}] will be processed" . format(job_id))
    os.chdir(str(job_id))
    
    worker_communication_point = os.getcwd()

    work_report['workdir'] = worker_communication_point
    workerAttributesFile = os.path.join(worker_communication_point, workerAttributesFile)
    trans_job_workdir = os.path.join(scratch_path, str(job_id))
    
    try:
        job_file = open("HPCJobs.json")
        jobs = json.load(job_file)
        job_file.close()
    except IOError as (errno, strerror):
        logger.critical("I/O error({0}): {1}" . format(errno, strerror))
        logger.critical("Unable to open 'HPCJobs.json'")
        main_exit(errno)

    job_dict = jobs[str(job_id)]
    
    job = JobDescription()
    job.load(job_dict)
    
    job.startTime = ""
    job.endTime = ""

    job_working_dir = os.getcwd()
    if rank % 56 != 0:
        sleep_time = 2 + random.randint(10, 30)
        logger.info("Rank {0} is going to sleep {1} seconds to avoid overloading of the FS while creating the working dir" . format(rank, sleep_time))
        time.sleep(sleep_time)
    
    logger.info("Rank {0} job.script: {1}" . format(rank, job.script))
    if job_dict['transformation'].find('merging') == -1:
        job_working_dir = frontera_prepare_wd(scratch_path, trans_job_workdir, worker_communication_point, job, workerAttributesFile)
    
    if rank % 56 == 0:
        logger.info("Rank {0} is going to start MySQL db" . format(rank))
        logger.info("Preparing environment")
        dbsetup_comm_arr = [
                            'cp /scratch3/projects/phy20003/PanDA/sw/standalone-database.sh .',
                            'sh standalone-database.sh &>dbsetup.log &'
                        ]
        dbsetup_comm = "\n" . join(dbsetup_comm_arr)
        p = subprocess.Popen(dbsetup_comm, shell=True)
        time.sleep(600)
        
        logger.info("Going to check MySQL server status")
        output = subprocess.Popen("ps -C mysqld -o pid=", stdout=subprocess.PIPE, shell=True).communicate()[0]
        logger.info("MySQL server pid: {0}" . format(output.rstrip()))
        if len(output) > 0:
            logger.info("MySQL database is running")
            
            output = subprocess.Popen("echo $HOSTNAME", stdout=subprocess.PIPE, shell=True).communicate()[0]
            logger.info("MySQL database is running on {0}" . format(output.rstrip()))
        else:
            logger.info("MySQL database is not running, exiting")
    else:
        sleep_time = 600 + random.randint(20, 100)
        logger.info("Rank {0} is going to sleep {1} seconds to allow MySQL db to start" . format(rank, sleep_time))
        time.sleep(sleep_time)
    
    mysql_host = "0.0.0.0"
    coral_cdbserver_comm = "export MYSQL_UNIX_PORT=/tmp/sw_mysql_859171/mysql.sock;export CDBSERVER={0};" . format(mysql_host)
    
    my_command = " " . join([job_dict['transformation'], job_dict['jobPars']])
    my_command = my_command.strip()
    my_command = coral_cdbserver_comm + my_command #+ '; ps -ax | grep COMPASS_wrapper_mpi.py | wc -l'
    logger.debug("Going to launch: {0}" . format(my_command))
    payloadstdout = open(payload_stdout_file, "w")
    payloadstderr = open(payload_stderr_file, "w")
    
    job.state = 'running'
    work_report["jobStatus"] = job.state
    start_time = time.asctime(time.localtime(time.time()))
    job.startTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    publish_work_report(work_report, workerAttributesFile)
    
    stime = time.time()
    t0 = os.times()
    exit_code = subprocess.call(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
    t1 = os.times()
    exetime = time.time() - stime
    end_time = time.asctime(time.localtime(time.time()))
    t = map(lambda x, y: x - y, t1, t0)
    t_tot = reduce(lambda x, y: x + y, t[2:3])
    job.endTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    payloadstdout.close()
    payloadstderr.close()
    if exit_code == 0:
        job.state = 'finished'
    else:
        job.state = 'failed'
    job.exitcode = exit_code

    work_report["endTime"] = job.endTime
    work_report["jobStatus"] = job.state
    work_report["cpuConsumptionTime"] = t_tot
    work_report["transExitCode"] = job.exitcode
    
    logger.info("Payload exit code: {0} JobID: {1}" . format(exit_code, job_id))
    logger.info("CPU comsumption time: {0} JobID: {1}" . format(t_tot, job_id))
    logger.info("Start time: {0} JobID: {1}" . format(start_time, job_id))
    logger.info("End time: {0} JobID: {1}" . format(end_time, job_id))
    logger.info("Execution time: {0} sec. JobID: {1}" . format(exetime, job_id))
    logger.debug("Job report start time: {0}" . format(job.startTime))
    logger.debug("Job report end time: {0}" . format(job.endTime))
    
    # Fill in payload attributes
    payload_report = {}
    payload_report = interpretPayloadStds(job, payload_stdout_file, payload_stderr_file)
    work_report.update(payload_report)
    
    dst_file = os.path.join(worker_communication_point, payload_report_file)    
    with open(dst_file, 'w') as job_report_outfile:
        json.dump(work_report, job_report_outfile)
    
#    copy_jobreport(job_working_dir, worker_communication_point, payload_report_file, workerAttributesFile)
    
    # log file not produced (yet)
    protectedfiles = job.output_files.keys()
    if job.log_file in protectedfiles:
        protectedfiles.remove(job.log_file)
    else:
        logger.info("Log files were not declared")
     
    logger.info("protected_files: {0}".format(job.output_files.keys()))
#     
#     cleanup_start = time.time()
#     logger.info("Cleanup of working directory")
#     protectedfiles.extend([workerAttributesFile, StageOutnFile])
#     removeRedundantFiles(job_working_dir, protectedfiles)
#     cleanup_time = time.time() - cleanup_start
#     logger.info("Cleanup took: {0} sec" . format(cleanup_time))
    res = packlogs(job_working_dir, protectedfiles, job.log_file)
    if res > 0:
        job.state = 'failed'
        work_report['pilotErrorCode'] = 1164  # Let's take this as closed one
        work_report['jobStatus'] = job.state
        main_exit(0, work_report, workerAttributesFile)
    
    # Copy of output to shared FS for stageout
    if not job_working_dir == worker_communication_point:
        cp_start = time.time()
        for outfile in job.output_files.keys():
            if os.path.exists(outfile):
                shutil.copyfile(os.path.join(job_working_dir, outfile), os.path.join(worker_communication_point, outfile))
            time.sleep(1)
        os.chdir(worker_communication_point)
        cp_time = time.time() - cp_start
        logger.info("Copy of outputs took: {0} sec.".format(cp_time))
    
    logger.info("Declare stage-out")
    out_file_report = {}
    out_file_report[job_id] = []

    for outfile in job.output_files.keys():
        logger.debug("File {} will be checked and declared for stage out" . format(outfile))
        if os.path.exists(outfile):
            file_desc = {}
            if outfile == job.log_file:
                file_desc['type'] = 'log'
            else:
                file_desc['type'] = 'output'
            file_desc['path'] = os.path.abspath(outfile)
            file_desc['fsize'] = os.path.getsize(outfile)
            out_file_report[job_id].append(file_desc)
        else:
            logger.info("Expected output file {0} missed. Job {1} will be failed" . format(outfile, job_id))
            job.state = 'failed'

    if out_file_report[job_id]:
        with open(StageOutnFile, 'w') as stageoutfile:
            json.dump(out_file_report, stageoutfile)
        logger.debug('Stageout declared in: {0}' . format(StageOutnFile))
        logger.debug('Report for stageout: {0}' . format(out_file_report))
    
    report = open("rank_report.txt", "w")
    report.write("cpuConsumptionTime: {0}\n" . format(t_tot))
    report.write("exitCode: {0}" . format(exit_code))
    report.close()
    
    logger.info("All done")
    logger.debug("Final report: {0}" . format(work_report))
        
    main_exit(0, work_report, workerAttributesFile)

if __name__ == "__main__":
    sys.exit(main())
