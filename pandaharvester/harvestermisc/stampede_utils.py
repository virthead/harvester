from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec as ws

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

import datetime

# logger
baseLogger = core_utils.setup_logger('stampede_utils')

class StampedeUtils(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        tmpLog = self.make_logger(baseLogger, method_name='__init__')
        tmpLog.info("Stampede utils initiated")

    def get_batchjob_info(self, batchid):
        """
        Collect job info from scheduler
        :param batchid:
        :return res - dictonary with job state and some timing:
        """
        """
        :param batchid: 
        :return: 
        """
        tmpLog = self.make_logger(baseLogger, method_name='get_batchjob_info')
        res = {}
        tmpLog.info("Collect job info for batchid {}".format(batchid))
        info_dict = self.get_slurmjob_info(batchid)
        tmpLog.info("Got: {0}".format(info_dict))
        if info_dict:
            tmpLog.debug("Translate results")
            res['status'] = self.translate_status(info_dict['state'])
            res['nativeStatus'] = info_dict['state']
            res['nativeExitCode'] = info_dict['exit_code']
            res['nativeExitMsg'] = ''
            res['start_time'] = info_dict['start_time']
            res['finish_time'] = info_dict['finish_time']
        tmpLog.info("Collected job info: {0}".format(res))
        return res

    def get_slurmjob_info(self, batchid):
        """
        Parsing of checkjob output to get job state, exit code, start time, finish time (if available)
        :return job_info dictonary:
        """
        tmpLog = self.make_logger(baseLogger, method_name='get_slurmjob_info')

        job_info = {
            'state': "",
            'exit_code': None,
            'queued_time': None,
            'start_time': None,
            'finish_time': None
        }

        cmd = 'sacct -j {0} --format=start,end,state,exitcode --parsable2'.format(batchid)
        p = subprocess.Popen(cmd.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        # check return code
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        checkjob_str = ""
        if retCode == 0:
            checkjob_str = stdOut
        else:
            tmpLog.info("sacct -j failed with errcode: {0}\nstdout:{1}\nstderr:{2}".format(retCode, stdOut, stdErr))
            return {}
        if checkjob_str:
            checkjob_out = checkjob_str.splitlines()
            for l in checkjob_out:
                tmpLog.debug(l)
            vals = l.split('|')
            job_info['start_time'] = vals[0]
            job_info['finish_time'] = vals[1]
            job_info['state'] = vals[2]
            job_info['exit_code'] = vals[3].split(':')[0]
        tmpLog.debug("checkjob parsing results: {0}".format(job_info))
        return job_info

    def translate_status(self, status):
        """
        Slurm status to worker status
        :param status:
        :return:
        """
        submited = ['PENDING']
        running = ['RUNNING', 'SUSPENDED']
        finished = ['COMPLETED']
        cancelled = ['CANCELLED']
        failed = ['FAILED']
        status = status.lower()
        if status in submited:
            return ws.ST_submitted
        elif status in running:
            return ws.ST_running
        elif status in finished:
            return ws.ST_finished
        elif status in cancelled:
            return ws.ST_cancelled
        elif status in failed:
            return ws.ST_failed
        else:
            return ws.ST_finished
    
    def get_resources(self):
        """
        Function to provide number of nodes with walltime limit to worker maker
        :return: 
        nodes: integer  
        walltime: integer, seconds
        """

        tmpLog = self.make_logger(baseLogger, method_name='get_resources')

        tmpLog.info("Nodes: {0} Walltime: {1}".format(self.nNodes, self.walltimelimit))
    
        return self.nNodes, self.walltimelimit
