import json
import re
try:
    import subprocess32 as subprocess
except:
    import subprocess
import arc

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc import arc_utils

# logger
baselogger = core_utils.setup_logger()


class ARCMonitor(PluginBase):
    '''Monitor for ARC CE plugin'''

    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        # Credential dictionary role: proxy file
        self.certs = dict(zip([r.split('=')[1] for r in list(harvester_config.credmanager.voms)],
                              list(harvester_config.credmanager.outCertFile)))
        self.cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)


    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workspec in workspec_list:

            # make logger
            arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
            tmplog = arclog.log
            tmplog.info("checking worker id {0}".format(workspec.workerID))
            (job, modtime, proxyrole) = arc_utils.workspec2arcjob(workspec)

            # Set certificate
            userconfig = arc.UserConfig(self.cred_type)
            try:
                userconfig.ProxyPath(str(self.certs[proxyrole]))
            except:
                tmplog.error("Job {0}: no proxy found with role {1}".format(job.JobID, proxyrole))
                retList.append((workspec.status, ''))
                continue

            job_supervisor = arc.JobSupervisor(userconfig, [job])
            job_supervisor.Update()

            jobsupdated = job_supervisor.GetAllJobs()
            jobsnotupdated = job_supervisor.GetIDsNotProcessed()

            for updatedjob in jobsupdated:
                if updatedjob.JobID in jobsnotupdated:
                    tmplog.error("Failed to find information on {0}".format(updatedjob.JobID))
                    # If missing for too long (2 days), mark as lost
                    if arc.Time() - modtime > arc.Period(172800):
                        tmplog.error("Job {0} missing for more than 2 days, marking as lost".format(updatedjob.JobID))
                        retList.append((workspec.ST_failed, ''))
                    else:
                        retList.append((workspec.status, ''))
                    continue
                
                # Convert arc state to WorkSpec state
                arcstatus = updatedjob.State
                newstatus = WorkSpec.ST_submitted
                if arcstatus == arc.JobState.RUNNING or \
                   arcstatus == arc.JobState.FINISHING:
                    newstatus = WorkSpec.ST_running
                elif arcstatus == arc.JobState.FINISHED:
                    if updatedjob.ExitCode == -1:
                        # Missing exit code, but assume success
                        tmplog.warning("Job {0} FINISHED but has missing exit code, setting to zero".format(updatedjob.JobID))
                        updatedjob.ExitCode = 0
                    newstatus = WorkSpec.ST_finished
                elif arcstatus == arc.JobState.FAILED:
                    newstatus = WorkSpec.ST_failed
                    tmplog.info("Job {0} failed: {1}".format(updatedjob.JobID, ";".join([joberr for joberr in updatedjob.Error])))
                elif arcstatus == arc.JobState.KILLED:
                    newstatus = WorkSpec.ST_cancelled
                elif arcstatus == arc.JobState.DELETED or \
                     arcstatus == arc.JobState.OTHER:
                    # unexpected
                    newstatus = WorkSpec.ST_failed
                # Not covered: arc.JobState.HOLD. Maybe need a post-run state in
                # harvester, also to cover FINISHING

                # compare strings here to get around limitations of JobState API
                if job.State.GetGeneralState() == updatedjob.State.GetGeneralState():
                    tmplog.debug("Job {0} still in state {1}".format(job.JobID, job.State.GetGeneralState()))
                    retList.append((newstatus, ''))
                    continue

                tmplog.info("Job {0}: {1} -> {2} ({3})".format(job.JobID, job.State.GetGeneralState(),
                                                               updatedjob.State.GetGeneralState(), 
                                                               updatedjob.State.GetSpecificState()))

                arc_utils.arcjob2workspec(updatedjob, workspec)
                # Have to force update to change info in DB
                workspec.force_update('workAttributes')
                tmplog.debug("batchStatus {0} -> workerStatus {1}".format(arcstatus.GetGeneralState(), newstatus))
                retList.append((newstatus, ''))

        return True, retList

def test(jobid):
    '''Test checking status'''
    from pandaharvester.harvestercore.work_spec import WorkSpec
    wspec = WorkSpec()
    wspec.batchID = jobid #"gsiftp://pikolit.ijs.si:2811/jobs/HtgKDmtCe7qn4J8tmqCBXHLnABFKDmABFKDmBcGKDmABFKDm4NCTCn"
    workAttributes = {"arcjob": {}}
    workAttributes["arcjob"]["JobID"] = wspec.batchID
    workAttributes["arcjob"]["JobStatusURL"] = "ldap://{0}:2135/mds-vo-name=local,o=grid??sub?(nordugrid-job-globalid={1})".format(urlparse.urlparse(jobid).netloc, jobid)
    workAttributes["arcjob"]["JobStatusInterfaceName"] = "org.nordugrid.ldapng"
    jobmanagementurl = arc.URL(wspec.batchID)
    jobmanagementurl.ChangePath("/jobs")
    workAttributes["arcjob"]["JobManagementURL"] = jobmanagementurl.str()
    workAttributes["arcjob"]["JobManagementInterfaceName"] = "org.nordugrid.gridftpjob"
    
    wspec.workAttributes = workAttributes
    print wspec.workAttributes

    monitor = ARCMonitor()
    print monitor.check_workers([wspec])

if __name__ == "__main__":
    import time, sys, urlparse
    if len(sys.argv) != 2:
        print "Please give ARC job id"
        sys.exit(1)
    while True:
        test(sys.argv[1])
        time.sleep(2)
