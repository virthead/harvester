from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

import uuid
import shutil
import os

# logger
baseLogger = core_utils.setup_logger('cp_compass_stager_hpc')


class cpCompasStagerHPC(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)
        self.queue_config_mapper = QueueConfigMapper()
        
    # check status
    def check_stage_out_status(self, jobspec):
        """Check the status of stage-out procedure. If staging-out is done synchronously in trigger_stage_out
        this method should always return True.
        Output files are available through jobspec.get_outfile_specs(skip_done=False) which gives
        a list of FileSpecs not yet done.
        FileSpec.attemptNr shows how many times the transfer was checked for the file.
        If the file was successfully transferred, status should be set to 'finished'.
        Or 'failed', if the file failed to be transferred. Once files are set to 'finished' or 'failed',
        jobspec.get_outfile_specs(skip_done=False) ignores them.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: transfer success, False: fatal transfer failure,
                 None: on-going or temporary failure) and error dialog
        :rtype: (bool, string)
        """
        
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='check_stage_out_status')
        tmpLog.debug('start')
        
#         for fileSpec in jobspec.get_output_file_specs(skip_done=True):
#             fileSpec.status = 'finished'
        return True, ''

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        """Trigger the stage-out procedure for the job.
        Output files are available through jobspec.get_outfile_specs(skip_done=False) which gives
        a list of FileSpecs not yet done.
        FileSpec.attemptNr shows how many times transfer was tried for the file so far.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: success, False: fatal failure, None: temporary failure)
                 and error dialog
        :rtype: (bool, string)
        """
        
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='check_stage_out_status')
        tmpLog.debug('start')
        allChecked = True
        ErrMsg = 'These files failed to upload: '
        
        tmpLog.debug('Getting seprodpath from queue_config')
        queue_config = self.queue_config_mapper.get_queue(self.queueName)
        
        tmpLog.debug('Requesting full spec of the job {0}' . format(jobspec.PandaID))
        proxy = DBProxy()
        jobSpec_full = proxy.get_job(jobspec.PandaID)
        
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            destination = queue_config.seprodpath
            filename = fileSpec.lfn
            
            se_path = ''
            sw_path = ''
            prod_name = ''
            prodSlt = ''
            TMPMDSTFILE = ''
            TMPHISTFILE = ''
            EVTDUMPFILE = ''
            MERGEDMDSTFILE = ''
            MERGEDHISTFILE = ''
            MERGEDDUMPFILE = ''
            
            if not ".log.tgz" in fileSpec.lfn:
                tmpLog.debug('Getting sw path, name and hist filename from jobPars')
                sw_prefix, sw_path, prod_name, prodSlt, TMPMDSTFILE, TMPHISTFILE, EVTDUMPFILE, MERGEDMDSTFILE, MERGEDHISTFILE, MERGEDDUMPFILE, PRODSOFT, MCGENFILEOUT = self.getSWPathAndNameAndFilename(jobSpec_full.jobParams['jobPars'])
                
                tmpLog.debug('sw_prefix: {0}' . format(sw_prefix))
                tmpLog.debug('sw_path: {0}' . format(sw_path))
                tmpLog.debug('prod_name: {0}' . format(prod_name))
                tmpLog.debug('prodSlt: {0}' . format(prodSlt))
                tmpLog.debug('TMPMDSTFILE: {0}' . format(TMPMDSTFILE))
                tmpLog.debug('TMPHISTFILE: {0}' . format(TMPHISTFILE))
                tmpLog.debug('EVTDUMPFILE: {0}' . format(EVTDUMPFILE))
                tmpLog.debug('MERGEDMDSTFILE: {0}' . format(MERGEDMDSTFILE))
                tmpLog.debug('MERGEDHISTFILE: {0}' . format(MERGEDHISTFILE))
                tmpLog.debug('MERGEDDUMPFILE: {0}' . format(MERGEDDUMPFILE))
                tmpLog.debug('PRODSOFT: {0}' . format(PRODSOFT))
                tmpLog.debug('MCGENFILEOUT: {0}' . format(MCGENFILEOUT))
                           
                # prod
                if fileSpec.lfn == TMPMDSTFILE :
                    se_path = sw_prefix + sw_path + prod_name + '/mDST.chunks'
                if fileSpec.lfn == TMPHISTFILE:
                    se_path = sw_prefix + sw_path + prod_name + '/TRAFDIC'
                if fileSpec.lfn == "testevtdump.raw":
                    se_path = sw_prefix + sw_path + prod_name + '/evtdump/slot' + prodSlt
                    filename = EVTDUMPFILE
                if fileSpec.lfn == "payload_stdout.out.gz":
                    se_path = sw_prefix + sw_path + PRODSOFT + '/logFiles'
                    filename = prod_name + '.' + TMPHISTFILE.replace('.root', '.stdout.gz')
                if fileSpec.lfn == "payload_stderr.out.gz":
                    se_path = sw_prefix + sw_path + PRODSOFT + '/logFiles'
                    filename = prod_name + '.' + TMPHISTFILE.replace('.root', '.stderr.gz')
                                
                # merge
                if fileSpec.lfn == MERGEDMDSTFILE :
                    se_path = sw_prefix + sw_path + prod_name + '/mDST'
                if fileSpec.lfn == MERGEDHISTFILE:
                    se_path = sw_prefix + sw_path + prod_name + '/histos'
                if fileSpec.lfn == MERGEDDUMPFILE:
                    se_path = sw_prefix + sw_path + prod_name + '/mergedDump/slot' + prodSlt
                
                # mc generation
                if fileSpec.lfn == MCGENFILEOUT:
                    se_path = sw_prefix + '/mc/' + sw_path + PRODSOFT + '/mcgen'
                    filename = MCGENFILEOUT
                                
                destination = se_path
                
            surl = "{0}/{1}" . format(destination, filename)
            dst_gpfn = "{0}/{1}" . format(destination, filename)
            lfcdir = destination
            
            tmpLog.debug('fileSpec.path = {0}' . format(fileSpec.path))
            tmpLog.debug('SURL = {0}' . format(surl))
            tmpLog.debug('dst_gpfn = {0}' . format(dst_gpfn))
            tmpLog.debug('lfcdir = {0}' . format(lfcdir))
            
            tmpLog.debug('Create if does not exist {0}' . format(lfcdir))
            if not os.path.exists(lfcdir):
                os.makedirs(lfcdir)
            
            tmpLog.debug('Copy {0} to {1}' . format(fileSpec.path, dst_gpfn))
            shutil.copyfile(fileSpec.path, dst_gpfn)
            if os.path.exists(dst_gpfn):
                fileSpec.status = 'finished'
            else:
                fileSpec.status = 'failed'
                allChecked = False
                ErrMsg += '{0} ' . format(fileSpec.lfn)
            
            # force update
            fileSpec.force_update('status')
            
            tmpLog.debug('Status of file {0} is {1}' . format(fileSpec.path, fileSpec.status))
            
        del jobSpec_full
        
        tmpLog.debug('done')
        
        if allChecked:
            return True, ''
        else:
            return False, ErrMsg

    def getSWPathAndNameAndFilename(self, jobPars):
        """ Get COMPASS_SW_PATH and COMPASS_PROD_NAME from JobPars """
        
        a = jobPars.find('COMPASS_SW_PREFIX')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        sw_prefix = d[d.find('=') + 1:]
        
        a = jobPars.find('COMPASS_SW_PATH')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        sw_path = d[d.find('=') + 1:]

        a = jobPars.find('COMPASS_PROD_NAME')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        prod_name = d[d.find('=') + 1:]
        
        a = jobPars.find('prodSlt')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        prodSlt = d[d.find('=') + 1:]
        
        a = jobPars.find('TMPMDSTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        TMPMDSTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('TMPHISTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        TMPHISTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('EVTDUMPFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        EVTDUMPFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('MERGEDMDSTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        MERGEDMDSTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('MERGEDHISTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        MERGEDHISTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('MERGEDDUMPFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        MERGEDDUMPFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('PRODSOFT')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        PRODSOFT = d[d.find('=') + 1:]
        
        a = jobPars.find('MCGENFILEOUT')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        MCGENFILEOUT = d[d.find('=') + 1:]
        
        return sw_prefix, sw_path, prod_name, prodSlt, TMPMDSTFILE, TMPHISTFILE, EVTDUMPFILE, MERGEDMDSTFILE, MERGEDHISTFILE, MERGEDDUMPFILE, PRODSOFT, MCGENFILEOUT

    # zip output files
    def zip_output(self, jobspec):
        return self.simple_zip_output(jobspec, tmpLog)

    # asynchronous zip output
    def async_zip_output(self, jobspec):
        return True, ''

    # post zipping
    def post_zip_output(self, jobspec):
        return True, ''
