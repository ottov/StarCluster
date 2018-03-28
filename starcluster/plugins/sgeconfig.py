from starcluster import clustersetup
#from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
import re

local_pe_attrs = {
    'pe_name':            'DJ',
    'slots':              '99999',
    'user_lists':         'NONE',
    'xuser_lists':        'NONE',
    'start_proc_args':    '/bin/true',
    'stop_proc_args':     '/bin/true',
    'allocation_rule':    '$pe_slots',
    'control_slaves':     'FALSE',
    'job_is_first_task':  'TRUE',
    'urgency_slots':      'min',
    'accounting_summary': 'TRUE',
}
orte_pe_attrs = {
    'pe_name':            'orte',
    'slots':              '99999',
    'user_lists':         'NONE',
    'xuser_lists':        'NONE',
    'start_proc_args':    '/bin/true',
    'stop_proc_args':     '/bin/true',
    'allocation_rule':    '$pe_slots',
    'control_slaves':     'FALSE',
    'job_is_first_task':  'TRUE',
    'urgency_slots':      'min',
    'accounting_summary': 'TRUE',
}

global_attrs = {
    'shell_start_mode':   'unix_behavior',
    'login_shells':       'sh,ksh,csh,tcsh',
    'qmaster_params':     'ENABLE_RESCHEDULE_SLAVE=1',
    'load_report_time':   '00:00:10',
    'max_unheard':        '00:16:00',
    'reschedule_unknown': '00:10:00',
    'reporting_params':   'accounting=true reporting=false flush_time=00:00:15 joblog=false sharelog=00:00:00',
}

sched_attrs = {
    'schedule_interval': '0:0:05',
    'schedd_job_info': 'true',
    'weight_waiting_time': '1.000000',
    'job_load_adjustments': 'np_load_avg=0.90',
    'usage_weight_list': 'cpu=1.000000,mem=1.000000,io=0.100000',
    'reprioritize_interval': '0:1:0',
}

queue_attrs = {
   'qtype':                'BATCH INTERACTIVE',
   'pe_list':              'make DJ orte',
   'rerun':                'TRUE',
   'tmpdir':               '/tmp',
   'shell':                '/bin/sh',
   'shell_start_mode':     'unix_behavior',
}

complex_attrs={
  'h_vmem':'              h_vmem     MEMORY      <=    YES         YES         6G        0',
  }

class SGEConfig(clustersetup.DefaultClusterSetup):
    """Apply additional configuration to a running SGE instance.
       This plugin is mean to run after the built-in SGE plugin of StarCluster.
    """

    def __init__(self):

        super(SGEConfig, self).__init__()
        pass

    def run (self, nodes, master, user, user_shell, volumes):
        self._nodes = nodes
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes

        sge = SGE(master)
        if not sge.is_installed():
            log.error("SGE is not installed on this AMI, skipping...")
            return

        self._setup_nfs()

        log.info("Applying additional SGE configuration...")
        sge.create_or_update_pe('DJ', local_pe_attrs, ['all.q'])
        sge.create_or_update_pe('orte', orte_pe_attrs, ['all.q'])
        sge.update_global_config(global_attrs)

        sge.set_mem_on_all_nodes(master,nodes)
        sge.update_scheduler_config(sched_attrs)
        sge.update_complex_config(complex_attrs)
        sge.update_queue_config(queue_attrs)

        log.info(nodes)

        sge.cleanup()

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        # This code configures a specific user and group id for the user that
        # you wish to run the jobs under (if it's not sgeadmin).
        # Enable and customize as needed
        #mgroup = 'mygroup'
        #myuser = 'myuser'
        #node.ssh.execute('addgroup --system --gid 1014 %s' % mygroup)
        #node.ssh.execute('adduser --gid 1014 --uid 1014 %s --system' % myuser)
        self._nodes = nodes
        self._node = node
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes

        sge = SGE(master)

        log.info("[sgeconfig-ov] -- in add node")
        #self._setup_nfs()
        log.info("Adding all NFS devices to %s" % node.alias)
        self._setup_nfs(nodes=[node])

        sge.set_mem_on_node(master,node)


        pass
    def _get_nfs_export_paths(self):
        master=self._master
        exports_file='/root/exports.list'
        export_paths = [] #'/home','/mnt/data']

        if self._master.ssh.isfile('/root/exports.list'):
            log.info("[sgeconfig-ov] Reading /root/exports.list")
            efile = master.ssh.remote_file(exports_file, 'r')
            exports = efile.read()
            efile.close()
            
            extra_exports = exports.strip().split('\n')

            for e in extra_exports:
              export_paths.append(e)
        else:
            export_paths = ['/home','/mnt/data', '/mnt/ssd1','/mnt/ssd2']
            log.info("[sgeconfig-ov] Creating /root/exports.list")
            efile = master.ssh.remote_file(exports_file, 'w')
            efile.write('\n'.join(export_paths))
            efile.close()
        
        log.info(export_paths)
        return export_paths
       
class SGE(object):
    def __init__(self, master):
        self.mssh = master.ssh
        self.cleanup_dirs = []
        self.master=master
        
    def is_installed(self):        
        return self.mssh.isdir("/opt/sge6-fresh")
        
        
    def cleanup(self):
        log.debug("Need to cleanup %s", self.cleanup_dirs)

    def exists_pe(self, pe_name):
        """Check if parallel environment exists"""
        spl = self.mssh.execute("qconf -spl")
        return pe_name in spl

    def create_or_update_pe(self, name, attrs, queues=None):
        """Create or update parallel environment with the specified attributes.
           Any attributes of an existing PE are replaced with the provided dict.
        """
        file = self._stage_attrs(name, attrs)

        if self.exists_pe(name):
            mode="M"
            verb = 'Updating'
        else:
            mode="A"
            verb = 'Creating'

        log.info("%s SGE parallel environment '%s'" % (verb, name))
        self.mssh.execute("qconf -{mode}p {file}".format(mode=mode, file=file))

        if queues:
            qs=','.join(queues)
            log.info("Adding parallel environment '%s' to queues '%s'", name, qs)
            self.mssh.execute('qconf -mattr queue pe_list "%s" %s' % (name, qs))
            self.mssh.execute('qconf -mattr queue rerun TRUE %s' % (qs) )

        #bSize = self.mssh.execute('free -b|grep "\-/+"|cut -d" " -f4')[0]
        #bSize = self.mssh.execute('free -b|grep -m1 -Po "\d+$"')[0]
        #bSize = self.mssh.execute('cat /proc/meminfo |grep MemTotal|awk "{print $2 * 1024;}"')[0]
        bSize = self.mssh.execute("cat /proc/meminfo |grep MemTotal|awk '{print $2 * 1210;}'")[0]
        #bSize = self.mssh.execute("cat /proc/meminfo |grep MemTotal|awk '{print $2 * 1925;}'")[0]
        bSize = int(bSize)
        log.info("Setting h_vmem limits on master to %d" % bSize)
        self.mssh.execute('qconf -rattr exechost complex_values h_vmem=%d %s' % (bSize,self.master.alias))

    def update_global_config(self, attrsDict):
        """Update global config with specified attributes."""
        dir=self._create_tmp_dir()
        file="{dir}/{name}".format(dir=dir, name='global')
        sed_cmd_template="s/^({key})(\s+)(.*)/\\1\\2{value}/"
        sed_cmd = ""
        for k,v in attrsDict.iteritems():
            frag = sed_cmd_template.format(key=k, value=re.escape(v))
            sed_cmd += ' -e "%s"' % frag

        self.mssh.execute("qconf -sconf global | sed -r %s > %s" % (sed_cmd, file))
        self.mssh.execute("qconf -Mconf %s" % file)
    def update_scheduler_config(self, attrsDict):
        """Update update_scheduler_config config with specified attributes."""
        dir=self._create_tmp_dir()
        file="{dir}/{name}".format(dir=dir, name='ssconf')
        sed_cmd_template="s/^({key})(\s+)(.*)/\\1\\2{value}/"
        sed_cmd = ""
        for k,v in attrsDict.iteritems():
            frag = sed_cmd_template.format(key=k, value=re.escape(v))
            sed_cmd += ' -e "%s"' % frag

        log.info("Updating from sched_attrs")
        self.mssh.execute("qconf -ssconf | sed -r %s > %s" % (sed_cmd, file))
        self.mssh.execute("qconf -Msconf %s" % file)
    def update_queue_config(self, attrsDict):
        """Update update_queue_config config with specified attributes."""
        dir=self._create_tmp_dir()
        file="{dir}/{name}".format(dir=dir, name='qu.all.q.conf')
        sed_cmd_template="s/^({key})(\s+)(.*)/\\1\\2{value}/"
        sed_cmd = ""
        for k,v in attrsDict.iteritems():
            frag = sed_cmd_template.format(key=k, value=re.escape(v))
            sed_cmd += ' -e "%s"' % frag

        log.info("Updating from queue_attrs")
        self.mssh.execute("qconf -sq all.q | sed -r %s > %s" % (sed_cmd, file))
        self.mssh.execute("qconf -Mq %s" % file)
    def update_complex_config(self, attrsDict):
        """Update update_complex_config config with specified attributes."""
        dir=self._create_tmp_dir()
        file="{dir}/{name}".format(dir=dir, name='complex')
        sed_cmd_template="s/^({key})(\s+)(.*)/\\1\\2{value}/"
        sed_cmd = ""
        for k,v in attrsDict.iteritems():
            frag = sed_cmd_template.format(key=k, value=re.escape(v))
            sed_cmd += ' -e "%s"' % frag

        log.info("Updating from complex_attrs")
        self.mssh.execute("qconf -sc | sed -r %s > %s" % (sed_cmd, file))
        self.mssh.execute("qconf -Mc %s" % file)
    def set_mem_on_node(self,master,node):

        #bSize = node.ssh.execute("free -b|grep Mem:|awk '{print $2}'")[0]
        bSize = node.ssh.execute("cat /proc/meminfo |grep MemTotal|awk '{print $2 * 1024;}'")[0]
        bSize = int(int(bSize) * 1.88)
        log.info("Setting h_vmem limits on %s to %d" % (node.alias, bSize))
        master.ssh.execute('qconf -rattr exechost complex_values h_vmem=%d %s' % (bSize,node.alias))

    def set_mem_on_all_nodes(self,master,nodes):

        for node in nodes:
          self.set_mem_on_node(master,node)

    def _stage_attrs(self, fileName, attrsDict):
        dir=self._create_tmp_dir()
        file="{dir}/{name}".format(dir=dir, name=fileName)
        log.debug("Checking for file %s", file)
        f = self.mssh.remote_file(file, mode="w")
        f.writelines(self._format_attrs(attrsDict))
        f.close()
        return file

    def _format_attrs(self, attrsDict):
        """Format dictionary of attributes into a list of lines in the sge_config format.
        """
        return ["%s\t\t\t%s\n" % (k,v) for k,v in attrsDict.iteritems()]

    def _create_tmp_dir(self):
        dir=self.mssh.execute("mktemp --tmpdir=/tmp --directory sgeconf.XXXXXXX")
        if not dir:
            raise Exception("Failed to create temp directory")
        #master.ssh.execute("ls /tmp" % dir)
        self.cleanup_dirs.append(dir)
        return dir[0]


