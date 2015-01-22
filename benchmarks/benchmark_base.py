from pprint import pprint

from ordereddict import OrderedDict

import itertools

import exporters

class ExecutorBase(object):

    def __call__(self, exe, args, opts):
        raise NotImplemented

class ExecutionWrapper(object):
    def __init__(self):
        self._exe = None
        self._args = None

    def __call__(self, exe, args, opts):
        return self._executor(self._exe, "%s %s %s" % (self._args, exe, args), opts)


class BenchmarkBase(object):
    def __init__(self, name, exporter=None, executor=None, enabled=True):
        self._enabled = enabled
        self._exe = None
        self._args = None
        self._hostset = None
        self._executor = executor
        self._name = name
        self._exporter = exporter

    def __call__(self):
        for hosts in self._hostset:
            output = self._executor(self.exe, self.args, hosts)
            data = self.filter(output)
            self.export(data)

    def filter(self):
        raise NotImplemented

    @property
    def executor(self):
        return self._executor

    @executor.setter
    def executor(self, value):
        self._executor = value

    @property
    def hostset(self):
        return self._hostset

    @hostset.setter
    def hostset(self, host_list):
        self._hostset = (h for h in host_list)

    def header(self):
        pass

    def export(self, data):
        if self._exporter is None:
            print data
        else:
            self._exporter.export(data)

    @property
    def name(self):
        return self._name

    @property
    def exe(self):
        return self._exe

    @property
    def args(self):
        return self._args

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        self._enabled = value



#######################################################


class Ping(BenchmarkBase):

    def __init__(self, name, exporter=None):
        super(Ping, self).__init__(name=name, exporter=exporter)
        self._exe = "ping"
        self._args = "-q -c 100 -A %(dsthost)s"
	self.filter = lambda x: [ OrderedDict(zip(['min','avg','max','mdev'],str.splitlines(x)[4].split()[3].split('/'))) ]

    def header(self, trial, hosts):
        return OrderedDict([("trial", trial), ("srchost", hosts[0]), ("dsthost", hosts[1])])

    @BenchmarkBase.hostset.setter
    def hostset(self, host_list):
        self._hostset = itertools.permutations(host_list, 2)



######################################################

class MpiEth0TcpWrapper(ExecutionWrapper):

    def __init__(self):
        super(MpiEth0TcpWrapper, self).__init__()
        self._exe = "mpirun"
        self._args = "-H %s --mca btl tcp,self --mca btl_tcp_if_include eth0"

    def __call__(self, exe, args, host_list):
        return self._executor(self._exe, self._args % ",".join(host_list) + " %s %s" % (exe, args), (host_list))


class Sh(ExecutorBase):

    def __call__(self, exe, args, opts):
       cmd = sh.Command(sh.which(exe))
       return cmd(str.split(args % opts)).stdout





