#!/usr/bin/python

from pprint import pprint

from ordereddict import OrderedDict

import csv
import itertools
import sh
import time

hosts = ["hd0032", "hd0033", "hd0034", "hd0035"]

tests = {
	"ping" : {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 ping -q -c 100 -A %(dsthost)s",
		"filter": lambda x: [ OrderedDict(zip(['min','avg','max','mdev'],str.splitlines(x)[4].split()[3].split('/'))) ],
		"hostset": itertools.permutations(hosts, 2)},
	"bwctl": {
		"enabled": True, 
		"exe": "bwctl",
		"args": "-q -x -y c -T iperf -c %(dsthost)s -s %(srchost)s",
#		"filter": lambda x: str.splitlines(x)[2:9:6]},
		"filter": lambda x: [ OrderedDict(zip( ['timestamp','src','sport','dst','dport','id','interval','size','bw'], line.split(',') )) for line in str.splitlines(x)[2:9:6]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_latency": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/pt2pt/osu_latency", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_multi_lat": {
# Need to figure out how to deal with multihost runs with the current code.
		"enabled": False, 
		"exe": "mpirun",
		"args": "-H %(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 mpitests-osu_multilat",
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, len(hosts) - (len(hosts) % 2))},
	"osu_bw": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/pt2pt/osu_bw",
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "bw"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.permutations(hosts, 2)},
	"osu_allgather": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_allgather", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_allgatherv": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_allgatherv", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_allreduce": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_allreduce", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_alltoall": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_alltoall", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_alltoallv": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_alltoallv", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_barrier": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_barrier", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_bcast": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_bcast", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_gather": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_gather", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_gatherv": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_gatherv", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_reduce": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_reduce", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_reduce_scatter": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_reduce_scatter", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_scatter": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_scatter", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
	"osu_scatterv": {
		"enabled": True, 
		"exe": "mpirun",
		"args": "-H %(srchost)s,%(dsthost)s --mca btl tcp,self --mca btl_tcp_if_include eth0 /root/osu-micro-benchmarks/mpi/collective/osu_scatterv", 
#		"filter": lambda x: dict([line.split() for line in x.splitlines()[3::]])},
		"filter": lambda x: [ OrderedDict(zip(["size", "latency"], line.split())) for line in x.splitlines()[3::]],
		"hostset": itertools.combinations(hosts, 2)},
}


for test in tests:
	if tests[test]["enabled"]:
		with open("%s.csv" % test, 'w') as csvfile:
			writer = None
			trial = 0
			pprint(test)
			
			for hostset in tests[test]["hostset"]:
				trial += 1
				pprint(hostset)
				header = OrderedDict([("trial", trial), ("srchost", hostset[0]), ("dsthost", hostset[1])])

				args = str.split(tests[test]["args"] % header) #{"srchost": hostset[0], "dsthost": hostset[1]})
				cmd = sh.Command(sh.which(tests[test]["exe"]))
				o = cmd(args)
				if o.exit_code != 0:
					pass #sh automatically raises an exception 
				if "filter" in tests[test]:
					data = [ OrderedDict(header, **result) for result in tests[test]["filter"](o.stdout) ]
	#				pprint(data)
					if writer is None:
						writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
						writer.writerow(dict((fn,fn) for fn in writer.fieldnames))
						
	#					writer.writeheader()
					writer.writerows(data)
				else:
					print o.stdout


