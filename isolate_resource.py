#!/usr/bin/python
import argparse
import ConfigParser
import logging
import math
import os
import subprocess
import time
from decimal import *
from logging.config import dictConfig

import yaml

nova_conf_path = '/etc/nova/nova.conf'
isolate_resource_conf_path = '/root/isolate_resource.conf'
astute_conf_path = '/etc/astute.yaml'
CGCONFIG_PATH = '/usr/lib/systemd/system/cgconfig.service'
CEPH_INIT_PATH = '/etc/init.d/ceph'
LOG_PATH = '/var/log/isolate_resource.log'


class ConfigParserForbid(ConfigParser.ConfigParser):
    def __init__(self, defaults=None):
        ConfigParser.ConfigParser.__init__(self, defaults=None)

    def optionxform(self, optionstr):
        return optionstr


def get_logger():
    if not os.path.isfile(LOG_PATH):
        os.mknod(LOG_PATH)
    dict_config = dict(
        version=1,
        formatters={
            'f': {'format':
                  '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
        },
        handlers={
            'StreamHandler': {'class': 'logging.StreamHandler',
                              'formatter': 'f',
                              'level': logging.INFO},
            'FileHandler': {
                'class': 'logging.FileHandler',
                'formatter': 'f',
                'level': logging.INFO,
                'filename': '%s' % LOG_PATH
            }
        },
        root={
                'handlers': ['StreamHandler', 'FileHandler'],
                'level': logging.DEBUG,
                },
    )
    dictConfig(dict_config)
    logger = logging.getLogger('isolate_resource')
    return logger


logger = get_logger()


def execute_command(args):
    """execute command"""
    s = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE)
    return s.communicate()


def get_numa_node_info(numa_info):
    """get numa node info"""
    return numa_info.split('\n')[0]


def get_node_cpu_info(node_info):
    """get node cpu infomation from node info"""
    node_info_1 = node_info.split('\n')
    node_cpus = []
    node_cpus_1 = []
    for value in node_info_1:
        if value.__contains__('cpus'):
            node_cpus_1.append(value.split(':')[1].strip())
    for value in node_cpus_1:
        cpus = value.split(' ')
        node_cpus.append(cpus)
    return node_cpus


def get_node_mem_info(node_info):
    """get node memory infomation from node info"""
    node_info_1 = node_info.split('\n')
    node_mems = []
    for value in node_info_1:
        if value.__contains__('size'):
            node_mems.append(value.split(':')[1].strip())
    node_mems = [int(node_mem.replace('MB', ' ').strip())
                 for node_mem in node_mems]
    return node_mems


def get_cpus_mems_by_osdId(
        cpus,
        mems,
        node_numa_info,
        cpu_use_rates,
        node_mem_info):
    mems = mems
    flag = False
    for index, value in enumerate(node_mem_info):
        if mems <= value:
            for index_, value_ in enumerate(cpu_use_rates[index]):
                if cpus <= value_[1]:
                    cpus_value = value_[0]
                    cpu_use_rates[index][index_] = (
                        value_[0],
                        (value_[1]*100-cpus*100)/100)
                    node_mem_info[index] -= mems
                    flag = True
                    break
        if flag:
            break
    return cpus_value, mems, cpu_use_rates, node_mem_info, index


def get_osd_ids():
    arg = 'ceph osd tree | grep osd \
        | awk \'{print $3}\' | sort -g'
    osd_ids_tuples = execute_command(arg)
    osd_ids = osd_ids_tuples[0].split('\n')[:-1]
    return osd_ids


def bytes_to_gb(size_bytes):
    if size_bytes == 0:
        return "0G"
    p = math.pow(1024, 3)
    s = round(size_bytes / p, 1)
    return s


def get_osd_mems_by_ids(parameters):
    osd_ids = get_osd_ids()
    osd_ids_cap = []
    osd_mems = []
    for index, value in enumerate(osd_ids):
        condition = "/var/lib/ceph/osd/ceph-" + value[4:]
        arg = 'lsblk -b | awk \'{if ($7 == \"' + \
            condition + '\") {print $4}}\''
        osd_ids_cap.append(execute_command(arg)[0].replace('\n', ''))
        if osd_ids_cap[index].__len__() == 0:
            osd_mems.append(0)
        else:
            mem_scale_osd = parameters.get("osd_cap_mems")
            osd_ids_cap[index] = bytes_to_gb(float(osd_ids_cap[index]))
            osd_mems.append(
                math.ceil(
                    osd_ids_cap[index]/int(mem_scale_osd.split(':')[0])
                    * int(mem_scale_osd.split(':')[1])))
        osd_ids[index] = osd_ids[index].replace('.', '')
    return osd_ids, osd_mems


def init_cpu_use_rate_by_node_cpus(node_cpus):
    cpu_use_rates = []
    for cpus in node_cpus:
        cpu_use_rate = []
        for cpu in cpus:
            cpu_use_rate.append((cpu, 1))
        cpu_use_rates.append(cpu_use_rate)
    return cpu_use_rates


def create_cgroup(osd, cpu, numa_node, memory):
    # create controller group
    args = []
    args.append('cgcreate -g cpuset:' + osd)
    args.append('cgcreate -g memory:' + osd)
    # set cpuset.cpus parameter
    args.append('cgset -r cpuset.cpus="' + cpu + '" ' + osd)
    # set cpuset.mems parameter
    args.append('cgset -r cpuset.mems=' + numa_node + ' ' + osd)
    # set memory.limit_in_bytes parameter
    args.append('cgset -r memory.limit_in_bytes='
                + str(memory) + 'M ' + osd)
    # set memory.swappiness parameter
    args.append('cgset -r memory.swappiness=0 ' + osd)
    # set memory.oom_control parameter
    args.append('cgset -r memory.oom_control=1 ' + osd)
    for arg in args:
        execute_command(arg)


def generate_config(*args, **kw):
    templete = ('group osd {\n\tmemory {'
                '\n\t\tmemory.limit_in_bytes = limit_in_bytes_value_M;'
                '\n\t\tmemory.soft_limit_in_bytes = '
                'soft_limit_in_bytes_valueM;'
                '\n\t\tmemory.swappiness = 0;'
                '\n\t\tmemory.oom_control = 1;\n\t}'
                '\n\tcpuset {\n\t\tcpuset.cpus = cpuset.cpus_value;'
                '\n\t\tcpuset.mems = cpuset.mems_value;\n\t}\n}\n')
    for item in kw.items():
        templete = templete.replace(item[0], item[1])
    return templete


def create_osd_cgroup(parameters):
    """create cgroup by sd_id"""
    arg = 'numactl --hardware | grep -E "node . size|cpus"'
    node_info = execute_command(arg)

    arg = 'numactl --hardware | awk -F \':\' \'{if($1 == "available") \
        {print $2}}\' | awk \'{print $1}\''
    numa_info = execute_command(arg)

    # get physical cpus
    node_cpu_info = get_node_cpu_info(node_info[0])
    cpu_use_rates = init_cpu_use_rate_by_node_cpus(node_cpu_info)
    # get physical memorys
    node_mem_info = get_node_mem_info(node_info[0])
    # get physical numa node number
    node_numa_info = get_numa_node_info(numa_info[0])

    osd_cpu = parameters.get('osd_cpu')
    osds, osd_mems = get_osd_mems_by_ids(parameters)
    templete = 'group . {\n\tmemory {\
        \n\t\tmemory.use_hierarchy = 0;\n\t}\n}\n'
    ratio = float(parameters['soft_ratio'])
    for index, value in enumerate(osds):
        if osd_mems[index] == 0:
            continue
        cpus, memory, cpu_use_rates,\
            node_mem_info, numa_node = get_cpus_mems_by_osdId(
                osd_cpu, osd_mems[index], node_numa_info,
                cpu_use_rates, node_mem_info)
        # create controller group
        create_cgroup(osds[index], cpus, str(numa_node),
                      int(math.ceil(memory)))
        dict_values = {
            'osd': osds[index],
            'limit_in_bytes_value_': str(int(math.ceil(memory))),
            'soft_limit_in_bytes_value': str(int(math.ceil(memory)
                                                 * ratio)),
            'cpuset.cpus_value': cpus,
            'cpuset.mems_value': str(numa_node)}
        templete += generate_config(**dict_values)
        logger.info('For {}, allocate {} CPUs,'
                    '{} MB memroy, '
                    'and the numa node is node{}.'.format(
                        osds[index],
                        cpus,
                        str(int(math.ceil(memory))),
                        str(numa_node)))
    file_path = '/tmp/cgconfig_ceph.conf'
    with open(file_path, 'w') as f:
        f.write(templete)
    # move to /etc/cgconfig.d file
    arg = 'mv -f /tmp/cgconfig_ceph.conf /etc/cgconfig.d/'
    execute_command(arg)
    return cpu_use_rates


def get_cpu_number():
    arg = 'lscpu | awk -F \':\' \'{if($1 == "CPU(s)") {print $2}}\''
    cpus_tuples = execute_command(arg)
    return int(cpus_tuples[0].decode("utf-8").strip())


def get_mems_for_check():
    # unit : M
    arg = 'numactl --hardware | grep -E "node . size|cpus"'
    node_info = execute_command(arg)
    node_mem = get_node_mem_info(node_info[0])
    return sum(node_mem)


def get_parameter():
    """get parameter by configruation"""
    cf = ConfigParser.ConfigParser()
    cf.read(isolate_resource_conf_path)
    parameters = {}
    options = cf.sections()
    opts = cf.options(options[0])
    for parameter in opts:
        parameters[parameter] = cf.get(options[0], parameter)
    return parameters


def get_roles():
    with open(astute_conf_path) as f:
        info_dict = yaml.load(f)
    uid = info_dict.get('uid')
    nodes = info_dict.get('nodes')
    roles = ''
    for node in nodes:
        if node.get('uid') == uid:
            roles = roles + ',' + node.get('role')
    roles = roles[1:]
    return roles


def check_resource_enough(parameters, fuel_vm_enable):
    mems = get_mems_for_check()
    osds, osd_mems = get_osd_mems_by_ids(parameters)
    roles = get_roles()
    need_mems_str = "os_memory"
    if fuel_vm_enable:
        need_mems_str += ",fuel_vm_memory"
    if roles.__contains__('controller'):
        need_mems_str = need_mems_str + ',controller_memory'

    if roles.__contains__('mongo'):
        need_mems_str = need_mems_str + ',mongo_memory'
    need_mems = 0.0
    for item in parameters.items():
        if need_mems_str.__contains__(item[0]):
            need_mems += float(item[1])*1024
    for value in osd_mems:
        need_mems += float(value)
    return mems, need_mems, osd_mems


def cal_cpu_over_ratio(compute_mems, parameters, fuel_vm_enable):
    cpus = get_cpu_number()
    osds, osd_mems = get_osd_mems_by_ids(parameters)
    roles = get_roles()
    need_cpu_str = "os_cpu"
    if fuel_vm_enable:
        need_cpu_str += ",fuel_vm_cpu"
    if roles.__contains__('controller'):
        need_cpu_str = need_cpu_str + ',controller_cpu'

    if roles.__contains__('mongo'):
        need_cpu_str = need_cpu_str + ',mongo_cpu'
    need_cpus = 0
    for item in parameters.items():
        if need_cpu_str.__contains__(item[0]):
            need_cpus += int(item[1])
    other_cpus = need_cpus
    for osd_mem in osd_mems:
        if osd_mem > 0:
            need_cpus += 1
    compute_cpus = math.ceil(math.ceil(compute_mems/1024)/2)
    need_cpus += compute_cpus
    cpu_over_ratio = math.ceil(Decimal(need_cpus)/Decimal(cpus))
    return cpu_over_ratio, other_cpus


def restart_service(osd_mems):
    # restart service about cgconfig,nova-conputer and ceph
    # frist restart cgconfig.service
    check_arg = 'systemctl is-active cgconfig.service'
    arg = 'systemctl restart cgconfig.service'
    execute_command(arg)
    count = 0
    while count < 3:
        is_active = execute_command(check_arg)[0].split('\n')[0]
        if is_active.__eq__('active'):
            break
        else:
            count += 1
            time.sleep(1)
    if count < 3:
        # restart openstack-nova-computer service
        arg = 'systemctl restart openstack-nova-compute.service'
        execute_command(arg)
        # restart ceph service
        osds = get_osd_ids()
        for index, osd in enumerate(osds):
            if osd_mems[index] > 0:
                # restart this osd service
                arg = '/etc/init.d/ceph restart ' + osd
                execute_command(arg)


def get_options():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--auto-restart",
        help="restart service about cgconfig,nova-conputer and ceph",
        action="store_true")
    parser.add_argument("--fuel-vm-enable",
                        help="Whether you need to reserve resources for fule",
                        action="store_true")
    args = parser.parse_args()
    return args


def modify_nova_config(dict_compute):
    file_path = nova_conf_path
    with open(file_path) as f:
        context = f.read()
    begin = 0
    end = 0
    context_ = context
    for key in dict_compute:
        if context_.__contains__(key):
            begin = context_.index(key)
            if key == 'vcpu_pin_set' and context_.count(key) > 1:
                begin = context_.find(key, begin+1)
            end = begin
            while end < len(context_):
                if context_[end] == '\n':
                    break
                end += 1
            if key == 'vcpu_pin_set':
                context_ = \
                    context_.replace(context_[begin:end], key +
                                     ' = "' + dict_compute.get(key) + '"')
            else:
                context_ = \
                    context_.replace(context_[begin:end], key +
                                     ' = ' + dict_compute.get(key))
            i = begin - 1
            while i > 0:
                if context_[i] == '\n':
                    break
                i -= 1
            if i != begin - 1:
                context_ = context_[:i+1] + context_[begin:]
            with open(file_path, 'w') as f:
                f.write(context_)


def allocate_cpu_for_compute(cpu_use_rates, compute_mems, other_cpus):
    compute_cpu = ''
    count = 0
    for index, value in enumerate(cpu_use_rates):
        for index_, value_ in enumerate(value):
            if cpu_use_rates[index][index_][1] == 1:
                if count < other_cpus:
                    count += 1
                else:
                    compute_cpu = compute_cpu + ',' + \
                        cpu_use_rates[index][index_][0]
    dict_compute = {}
    dict_compute['reserved_host_memory_mb'] = str(int(compute_mems))
    dict_compute['vcpu_pin_set'] = compute_cpu[1:]
    logger.info('For compute service, allocate {} CPUs,'
                '{} MB memory.'.format(compute_cpu[1:], str(int(compute_mems))))
    modify_nova_config(dict_compute)


def modify_ceph_init_script():
    """
    modify ceph osd init's script
    """
    command_verify = 'grep -rn \'cgroup\' ' + CEPH_INIT_PATH
    # By checking whether the file contains keywords(cgroup)
    verify = execute_command(command_verify)
    if verify[0] != '':
        return
    command_find = 'grep -rn \'$binary -i $id\' ' + CEPH_INIT_PATH
    line_number = execute_command(command_find)[0].split(':')[0]
    sed_one = 'sed -i \'s/$binary -i $id/$cgroup $binary -i $id/g\' ' + \
        CEPH_INIT_PATH
    sed_two = "sed -i '" + line_number + \
        "i\\    cgroup=\"cgexec -g cpuset,memory:osd$id\"' " + \
        CEPH_INIT_PATH
    execute_command(sed_one)
    execute_command(sed_two)


def begin(fuel_vm_enable):
    parameters = get_parameter()
    mems, need_mems, osd_mems = check_resource_enough(
        parameters, fuel_vm_enable)
    if mems <= need_mems:
        logger.info('The memory resource is not enough!')
    compute_mems = mems - need_mems
    cpu_over_ratio, other_cpus = cal_cpu_over_ratio(
        compute_mems,
        parameters,
        fuel_vm_enable)
    getcontext().rounding = ROUND_HALF_UP
    getcontext().prec = 2
    parameters["osd_cpu"] = float(Decimal(1)/Decimal(cpu_over_ratio))
    cpu_use_rates = create_osd_cgroup(parameters)
    allocate_cpu_for_compute(
        cpu_use_rates,
        compute_mems,
        int(math.ceil(Decimal(other_cpus)/Decimal(cpu_over_ratio))))
    modify_ceph_init_script()
    logger.info('The allocation of resources was successful.')
    return osd_mems


def modify_cgconfig():
    """
    modify cgconfig service's config file
    """
    if not os.path.isfile(CGCONFIG_PATH):
        return
    section = 'Service'
    options = ['ExecStart', 'ExecStop']
    insert_str = ' -L /etc/cgconfig.d'
    parse = ConfigParserForbid()
    parse.read(CGCONFIG_PATH)
    for option in options:
        value = parse.get(section, option)
        if value.__contains__(insert_str):
            return
        # modify value
        index = value.find('.conf') + 5
        value = value[:index] + insert_str + value[index:]
        parse.set(section, option, value)
    with open(CGCONFIG_PATH, 'wb') as configfile:
        parse.write(configfile)
    command = 'systemctl daemon-reload'
    execute_command(command)


def generate_cfconfigd_file():
    dir_path = '/etc/cgconfig.d'
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


def check_cgconfig_enable():
    check_command = 'systemctl is-enabled cgconfig.service'
    is_enabled = execute_command(check_command)[0]
    if not is_enabled == 'enabled':
        enable_command = 'systemctl enable cgconfig.service'
        execute_command(enable_command)


if __name__ == '__main__':
    args = get_options()
    auto_restart = args.auto_restart
    fuel_vm_enable = args.fuel_vm_enable
    modify_cgconfig()
    generate_cfconfigd_file()
    check_arg = 'systemctl is-active cgconfig.service'
    arg = 'systemctl start cgconfig.service'
    is_active = execute_command(check_arg)[0].split('\n')[0]
    if not is_active.__eq__('active'):
        execute_command(arg)
    check_cgconfig_enable()
    if auto_restart:
        osd_mems = begin(fuel_vm_enable)
        restart_service(osd_mems)
        logger.info('Service restarted successfully, '
                    'and the changes are in effect.')
    else:
        begin(fuel_vm_enable)
        logger.info('Please restart the service manually, '
                    'otherwise the changes will not take effect.')
