global net_file
global net_name

# net_file = '400011n.net.xml'
net_file = '900011n.net.xml'
net_name = net_file.split('n')[0]
schema_nm = 'anyang_second' if net_file == '400011n.net.xml' else 'anyang_third'


def set_net_name(name):
    global net_name
    net_name = name


def set_net_file(file):
    global net_file
    net_file = file
