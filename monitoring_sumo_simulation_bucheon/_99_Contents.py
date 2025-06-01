global net_file
global net_name

# net_file = '400011n.net.xml'
net_file = '1200012n.net.xml'
net_name = net_file.split('n')[0]
schema_nm = 'bcdb'


def set_net_name(name):
    global net_name
    net_name = name


def set_net_file(file):
    global net_file
    net_file = file
