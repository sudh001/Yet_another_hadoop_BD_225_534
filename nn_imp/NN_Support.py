import os
import dfs_tree as tr
import DataNode as dn
from datetime import datetime
import random as r


import  pickle as pk

fi_delim = "\ "
blank_delim = '_'

class NN_attr:
    def __init__(self,config_dict):
        '''
        fs_name: name of the file system
        nn_dir: path_to_primary namenode


        '''
        try:
            self.fs_name = config_dict['fs_name'] #1
            self.nn_dir = config_dict['nn_dir']  #2
            self.dn_dir = config_dict['dn_dir'] #3
            self.rep_factor = config_dict['rep_factor'] #4
            self.num_datanodes = config_dict['num_datanodes'] #5
            self.block_size = config_dict['block_size'] #6
            self.dn_size = config_dict['dn_size'] #7
            self.dn_log_path = config_dict['dn_log_path'] #8
            self.nn_log_path = config_dict['nn_log_path'] #9
            self.sync_period = config_dict['sync_period'] #10
            self.nn_checkpoints = config_dict['nn_checkpoints'] #11
        except:
            
            raise ValueError("Poor Arguments")
        

    
    def is_same(self,attr1):

        if type(attr1) != NN_attr: return False
        if self.fs_name != attr1.fs_name: return False
        if self.nn_dir != attr1.nn_dir: return False
        if self.dn_dir != attr1.dn_dir: return False
        if self.block_size != attr1.block_size: return False
        if self.dn_size != attr1.dn_size: return False
        if self.dn_log_path != attr1.dn_log_path: return False
        if self.nn_log_path != attr1.nn_log_path: return False
        if self.sync_period != attr1.sync_period: return False
        if self.nn_checkpoints != attr1.nn_checkpoints: return False

        return True


def nn_check(nn_attr,isNew = False):
    '''
    Checks if the given name node attributes is valid
    '''
    global fi_delim
    try:
        if isNew:
            assert type(nn_attr) == NN_attr
            assert os.listdir(nn_attr.nn_dir) == []
            assert os.listdir(nn_attr.dn_dir) == []
            
            assert nn_attr.nn_dir in nn_attr.nn_checkpoints
            assert nn_attr.nn_dir in nn_attr.nn_log_path
            
            return True
        else:
            pk_f_name = nn_attr.nn_dir + fi_delim + 'nn_attr.pkl'
            pk_f = open(pk_f_name,'rb')
            disk_attr = pk.load(pk_f)
            assert disk_attr.is_same(nn_attr)
            
            return True

    except Exception as E:
        raise ValueError("Expected arguments for config for creation or loading of name node not met")
        




def nn_create_datanodes(nn_attr):
    global fi_delim
    dn_config = {
        'id_n':-1,
        'dn_dir': '',
        'block_size': nn_attr.block_size,
        'dn_size':nn_attr.dn_size,
        'dn_log_path':nn_attr.dn_log_path
    }
    os.mkdir(dn_config['dn_log_path'])
    
    for id_n in range(nn_attr.num_datanodes):
        dn_config['id_n'] = id_n
        dn_config['dn_dir'] = nn_attr.dn_dir + fi_delim + 'dn_%s' %(id_n)
        os.mkdir(dn_config['dn_dir'])

        dn.DataNode(dn_config,isNew= True)
    

def  nn_parse_file(file_path,block_size):
    global blank_delim
    with open(file_path,'r') as f:
        content = f.read()
    
    r = len(content)%block_size
    content += (block_size - r)*blank_delim
    q = len(content)//block_size
    assert q != 0

    li = [content[i:i+block_size] for i in range(0,len(content),block_size)]
    
    return li

def get_useful_dn(dn_info_arr):
    ret = []
    for i in range(len(dn_info_arr)):
        ret += [i for j in range(dn_info_arr[i])]
    
    return ret



def nn_store_file(nn_attr: NN_attr,broken_file,dn_info_arr,dfs_file_path):
    dn_config = {
        'id_n':-1,
        'dn_dir': '',
        'block_size': nn_attr.block_size,
        'dn_size':nn_attr.dn_size,
        'dn_log_path':nn_attr.dn_log_path
    }

    useful_dn = get_useful_dn(dn_info_arr)
    alloc_arr = []

    for i in range(len(broken_file)):
        dns = r.sample(useful_dn,nn_attr.rep_factor)
        alloc_blk = []
        for id_n in dns:
            dn_config['id_n'] = id_n
            dn_config['dn_dir'] = nn_attr.dn_dir + fi_delim + 'dn_%s' %(id_n)

            my_dn = dn.DataNode(dn_config)
            blk_num = my_dn.alloc_blk(dfs_file_path,broken_file[i])
            assert blk_num != -1
            
            dn_info_arr[id_n] -= 1
            alloc_blk.append((id_n,blk_num))

        alloc_arr.append(alloc_blk)
    
    assert [i for i in dn_info_arr if i >= 0] == dn_info_arr
    return dn_info_arr, alloc_arr


def nn_fetch_file(nn_attr,blks_loc):
    
    dn_config = {
        'id_n':-1,
        'dn_dir': '',
        'block_size': nn_attr.block_size,
        'dn_size':nn_attr.dn_size,
        'dn_log_path':nn_attr.dn_log_path
    }
    
    contents = ''

    for blk in blks_loc:
        
        dn_id,blk_num = blk[0]
        dn_config['id_n'] = dn_id
        dn_config['dn_dir'] = nn_attr.dn_dir + fi_delim + 'dn_%s' %(dn_id)

        my_dn = dn.DataNode(dn_config)
        contents += my_dn.read_blk(blk_num)
    
    contents = contents[:contents.index(blank_delim)]
    
    
    return contents




def nn_rem_files(nn_attr,files):

    dn_config = {
        'id_n':-1,
        'dn_dir': '',
        'block_size': nn_attr.block_size,
        'dn_size':nn_attr.dn_size,
        'dn_log_path':nn_attr.dn_log_path
    }

    for dn_id in range(nn_attr.num_datanodes):
        dn_config['id_n'] = dn_id
        dn_config['dn_dir'] = nn_attr.dn_dir + fi_delim + 'dn_%s' %(dn_id)

        my_dn = dn.DataNode(dn_config)
        for file in files:
            my_dn.free_blks(file)
        

