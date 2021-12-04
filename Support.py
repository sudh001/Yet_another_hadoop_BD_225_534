import os

import  pickle as pk

fi_delim = "\ "

class DN_attr:
    def __init__(self,dn_dir, block_size, dn_size,id_n,dn_log_path):
        
        self.id = id_n
        self.dn_dir = dn_dir
        self.block_size = block_size
        self.dn_size = dn_size
        self.dn_log_path = dn_log_path
    
    def is_same(self,attr1):

        if type(attr1) != DN_attr: return False
        if self.id != attr1.id: return False
        if self.dn_dir != attr1.dn_dir: return False
        if self.block_size != attr1.block_size: return False
        if self.dn_size != attr1.dn_size: return False
        if self.dn_log_path != attr1.dn_log_path: return False

        return True


    

def DN_check(attr,isNew):
    '''
    Checks if
    '''
    
    try:
        if isNew:
            assert type(attr) == DN_attr
            assert os.listdir(attr.dn_dir) == []
            _ = os.listdir(attr.dn_log_path)

            return True
        
        else:
            global fi_delim
            pk_f_name = attr.dn_dir + fi_delim + 'attr.pkl'
            pk_f = open(pk_f_name,'rb')
            disk_attr = pk.load(pk_f)
            if not attr.is_same(disk_attr):
                print('Failed at equi')
                return False
            
            pk_f.close()
            strg_f = open(attr.dn_dir + fi_delim + 'strg.txt','r')
            strg_f.close()

            alloc_f = open(attr.dn_dir + fi_delim + 'alloc.txt','r')
            alloc_f.close()

            log_f = open(attr.dn_log_path + fi_delim + 'log_%s.txt'%(attr.id))
            log_f.close()

            return True
    
    except Exception as E:
        print(E)
        return False


def dn_create_storage(f_name, block_size, dn_size):

    f = open(f_name,'w')
    for _ in range(block_size * dn_size):
        f.write('0')

    f.close()

def dn_create_alloc(f_name,dn_size):

    f = open(f_name,'w')
    for i in range(dn_size):
        f.write('%s,%s,%s\n' %(i,'none',0))
    
    f.close()

def dn_create_log(f_name,dn_id):

    f = open(f_name,'w')

    f.write('Logs for %s is created\n'%(dn_id))

    f.close()

def dn_find_Fblk(alloc_arr):
    for i in alloc_arr:
        if int(i[2]) == 0:
            return int(i[0])
    return -1


def dn_write_blk(strg_f_name,blk_num,block_size,content):
    
    strg_f = open(strg_f_name,'r')
    strg_str = strg_f.read()
    strg_str = strg_str[:block_size*blk_num] + content + strg_str[block_size*(blk_num+1):]
    strg_f.close()

    strg_f = open(strg_f_name,'w')
    strg_f.write(strg_str)
    strg_f.close()

def dn_update_alloc(alloc_f_name,blk_num,dfs_file):
    alloc_f = open(alloc_f_name,'r')
    alloc_arr = [i.split(',') for i in alloc_f.readlines()]
    alloc_f.close()

    
    alloc_arr[blk_num][1] = dfs_file
    alloc_arr[blk_num][2] = '1\n'
    
    alloc_w_arr = []
    for i in alloc_arr:
        st = ''
        for j in i:
            st += j + ','
            
        st = st[:-1]
        alloc_w_arr.append(st)
    
    alloc_f = open(alloc_f_name,'w')
    alloc_f.writelines(alloc_w_arr)
    alloc_f.close()


def dn_read_log(log_f_name,blk_num,isFail = False):
    if not isFail:
        st = 'block %s read\n' %(blk_num)
        log_f = open(log_f_name,'a')
        log_f.write(st)
        log_f.close()
    else:
        st = 'Failed reading block %s\n' %(blk_num)
        log_f = open(log_f_name,'a')
        log_f.write(st)
        log_f.close()


def dn_alloc_log(log_f_name,blk_num,dfs_file,isFail = False):
    if not isFail:
        st = 'block %s is allocated to %s\n' %(blk_num,dfs_file)
        log_f = open(log_f_name,'a')
        log_f.write(st)
        log_f.close()
    else:
        st = 'Allocation to %s failed\n' %(dfs_file)
        log_f = open(log_f_name,'a')
        log_f.write(st)
        log_f.close()

def dn_used_blk(blk_num,alloc_f_name):
    alloc_f = open(alloc_f_name,'r')
    alloc_arr = [i.split(',') for i in alloc_f.readlines()]
    ret = (alloc_arr[blk_num][2] == '1\n')
    alloc_f.close()
    return ret

def free_log(log_f_name,dfs_file):
    st = '%s File removed\n' %(dfs_file)
    log_f = open(log_f_name,'a')
    log_f.write(st)
    log_f.close()
