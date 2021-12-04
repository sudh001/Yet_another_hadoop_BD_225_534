import os
import json
import pickle as pk
import Support as su



class DataNode:
    '''
    Files/directories a DataNode object can modify:
    -> dn_dir : a directory where all the relevant files of the datanode is present
    -> strg : a file which contains the blocks of the data node, created/modified inside dn_dir
    -> attr : a file containing the relevant attributes of a datanode, created/modified inside dn_dir
    -> alloc: a file containing the alloation details of the blocks stored in the datanode, created/modified inside dn_dir
    -> dn_log_path : 
    '''
    
    def __init__(self, attr, isNew = False):
        try:
            assert type(attr) == su.DN_attr
        except:
            print("Poor attribute format")
            return 1
        if not su.DN_check(attr,isNew):
            print("Poor attributes for datanode construction")
            return
        
        if isNew:
            self.create_dn(attr)
            self.attr = attr
        else:
            
            self.attr = attr
            
    def create_dn(self,attr):
        '''
        This function is called by the constructor for creation the necessary files for a functioning datanode.
        Files created are:
            -> strg.txt : Stores the data, located in dn_dir 
            -> alloc.txt : Stores which block is allocated to which file in dfs, located in dn_dir
            -> log_id.txt : Stores operations done on datanode, located in dn_log_path
            -> attr.pkl : Stores the attributes of the datanode, located in dn_dir
        '''
        strg_f_name = attr.dn_dir + su.fi_delim + 'strg.txt'
        su.dn_create_storage(strg_f_name,attr.block_size,attr.dn_size)

        alloc_f_name = attr.dn_dir + su.fi_delim + 'alloc.txt'
        su.dn_create_alloc(alloc_f_name,attr.dn_size)

        log_f_name = attr.dn_log_path + su.fi_delim + 'log_%s.txt'%(attr.id)
        su.dn_create_log(log_f_name,attr.id)

        attr_f_name = attr.dn_dir + su.fi_delim + 'attr.pkl'
        

        with open(attr_f_name,'wb') as attr_f:
            pk.dump(attr,attr_f)
    
    def read_blk(self,blk_num):
        attr = self.attr
        try:
            assert blk_num < attr.dn_size
            alloc_f_name = attr.dn_dir + su.fi_delim + 'alloc.txt'
            assert su.dn_used_blk(blk_num,alloc_f_name)
            attr = self.attr
            strg_f_name = attr.dn_dir + su.fi_delim + 'strg.txt'
            strg_f = open(strg_f_name,'r')
            
            strg_f.seek(attr.block_size*blk_num)
            
            ret = strg_f.read(attr.block_size)
            
            strg_f.close()

            log_f_name = attr.dn_log_path + su.fi_delim + 'log_%s.txt'%(attr.id)
            su.dn_read_log(log_f_name,blk_num)

            return ret

        except Exception as E:
            log_f_name = attr.dn_log_path + su.fi_delim + 'log_%s.txt'%(attr.id)
            su.dn_read_log(log_f_name,blk_num,isFail=True)
            print(E)
            print("poor access request")
            return 1




    def alloc_blk(self,dfs_file,content):
        '''
        Name node can only tell data node to store a block. It can't tell where
        '''
        
        attr = self.attr
        try:
            assert len(content) == self.attr.block_size
        except:
            blk_num = -1
            log_f_name = attr.dn_log_path + su.fi_delim + 'log_%s.txt'%(attr.id)
            su.dn_alloc_log(log_f_name,blk_num,dfs_file,isFail = True)
            print('Poor content')
            return 1
        
        # Fetching the allocation array
        
        alloc_f_name = attr.dn_dir + su.fi_delim + 'alloc.txt'
        alloc_f = open(alloc_f_name,'r')
        alloc_arr = [i.split(',') for i in alloc_f.readlines()]
        alloc_f.close()

        try:

            blk_num = su.dn_find_Fblk(alloc_arr)
            assert blk_num != -1
            strg_f_name = attr.dn_dir + su.fi_delim + 'strg.txt'
            su.dn_write_blk(strg_f_name,blk_num,attr.block_size,content)

            su.dn_update_alloc(alloc_f_name,blk_num,dfs_file)

            log_f_name = attr.dn_log_path + su.fi_delim + 'log_%s.txt'%(attr.id)
            su.dn_alloc_log(log_f_name,blk_num,dfs_file)
           
            return blk_num



        except Exception as E:
            blk_num = -1
            log_f_name = attr.dn_log_path + su.fi_delim + 'log_%s.txt'%(attr.id)
            su.dn_alloc_log(log_f_name,blk_num,dfs_file,isFail = True)

            print(E)
            print('Datanode %s is full' %(attr.id))
            return 1

    
    def free_blks(self,dfs_file):
        '''
        Since HDFS dosen't suport editing of files, if we are freeing a block => 
        we are freeing a file => we need to free all blocks from same file.
        Hence only dfs_file argument is needed for freeing blocks
        '''
        attr = self.attr
        alloc_f_name = attr.dn_dir + su.fi_delim + 'alloc.txt'
        alloc_f = open(alloc_f_name,'r')
        alloc_arr = [i.split(',') for i in alloc_f.readlines()]

        alloc_f.close()
        for i in range(len(alloc_arr)):
            al = alloc_arr[i]
            if al[1] == dfs_file:
                alloc_arr[i][1] = 'None'
                alloc_arr[i][2] = '0\n'
        
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

        log_f_name = attr.dn_log_path + su.fi_delim + 'log_%s.txt'%(attr.id)
        su.free_log(log_f_name,dfs_file)
