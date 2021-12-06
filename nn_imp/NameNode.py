import NN_Support as n_su
import dfs_tree as tr
from datetime import datetime
import DataNode as dn
import pickle as pk
import os

fi_delim = "\ "

class NameNode:
    def __init__(self,nn_config,isNew = False) -> None:
        '''
        nn_config is a dict with following keys:
            fs_name: str, name of the file system
            nn_dir : str, path to primary namenode directory
            dn_dir: str, path_to_datanodes directory
            rep_factor: int, replication factor
            num_datanodes: int, number of datanodes
            block_size: int, size of a block in a datanode
            dn_size: int, number of blocks per datanode
            dn_log_path: str, path to datanodes logs directory
            nn_log_path: str, path to namenode logs file
            sync_period: int, num seconds b/w 2 syncs
            nn_checkpoints: str, path to diretory where checkpoints are stored
        
        Attributes of NameNode instance:
            -> attr: instance of NN_attr
            -> fs_image : instance of DFS_Tree
            -> dn_info_arr : array of free blocks in datanodes. dn_info_arr[i] -> num free blocks in ith datanode
        '''
        try:
            nn_attr = n_su.NN_attr(nn_config)
            assert n_su.nn_check(nn_attr,isNew)
            
            if isNew:
                self.attr = nn_attr
                self.fs_image = tr.DFS_Tree(nn_attr.fs_name)
                self.dn_info_arr = [ nn_attr.dn_size for i in range(nn_attr.num_datanodes)]

                self.create_checkpoint(isNew = True)

                n_su.nn_create_datanodes(nn_attr)
                
                self.update_nnlogs(nn_attr)
                

                nn_attr_f_name = nn_attr.nn_dir + fi_delim + 'nn_attr.pkl'
                nn_attr_f = open(nn_attr_f_name,'wb')
                pk.dump(nn_attr,nn_attr_f)
                nn_attr_f.close()
            
            else:
                self.attr = nn_attr
                
                chk_fname = nn_attr.nn_checkpoints + fi_delim +'checkpoint' +'.pkl'
                
                with open(chk_fname,'rb') as chk_f:
                    self.fs_image = pk.load(chk_f)
                
                self.dn_info_arr = [ nn_attr.dn_size for i in range(nn_attr.num_datanodes)]
                self.update_nnlogs(nn_attr)


                
                

        except Exception as E:
            # print("poor arguments")
            print(E)
    
    def update_nnlogs(self,nn_attr):
        '''
        Updates namenode logs and self.dn_info_arr
        '''
        status_arr = []
        dn_config = {
            'id_n':-1,
            'dn_dir': '',
            'block_size': nn_attr.block_size,
            'dn_size':nn_attr.dn_size,
            'dn_log_path':nn_attr.dn_log_path
        }

        for id_n in range(nn_attr.num_datanodes):
            # Creating datanode object

            dn_config['id_n'] = id_n
            dn_config['dn_dir'] = nn_attr.dn_dir + fi_delim + 'dn_%s' %(id_n)            
            cur_dn = dn.DataNode(dn_config)
            # updating nn_logs and dn_info_arr

            self.dn_info_arr[id_n] = cur_dn.get_status()
            status_arr.append('%s,%s,%s\n' %(id_n,datetime.now().strftime("%H:%M:%S"),self.dn_info_arr[id_n]))
        
        nn_log_f = open(nn_attr.nn_log_path,'w')
        nn_log_f.writelines(status_arr)
        nn_log_f.close()

    def create_checkpoint(self,isNew = False):
        global fi_delim
        nn_attr = self.attr
        check_dir = nn_attr.nn_checkpoints
        fs_image = self.fs_image
        try:
            if isNew:
                os.mkdir(check_dir)
                new_chk_fname = check_dir + fi_delim +'checkpoint' +'.pkl'
                with open(new_chk_fname,'wb') as new_chk_f:
                    pk.dump(fs_image,new_chk_f)
            
            else:
                new_chk_fname = check_dir + fi_delim +'checkpoint' +'.pkl'
                with open(new_chk_fname,'wb') as new_chk_f:
                    pk.dump(fs_image,new_chk_f)
                

        
        except Exception as E:
            print(E)
            print("Bad arguments")
            raise ValueError("Whoops from create checkpoints")
            
    def put(self,file_path,dfs_file_path):
        '''
        For specifying a dfs_flie_path, delim is /
        No spaces allowed in dfs_file_path
        file_path specifies path to file in local machine
        '''
        nn_attr = self.attr
        try:
            broken_file = n_su.nn_parse_file(file_path,nn_attr.block_size)

        except:
            print("Poor arguments")
            return
        
        try:
            assert len(broken_file) * nn_attr.rep_factor <= sum(self.dn_info_arr)
        except:
            print("Not enough space")
            return
        
        try:
            path = dfs_file_path.split(tr.delim)
            parent_path = path[:-1]
            
            f_name = path[-1]
            self.fs_image.check_path(parent_path)

        except Exception as E:
            print("Poor destination file path")
            print(E)
            return
        
        try: 
            c = 0
            self.fs_image.check_path(path)
        except:
            c = 1
        
        if c == 0:
            print("File or directory of same name exists")
            return
        
        try:
            self.dn_info_arr, content = n_su.nn_store_file(nn_attr,broken_file,self.dn_info_arr,dfs_file_path)
        except Exception as E:
            print("Whoops didn't expect that from put")
            raise E
        
        self.fs_image.create_node(parent_path,f_name,isdir= False,contents = content)
        self.update_nnlogs(self.attr)
        self.create_checkpoint()

        
    def cat(self,dfs_file_path,output_file_path):

        file_loc = dfs_file_path.split(tr.delim)
        try:
            assert self.fs_image.check_path(file_loc).isDir == False
        except:
            print("File dosen't exist in this location")
            return

        nn_attr = self.attr
        blks_loc = self.fs_image.show_node(file_loc)
        contents = n_su.nn_fetch_file(nn_attr,blks_loc)

        try:
            with open(output_file_path,'w') as f:
                f.write(contents)
        
        except:
            print("output path is invalid")
            return

    
    def ls(self,dfs_dir_path):

        dir_loc = dfs_dir_path.split(tr.delim)
        
        try:
            assert self.fs_image.check_path(dir_loc).isDir == True
        except:
            print("Directory dosen't exist in this location")
            return
        

        chld = self.fs_image.show_node(dir_loc)
        return [i.name for i in chld]


    def rm(self,rem_path,cascade = False):
        nn_attr = self.attr
        try:
            rem_loc = rem_path.split(tr.delim)
            files = self.fs_image.rem_node(rem_loc,cascade)

            if len(rem_loc) != 1:
                rem_loc_str = ''
                for i in rem_loc[:-1]:
                    rem_loc_str += i + tr.delim
                
                files = [rem_loc_str + i for i in files]

            n_su.nn_rem_files(nn_attr,files)
            self.update_nnlogs(self.attr)
            self.create_checkpoint()

        
        except Exception as E:
            print(E)
        
    def mkdir(self,new_dir_path):
        
        new_dir_loc = new_dir_path.split(tr.delim)

        try:
            self.fs_image.create_node(new_dir_loc[:-1],new_dir_loc[-1])
            self.create_checkpoint()

        except Exception as E:
            print(E)

    def rmdir(self,rem_dir_path):

        rem_dir_loc = rem_dir_path.split(tr.delim)
        try:
            assert self.fs_image.check_path(rem_dir_loc).isDir == False
        
        except:
            print("Path dosen't correspond to directory")
        
        try:
            self.fs_image.rem_node(rem_dir_loc)
            self.create_checkpoint()
        except:
            print("Directory not empty")