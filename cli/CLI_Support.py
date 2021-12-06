import json as j
import lib.NameNode as nn


def json_to_dict(config_path):
    with open('config.json','r') as f:
        dicti = j.load(f)
        for key in dicti:
            try:
                dicti[key] = int(dicti[key])
            except:
                continue
    
    return dicti

options_str = '''
Below are the list of dfs operations allowed along with their syntax:
    put local_file_path dfs_file_path
    cat dfs_file_path [output_file_path]
    ls dfs_dir_path
    rm rem_path [-r]
    mkdir new_dfs_dir_path
    rmdir rem_dfs_dir_path
    mr  -i_path_to_dfs_input  -o_path_to_dfs_output -c_path_to_config -m_path_to_mapper -r_path_to_reducer
    exit

'''

def command_parser(cmd,nnNew: nn.NameNode):
    cmd_li = [i for i in cmd.split(' ') if i != '']


    if cmd_li[0] == 'put':
        return put_parser(cmd_li,nnNew)
    elif cmd_li[0] == 'cat':
        return cat_parser(cmd_li,nnNew)
    elif cmd_li[0] == 'ls':
        return ls_parser(cmd_li,nnNew)
    elif cmd_li[0] == 'rm':
        return rm_parser(cmd_li,nnNew)
    elif cmd_li[0] == 'mkdir':
        return mkdir_parser(cmd_li,nnNew)
    elif cmd_li[0] == 'rmdir':
        return rmdir_parser(cmd_li,nnNew)
    elif cmd_li[0] == 'exit':
        quit()
    else:
        print("No command found named %s found"%(cmd_li[0]))
        return False
        
        


def put_parser(cmd_li,nnNew: nn.NameNode):
    try:
        nnNew.put(cmd_li[1],cmd_li[2])
        return True
    except Exception as E:
        print(E)
        return False


def cat_parser(cmd_li,nnNew: nn.NameNode):
    try:
        cmd_li.append(None)
        to_print = nnNew.cat(cmd_li[1],cmd_li[2])
        print(to_print)
        return True
    except Exception as E:
        print(E)
        return False

    

def ls_parser(cmd_li,nnNew: nn.NameNode):
    try:
        chlds = nnNew.ls(cmd_li[1])
        for i in chlds:
            print(i)
        return True

    except Exception as E:
        print(E)
        return False
    

def rm_parser(cmd_li,nnNew: nn.NameNode):
    try:
        if '-r' in cmd_li:
            cmd_li.remove('-r')
            nnNew.rm(cmd_li[1],cascade= True)
        else:
            nnNew.rm(cmd_li[1],cascade= False)
        
        return True
    except Exception as E:
        print(E)
        return False


def mkdir_parser(cmd_li,nnNew: nn.NameNode):
    try:
        nnNew.mkdir(cmd_li[1])
        return True
    except Exception as E:
        print(E)
        return False

def rmdir_parser(cmd_li,nnNew: nn.NameNode):
    try:
        nnNew.rmdir(cmd_li[1])
        return True
    except Exception as E:
        print(E)
        return False


    