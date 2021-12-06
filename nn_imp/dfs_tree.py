
delim = '/'

class DFS_Node:
    def __init__(self,name,isDir = True,children = []):
        '''
        attributes:
            name : name of the file
            isDir: a DFS_Node can be a directory or a file. If a directory => isDir = True. If file => isDir = False
            children : if a directory(isDir = True), it points to children DFS_Nodes. 
                       If a file(isDir = True) then it contains where the file contents are stored.
                       For a file children is a list of records where each record is of format: [...,(dn_id,blk_id),...] and is of len = rep_factor

        '''

        self.name = name
        self.isDir = isDir
        self.children = children

def get_files(node):
    global delim
    if not node.isDir:
        return [node.name]
    else:
        ret = []
        
        for chld in node.children:
            ret = ret + get_files(chld)
        
        ret = [node.name+ delim + i for i in ret]
        return ret
    
class DFS_Tree:
    def __init__(self,fs_name):
        self.root = DFS_Node(fs_name)
    
    def check_path(self,path):
        '''
        Input: path: a list of directory names where the last name can be a file or a directory

        If the path is valid, it returns the corresponding DFS_Node else raises ValueError
        '''
        
        if path[0] != self.root.name:
           raise ValueError("Invalid root")
        

        ind = 1
        trav = self.root
        nex = None
        
        while ind != len(path):
            if not trav.isDir:
                break
            c = False
            for nex in trav.children:
                if nex.name == path[ind]:
                    trav = nex
                    c = True
                    break
            
            if not c:
                break
            ind +=1
        
        if ind != len(path):
            raise ValueError(" Invalid file path")
            
        else:
            return trav
    
    def create_node(self,parent_path,new_name,isdir = True,contents = []):
        '''
        The new_node is added as a child to the parent specified by parent_path if valid
        Raises ValueError: invlaid path, path not to directory, new_name already used
        '''

        new_node = DFS_Node(new_name,isDir = isdir,children = contents)
        
        if type(new_node) != DFS_Node:
            print("Poor arguments")
            return -1
        try:
            par = self.check_path(parent_path)
        except Exception as E:
            raise E
        
        
        
        if not par.isDir:
            raise ValueError("Parent path dosen't correspond directory")
        
        elif new_node.name in [i.name for i in par.children]:
            raise ValueError("File or folder with same name exists as a child to the parent directory")
           
        else:
            
            par.children.append(new_node)
            return 0
        
    def rem_node(self,rem_path,cascade = False):
        '''
        Can't remove root node. If root node path is given, the tree is truncated
        If the node to be removed is a directory, all names of files who are descendents of the node is returned.
        If the node is a file, name of the file is returned.
        To be removed node is disconnected from the tree

        Raises ValuError
        '''
        if len(rem_path) == 1 and rem_path[0] == self.root.name and (cascade or self.root.children == []):
            ret = get_files(self.root)
            self.root.children = []
            return ret

        elif len(rem_path) < 2:
            raise ValueError("Trying to truncate root without cascade or poor naming")

        try:
            par_node = self.check_path(rem_path[:-1])
            rem_node = self.check_path(rem_path)
        except:
            raise ValueError("Poor path")
       
        if not cascade and rem_node.children != [] and rem_node.isDir:
            raise ValueError("Directory is not empty. \nHint: Use cascade option")
            
        
        fname_li = get_files(rem_node)


        for i in range(len(par_node.children)):
            if par_node.children[i]== rem_node:
                par_node.children = par_node.children[:i] + par_node.children[i+1:]
                break

        

        return fname_li
        
    def show_node(self,node_path):
        node = self.check_path(node_path)
        if node == None:
            raise ValueError("Poor path")
            
        else:
            return node.children
