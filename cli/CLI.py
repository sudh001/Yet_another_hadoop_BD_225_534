import lib.NameNode as nn
import sys

import CLI_Support as c_su

try:
    assert len(sys.argv) == 3
except:
    print("Please enter path_to_config.json file, isNew(y or n) and try again")
    quit()

try:
    config = c_su.json_to_dict(sys.argv[1])
    print(config,'\n\n')
except:
    print("Poor file path")
    quit()

try:
    if sys.argv[2] == 'y' or sys.argv[2] == 'Y':
        nnNew = nn.NameNode(config,isNew = True)
    else:
        nnNew = nn.NameNode(config,isNew = False)
except Exception as E:
    print(E)
    print("Poor config specs")
    quit()

case = True

while 1:
    print(c_su.options_str)
    cmd = input()
    case = c_su.command_parser(cmd,nnNew)
    if case:
        print("Command executed succesfully")
    else:
        print("Command failed")
    

