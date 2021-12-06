import lib.NameNode as nn
import json as j

with open('config.json','r') as f:
    dicti = j.load(f)
    for key in dicti:
        try:
            dicti[key] = int(dicti[key])
        except:
            continue

# print(dicti)

try:
    nnNew = nn.NameNode(dicti,isNew = True)
    nnNew.mkdir('My_dfs/Input')
    nnNew.mkdir('My_dfs/Output')
    nnNew.put('my_input.txt','My_dfs/Input/input.txt')

except:
    print("Initialization failed")