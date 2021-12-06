import sys

cur_word = ''
cur_count = 0



for rec in sys.stdin:

    rec = rec.strip().split('\t')
    
    rec[1] = int(rec[1])
    if cur_word == '':
        cur_word = rec[0]
        cur_count = 1
    elif cur_word != rec[0]:
        print('%s\t%s'%(cur_word,cur_count))
        cur_word = rec[0]
        cur_count = 1
    else:
        cur_count += 1


print('%s\t%s'%(cur_word,cur_count))