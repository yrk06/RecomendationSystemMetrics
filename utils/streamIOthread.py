import ijson
import time

_VERBOSE = False


def ioThread(q,stopEvent,arqs,args):
    d0 = time.time()
    print(len(arqs))
    _VERBOSE = args['v']
    pctg = 0
    for arq in arqs:
        #print(f'{pctg/len(arqs) * 100}%')
        
        with open(arq) as file:
            currentUser = {
                "userId": '',
                "test_items": [],
                "ranked_list": [],
                "file": pctg,
                ## Adicionar Track de arquivo
            }

            ijsonParser = ijson.parse(file)
            for prefix, event, value in ijsonParser:
                if prefix == '':
                    continue
                
                if not '.' in prefix:
                    if not currentUser['userId'] == '':
                        if currentUser['userId'] != prefix:
                            ##Concluiu montar um usuário
                            q.put_nowait(currentUser)        
                            currentUser["test_items"] = []
                            currentUser["ranked_list"] = []
                    currentUser['userId'] = prefix 
                    continue
                
                if '.item' in prefix:
                    currentUser[prefix.split('.')[1]].append(value)

            q.put_nowait(currentUser)
        pctg += 1
    stopEvent.set()
    print(f'IO: {time.time() - d0}')


def printv(content):
    if _VERBOSE:
        print(content)