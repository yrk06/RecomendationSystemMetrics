import os
import argparse
import time
import pandas as pd
import ijson
import math

from multiprocessing import Process, Queue, Manager
import queue

## IO Thread
from utils.streamIOthread import ioThread

PATH = 'results_batch'

parser = argparse.ArgumentParser(description="Recall")
parser.add_argument('--a',help="Algorimo para a avaliação",type=str)
parser.add_argument('--b',help="base de Dados para a avaliação",type=str)

parser.add_argument('--t',help="Para usar o Multithreading",type=int)
parser.add_argument('--v',help="Verbose",action="store_true")
parser.add_argument('--p',help="Profiling do algoritmo",action="store_true")

results = {}
_VERBOSE = False
_PROFILE = False
def main(args):

    ## Pegar as váriaveis globais para não ocorrer 'shadowing' na hora do assignment
    global _PROFILE
    global _VERBOSE

    ##Validação dos Parametros passados
    if not args.a :
        raise(ValueError("Algoritmo não definido"))
    if not args.b :
        raise(ValueError("Base de Dados não definida"))

    ## Set das flags para a thread main usar
    _PROFILE = args.p
    _VERBOSE = args.v

    ## Construção do caminho para os arquivos
    complete_path = PATH + '/' + args.a + '/' + args.b + '/'
    
    ## Esse array guarda o path de cada arquivo individual que vai ser analizado
    archives = []
    for arq in os.listdir(complete_path):
        archives.append(complete_path+arq)
    

    # Tempo para começar a medir
    delta0 = time.time()
    if not args.t:

        ## No modo singlethread, cada arquivo é processado sequencialmente
        # e os resultados são adicionados aos resultados

        printv('Starting singlethread')
        calculateSingleThread(archives)

    else:

        ## No modo multithread, uma thread processa todos os arquivos 
        # e posta os users em uma queue. as threads pegam os users das 
        # queues, calculam o precision e devolvem o resultado em uma
        # outra queue
        
        if args.t == 0:
            raise(ValueError("Numero Ilegal de threads para modo multithread"))
        
        ## manager é uma classe da biblioteca Multiprocessing que permite
        # compartilhar variaveis (e eventos) entre processos
        
        manager = Manager()

        printv('Starting multithread')

        ## Array que guarda t threads de processamento e 1 thread de IO
        threads = [None] * (args.t+1)

        ## Queue de usuários para processar e queue de respostas
        # como cada arquivo tem por volta de 8K usuários para processar
        # 10,000 vagas na queue são suficientes para o programa
        q = Queue(len(archives) * 10000)
        rq = Queue(len(archives) * 10000)

        ## Evento que avisa as threads para acabar o processamento
        se = manager.Event()


        printv(f'Starting with {args.t} threads')
        
        ## Inicialização das Threads de cálculo
        # elas ficam Idle enquanto o evento de parada não é setado
        # e enquanto não tem usuários na queue
        for idx in range(args.t):
            threads[idx+1] = Process(target=calcThread,args=(q,rq,se,vars(args)))
            threads[idx+1].start() 
        
        ## Thread de IO
        threads[0] = Process(target=ioThread,args=(q,se,archives,vars(args)))
        threads[0].start()

        ## A thread de resposta bloqueia o put caso os itens não estejam sendo retirados
        threads_finished = 0
        while threads_finished < args.t:
            try:
                data = rq.get(False)
                
                ## Cada thread envia um 'ts' (thread stopped) quando acaba tudo
                # para sinalizar que a thread finalizou o processamento
                if data == 'ts':
                    threads_finished += 1
                    continue
                
                ## Adicionar os resultados, mas deixar separado por arquivo que originou
                if not data[1] in results:
                    results[data[1]] = []
                results[data[1]].append(data[0])

            except queue.Empty:
                continue

        printv('All results joined')
        
        ## Como as threads ja acabaram, dar join para destrui-las
        for thread in threads:
            thread.join()
            printv('Thread joined')
        printv('All threads joined')
    
    ##Export dos resultados
    pandasExport(results,args.a+'-'+args.b+'.ndcg')
    print(f'Execution took: {time.time() - delta0} seconds')

def calcThread(q,rq,stopEvent,arg):
    ##Como cada thread é um processo diferente
    # Precisamos setar a flag de novo
    _VERBOSE = arg['v']
    while True:
        try:
            ## tentar pegar um user na queue
            data = q.get(timeout=0.01)
        except queue.Empty:
            if not stopEvent.is_set():
                continue
            else:
                ## Se a queue ta vazia e o evento de parada já foi acionado
                # sair do loop
                break
        ##Calcular o user e colocar na queue de resposta
        parcial = calculate(data)

        rq.put_nowait((parcial,data["file"]))
    
    ## Avisar que acabou
    rq.put_nowait('ts')

def calculateSingleThread(arqs):
    ## Essa função chama o calculatenDCGArchive pra cada arquivo
    q = 0
    for arq in arqs:
        printv(f'{q/len(arqs) * 100}%')
        calculatenDCGArchive(arq,q)
        q += 1

def calculatenDCGArchive(arq, idx):
    
    ## Essa função é identica ao IOThread,
    # mas ao invés de postar o user em uma queue, 
    # ele calcula o resultado e ja coloca no dict results
    with open(arq) as file:
    
        currentUser = {
            "userId": '',
            "test_items": [],
            "ranked_list": [],
            "file": idx,
            }

        ## O Ijson vai fazendo o Parse de uma propriedade de cada vez, então vamos reconstruindo o User
        ## Para processar
        ijsonParser = ijson.parse(file)
        for prefix, event, value in ijsonParser:
            if prefix == '':
                continue
            
            if not '.' in prefix:
                if not currentUser['userId'] == '':
                    if currentUser['userId'] != prefix:

                        ##Concluiu montar um usuário
                        ##Rodar o algoritmo
                        if not idx in results:
                            results[idx] = []

                        results[idx].append(calculate(currentUser))

                        ## Resetar o user
                        currentUser["test_items"] = []
                        currentUser["ranked_list"] = []

                currentUser['userId'] = prefix 
                continue
            
            if '.item' in prefix:
                currentUser[prefix.split('.')[1]].append(value)

        ## Calcular o ultimo usuário da lista
        if not idx in results:
            results[idx] = []
        results[idx].append(calculate(currentUser)) 



def mean(rList):
    result = 0
    for p in rList:
        result += p/ len(rList)
    return result

def pandasExport(results,name):
    data = {}
    rows = []

    ## Aqui pegamos os valores médios de cada Arquivo
    for p in results:
        rows.append(mean(results[p]))

    data[0] = rows

    ## Criação do dataframe
    df = pd.DataFrame.from_dict(data)

    df.loc['mean'] = df.mean()
    df.loc['std'] = df.std()

    printv(df)
    df.to_pickle(name)

def calculate(user):
    item_list = generateList(user)
    ndcg = nDCG(item_list)
    idcg = iDCG(item_list)
    return ndcg/idcg if idcg != 0 else 0

def nDCG(itemList):
    result = 0
    for idx in range(len(itemList)):
        item = itemList[idx]
        value = item[1] / math.log(idx+2,2) ## (idx+2 por que idx inicia em 0)
        result += value
    return result

def iDCG(itemList):
    ranked_list = sortByRelevance(itemList)
    return nDCG(ranked_list)

def generateList(user):
    return [(x, 1 if x in user['test_items'] else 0) for x in user['ranked_list']]

def sortByRelevance(itemList):
    return_list = []
    for item in itemList:
        if item[1] == 1:
            return_list.insert(0,item)
        else:
            return_list.append(item)
    return return_list

##Utils
def printv(content):
    if _VERBOSE:
        print(content)

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)