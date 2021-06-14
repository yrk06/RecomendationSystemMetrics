import os
import argparse
import time
import pandas as pd
import ijson
#import threading

from multiprocessing import Process, Queue, Manager
import queue

## IO Thread
from utils.streamIOthread import ioThread

PATH = 'results_batch'

parser = argparse.ArgumentParser(description="Precision")
parser.add_argument('--n',help="Lista de N para calcular o precision",nargs='+',type=int)
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
    if not args.n :
        raise(ValueError("N não definido para o Precision"))
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
        archives.append(complete_path + arq)

    # Tempo para começar a medir
    delta0 = time.time()
    if not args.t:

        ## No modo singlethread, cada arquivo é processado sequencialmente
        # e os resultados são adicionados aos resultados

        printv('Starting singlethread')
        calculateSingleThread(archives,vars(args))
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

        ## Iniciar t threads de processamento e 1 thread de IO
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

    #Contagem de users
    user_count = 0
    for p in results:
        user_count += len(results[p])
    print(user_count)

    ##Export dos resultados
    pandasExport(results,args.a+'-'+args.b+'.precision',args.n)
    print(f'Execution took: {time.time() - delta0} seconds')



def calcThread(q,rq,stopEvent,arg):
    ##Como cada thread é um processo diferente
    # Precisamos setar a flag de novo
    global _VERBOSE
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
        parcial = precision(data,arg['n'])

        rq.put_nowait((parcial,data['file']))

    ## Avisar que acabou
    rq.put_nowait('ts')



def calculateSingleThread(arqs, arg):
    ## Essa função chama o calculatePrecisionArchive pra cada arquivo
    q = 0
    for arq in arqs:
        printv(f'{q/len(arqs) * 100}%')
        calculatePrecisionArchive(arq,arg, q)
        q += 1

def calculatePrecisionArchive(arq, arg,idx):

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
                        # rodar o algoritmo
                        if not idx in results:
                            results[idx] = []

                        results[idx].append(precision(currentUser, arg['n'])) 

                        #Resetar o user
                        currentUser["test_items"] = []
                        currentUser["ranked_list"] = []
                currentUser['userId'] = prefix 
                continue
            
            if '.item' in prefix:
                currentUser[prefix.split('.')[1]].append(value)

        ## Calcular o ultimo usuário da lista
        if not idx in results:
            results[idx] = []
        results[idx].append(precision(currentUser, args.n))


## Algoritmo de calculo do precision
# conta quantos itens entre os N primeiros da ranked_list estão nos test_items
def precision(user,n):
    results = [0] * len(n)

    for idx_N in range(len(n)):
        currentN = n[idx_N]
        count = len([0 for x in user["ranked_list"][:n[idx_N]] if x in user["test_items"]])/currentN

        results[idx_N] += count
    return results



## Calcula a média de uma lista de resultados
# é utilizado para condensar os resultados de 
# todos os arquivos em 1 só
def calculateMean(results):
    precision = list(zip(*results))
    returnResults = []
    for p in range(len(precision)):
        returnResults.append(0)
        for r in precision[p]:
            returnResults[p] += r/len(precision[p])
    return returnResults

## Agrupa os resultados por arquivo
def pandasExport(results,name,n):
    data = {}
    rows = []

    ## Aqui pegamos os valores médios de cada Arquivo
    for row in results:
        file_mean = calculateMean(results[row])
        rows.append(file_mean)

    ## Transposição para gerar um Dataframe com label
    columns = list(zip(*rows))

    ##Criação do Label de cada coluna
    for p in range(len(columns)):
        data[n[p]] = columns[p]

    ## Criação do dataframe
    df = pd.DataFrame.from_dict(data)
    
    df.loc['mean'] = df.mean()
    df.loc['std'] = df.std()

    printv(df)
    df.to_pickle(name)

## Utils
def printv(content):
    if _VERBOSE:
        print(content)

def printp():
    pass
    ## Export profilling logic


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
            

        
    
    
