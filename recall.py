import os
import argparse
import json
import time
import pandas as pd
#import threading
import queue
from datetime import datetime

from multiprocessing import Process, Queue, Manager


PATH = 'results'

## Test: recall.py --n 1 5 --a BPRMF --b SMDI-400k_max200unique.csv --m lote

parser = argparse.ArgumentParser(description="Recall")
parser.add_argument('--n',help="Lista de N para calcular o recall",nargs='+',type=int)
parser.add_argument('--a',help="Algorimo para a avaliação",type=str)
parser.add_argument('--b',help="base de Dados para a avaliação",type=str)

parser.add_argument('--m',help="Modo de processamento [Janelada, Lote]",type=str)
parser.add_argument('--j',help="Tamanho da Janela",type=int)

parser.add_argument('--t',help="Para usar o Multithreading",type=int)

parser.add_argument('--v',help="Verbose",action="store_true")
parser.add_argument('--p',help="Profiling do algoritmo",action="store_true")

results = []

_VERBOSE = False
_PROFILE = False

## CPU start, Init, I/O, Calculo, Export, End time,
## Start, Init, Export só são rodados 1 vez
## I/O Load e Calculo podem ser rodados varias vezes
_profillingTimes = [0,0,[],[],[],0,0]

def main(args):
    global _PROFILE
    global _VERBOSE
    _profillingTimes[0] = time.time()
    ##Validação dos Parametros passados
    MODE = 0
    if not args.n :
        raise(ValueError("N não definido para o Recall"))
    if not args.a :
        raise(ValueError("Algoritmo não definido"))
    if not args.b :
        raise(ValueError("Base de Dados não definida"))

    if args.m.lower() == 'janelada':
        MODE = 0
        if not args.j:
            ValueError("Tamanho de Janela não presente")
    elif args.m.lower() == 'lote':
        MODE = 1
    else:
        raise(ValueError("Modo de processamento não suportado"))
    
    _PROFILE = args.p
    _VERBOSE = args.v

    ##Gerando o caminho completo até as execuções
    complete_path = PATH + '/' + args.a + '/' + args.b + '/'

    archives = []
    for arq in os.listdir(complete_path):
        if arq.split('.')[len(arq.split('.'))-1].lower() == 'dat':
            archives.append(complete_path + arq)

    _profillingTimes[1] = time.time()

    if not args.t:
        printv('Starting singlethread')
        calculateSingleThread(archives,MODE,vars(args))
    else:
        if args.t == 0:
            raise(ValueError("Numero Ilegal de threads para modo multithread"))
        manager = Manager()
        printv('Starting multithread')
        threads = [None] * (args.t+1)
        q = Queue(len(archives))
        rq = Queue(len(archives))
        se = manager.Event()
        printv(f'Starting with {args.t} threads')
        ## Init IO thread
        
        for idx in range(args.t):
            threads[idx+1] = Process(target=calcThread,args=(q,rq,se,MODE,vars(args)))
            threads[idx+1].start() 
        
        threads[0] = Process(target=ioThread,args=(q,se,archives))
        threads[0].start()

        ## Wait for everyone to finish
        for thread in threads:
            thread.join()
        
        ##Append results
        for p in range(len(archives)):
            try:
                data = rq.get(False)
                results.append(data)
                #rq.task_done()
            except queue.Empty:
                break

    _profillingTimes[5] = time.time()
    pandasExport(results,args.a+'-'+args.b+'.recall',args.n)
    _profillingTimes[6] = time.time()
    print(f'Execution took: {time.time() - _profillingTimes[0]} seconds')
    printp()
    #input('Press enter to continue')

def run(arg):
    pass

def ioThread(q,stopEvent,arqs):
    d0 = time.time()
    for arq in arqs:
        instance = prepareData(arq)
        q.put_nowait(instance)
    stopEvent.set()
    print(f'IO: {time.time()-d0}')

def calcThread(q,rq,stopEvent,MODE,arg):
    while True:
        try:
            data = q.get(timeout=0.01)
        except queue.Empty:
            if not stopEvent.is_set():
                continue
            else:
                break
        
        parcial = recallN(data,arg['n'],MODE,arg['j'])

        ##Signalize we're done here
        #q.task_done()

        if MODE == 1:
            rq.put_nowait(parcial)
            #results.append(parcial)
        else:
            for lista in parcial:
                #results.append(lista)
                rq.put_nowait(lista)

def calculateSingleThread(arqs,MODE,arg):
    ##Convertendo os dados de texto plano para a lista
    for arq in arqs:
        calculateRecallArquive(arq,MODE,arg)

def calculateRecallArquive(arq,MODE,arg):
    #printv(f"Preparing Data {arq}")
    instance = prepareData(arq)
    #printv("Calculating Instance")
    parcial = recallN(instance,arg['n'],MODE,arg['j'])
    if MODE == 1:
        results.append(parcial)
    else:
        for lista in parcial:
            results.append(lista)
    #printv(f'Archive {arq} finished')

## Coloca todos os resultados em um formato tabelado do Pandas
def pandasExport(results,name,n):
    data = {}
    it = 0
    for iterator in zip(*results):
        data[n[it]] = list(iterator)
        it += 1

    df = pd.DataFrame.from_dict(data)

    
    df.loc['mean'] = df.mean()
    df.loc['std'] = df.std()
    
    printv(df)
    df.to_csv(name)

##Encapsular todos os resultados em um Json e exportar
def jsonExport(results,name,n):
    exportDict = {}
    inst = 0
    while inst < len(results):
        #print(results[inst])
        exportDict[inst] = {}
        it = 0
        while it < len(n):
            exportDict[inst][n[it]] = results[inst][it] 
            it += 1
        inst += 1
        
    with open(name+'.json','w+') as exp_json:
        exp_json.write(json.dumps(exportDict))

##Faz o calculo do Recall
def recallN(instances,n,modo,tamanho_janela = 0):
    delta0 = time.time()
    results = []
    parcial = [0 for x in n]
    inst_idx = 0
    ##Para cada instancia vemos em qual N do recall se encaixa
    while inst_idx < len(instances):
        t_instance = instances[inst_idx]
        rank = int(t_instance[1]) +1 ## para os ranks começarem em 1
        it = 0
    
        while it < len(n):
            if rank <= n[it]:
                ##Adiciona uma contagem em cada N que a instancia está
                parcial[it] += 1
            it += 1

        ##Caso seja modo janelado, devolvemos os resultados como um array do recall das várias janelas
        if modo == 0:
            if (inst_idx + 1)  % tamanho_janela == 0:
                results.append(calculateCarryMean(parcial,n,len(instances)))
                parcial = [0 for x in n]
        inst_idx += 1

    ##Caso não seja janelado, colocamos todos os resultados juntos
    if modo == 1:
        results = calculateCarryMean(parcial,n, len(instances))
    _profillingTimes[3].append(time.time() - delta0)
    return results         

##Exibe os resultados
def printResults(results):
    print(results)
    for l in results:
        outstr = ''
        for st in [str(x) + ' ' for x in l]:
            outstr += st
        #print(outstr + '\n')

## Calculo da média da lista
def calculateCarryMean(results,n,size):
    for value in range(len(results)):
        results[value] = results[value] / size
    return results


## Os dados vem em tuplas em texto plano,
# Essa função faz o parse para o algoritmo usar
def prepareData(arq):
    
    lines = ''
    delta1 = time.time()
    #with open(arq) as data:
        #
        ##Leitura de todos os dados
        #lines = data.read()

    data = open(arq)
    lines = data.read()
    data.close()

    delta2 = time.time()
    delta0 = time.time()

    ## Aqui retiramos todos os espaços e parenteses
    unformated_content = lines.replace(')','').replace(' ','').split('(')
    content = []

    for inst in unformated_content:
        ##Se a instancia for vazia (por conta do split isso ocorre) pular
        if not inst:
            continue
        ## Separar os conteudos em ','
        content.append(inst.split(','))
    ## Aqui o conteudo esta separado em listas, do Python
    _profillingTimes[2].append(time.time() - delta0)
    _profillingTimes[4].append(delta2-delta1)
    return content

##Utils
def printv(content):
    if _VERBOSE:
        print(content)

def printp():
    if not _PROFILE:
        return
    print('------------------')
    print('Recall')
    print('Profiling Results:')
    #print(f'Init time: {_profillingTimes[1] - _profillingTimes[0]}')
    ## Calculate Avg time (by archive) spent in IO and calc
    #LT = sum([x for x in _profillingTimes[4]])/len(_profillingTimes[4])
    #IO = sum([x for x in _profillingTimes[2]])/len(_profillingTimes[2])
    #calc = sum([x for x in _profillingTimes[3]])/len(_profillingTimes[3])
    #print(f'Avg I/O function time: {IO}')
    #print(f'Avg Archive load time: {LT}')
    #print(f'Avg Calc time: {calc}')
    print(f'Amount of archives analized: {len(_profillingTimes[2])}')
    #print(f'Export time: {_profillingTimes[6] - _profillingTimes[5]}')
    print(f'Total real time: {_profillingTimes[6] - _profillingTimes[0]}')
    print('------------------')
    #with open('recall.csv','a+') as file:
        #file.write(f'\n{args.t},{_profillingTimes[1] - _profillingTimes[0]},{IO},{LT},{calc},{_profillingTimes[6] - _profillingTimes[5]},{_profillingTimes[6] - _profillingTimes[0]}')

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
