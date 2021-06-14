import pandas as pd
import matplotlib.pyplot as plt
import csv

headers = ["Workers","Inicialização", "I/O" , 'Loading' , 'calculo' , 'Exportação' , 'Total']
df = pd.read_csv('recallQueue.csv',names=headers)
X = df["Workers"][1:]

for key in df:
    if key == "Workers":
        continue 
    plt.plot(X,list(map(lambda x: round(float(x),2), df[key][1:])),label=key)
    plt.xlabel('nº de Threads')
    plt.ylabel('Tempo (s)')
    plt.grid()
    plt.autoscale()
    plt.legend()
    plt.show()
