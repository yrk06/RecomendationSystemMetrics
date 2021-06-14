import argparse
import recall
import precision
import nDCG
import mrr


parser = argparse.ArgumentParser(description="Metricas")
parser.add_argument('--c',help="Métrica para ser calculada: \n-recall\n-precision\n-ndcg\n-mrr",type=str)
parser.add_argument('--n',help="Lista de N para (apenas recall ou precision)",nargs='+',type=int)
parser.add_argument('--a',help="Algorimo para a avaliação",type=str)
parser.add_argument('--b',help="base de Dados para a avaliação",type=str)

parser.add_argument('--m',help="Modo de processamento [Janelada, Lote] (apenas recall)",type=str)
parser.add_argument('--j',help="Tamanho da Janela (apenas recall)",type=int)

parser.add_argument('--t',help="Quantidade de Threads para usar no calculo",type=int)

parser.add_argument('--p',help="Profiling da métrica",action="store_true")

parser.add_argument('--v',help="Verbose",action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()
    if args.c == 'recall':
        recall.main(args)
    elif args.c == 'precision':
        precision.main(args)
    elif args.c == 'ndcg':
        nDCG.main(args)
    elif args.c == 'mrr':
        mrr.main(args)
