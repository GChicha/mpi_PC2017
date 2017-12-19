#include <cmath>
#include <cstdlib>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <vector>
#include <iostream>
#include <string>
#include <sstream>
#include <mpi.h>

using namespace std;

typedef struct classificacao{
    float distancia;
    string classe;

    bool operator<(const classificacao& c) const {
        return distancia > c.distancia;
    }
} Classificacao;

typedef struct {
    vector<double> valor;
    string classe;
} Linha;


vector<Linha> *fazer(ifstream &file){
    string linha, valor;

    vector<Linha> *a = new vector<Linha>;

    while(getline(file, linha)){
        stringstream strA(linha);

        Linha l;
        while (getline(strA, valor, ','))
			l.valor.push_back(atof(valor.c_str()));
        l.classe = valor;

        a->push_back(l);
    }

    return a;
}

void classificar(vector<Linha> X, vector<Linha> B, int rank, int size){
    int linhas = 0;    

    for(vector<Linha>::iterator itX = X.begin()+rank; itX < X.end(); itX+=size) {

        vector<Classificacao> ks;
        ks.resize(10);

        for(vector<Linha>::iterator itB = B.begin(); itB < B.end(); itB++) {

            Classificacao c;
            c.distancia = 0;
            c.classe = itB->classe;

            for (int i = 0; i < itB->valor.size(); ++i)
                c.distancia += abs((itX->valor.at(i) - itB->valor.at(i)));

            ks.push_back(c);
            sort(ks.begin(), ks.end());
            ks.pop_back();
        }
        linhas++;
    }
    cout << "Processo[" << rank << "] leu " << linhas << "linhas" << endl;
}

int main(int argc, char** argv) {

    int i, rank, size;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    string nomeArqB = "train_59.data",
           nomeArqX = "test_59.data";
    ifstream arqB, arqX;

    arqB.open(nomeArqB);
    arqX.open(nomeArqX);

    if(!arqB.is_open() || !arqX.is_open()){
        MPI_Finalize();
        return 0;
    }

    vector<Linha> *B = fazer(arqB),
                  *X = fazer(arqX);

    MPI_Status st;

    int x[size],
        tam = X->size() / (size-1),
        resto = X->size() % (size-1);
    for (int i = 1; i < size; ++i) {
        if (resto > 0){
            x[i] = tam + resto;
            resto--;
        } else 
            x[i] = tam;
    }
    printf("%d dividido em %d é igual a %d com resto %d\n", X->size(), size-1, tam, resto);

    long int tamByteSize = X->size() * sizeof(Linha);
    printf("tamanho estimado: %ld", tamByteSize);

    if(rank == 0){
	for (int i = 1; i < size; i++){
            MPI_Send(&X[0], tamByteSize, MPI_BYTE, i, 0, MPI_COMM_WORLD);
//	    printf("processo %d enviou %d\n", rank, i);
	}
    }else{
	MPI_Recv(&X[0], tamByteSize, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &st);
//	printf("processo %d recebeu %d\n", rank, val);
    }

    arqB.close();
    arqX.close();

    MPI_Finalize();
    return 0;
}
