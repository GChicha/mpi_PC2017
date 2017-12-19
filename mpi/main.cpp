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

#include <unistd.h>

#define N_COLUNAS 59

using namespace std;

struct Classificacao{
    float distancia;
    string classe;

    bool operator<(const Classificacao& c) const {
        return distancia > c.distancia;
    }

    Classificacao() : distancia(0), classe("") {}
};

typedef struct {
    array<double, N_COLUNAS> valor;
    string classe;
} Linha;


vector<Linha> *fazer(ifstream &file){
    string linha, valor;

    vector<Linha> *a = new vector<Linha>;

    while(getline(file, linha)){
        stringstream strA(linha);

        Linha l;
        int pos = 0;
        while (getline(strA, valor, ',') && pos < N_COLUNAS) {
            l.valor[pos] = atof(valor.c_str());
            pos++;
        }
        l.classe = valor;

        a->push_back(l);
    }

    return a;
}

void classificar(Linha x, vector<Linha> *B) {

    vector<Classificacao> ks;
    ks.resize(B->size());

    Classificacao *pos = &ks[0];

    for (const auto &linha : *B) {
        pos->classe = linha.classe;

        for (int i = 0; i < linha.valor.size(); ++i)
            pos->distancia += abs((x.valor.at(i) - linha.valor.at(i)));

        pos += sizeof(Classificacao);
    }
    sort(ks.begin(), ks.end());
}

int main(int argc, char** argv) {

    int i, rank, size;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int status_fim = 0;

    if (rank == 0) {
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

        int BSize = ceil((float)B->size()/size);

        for (int i = 1; i < size; i++)
            MPI_Send(&BSize, 1, MPI_INT, i, 0, MPI_COMM_WORLD);

        MPI_Scatter(&B->front(), BSize * sizeof(Linha), MPI_BYTE, &B->front(), BSize * sizeof(Linha), MPI_BYTE, 0, MPI_COMM_WORLD);

        for (const auto &linha : *X) {
            for (int i = 1; i < size; i++)
                MPI_Send(&status_fim, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            for (int i = 1; i < size; i++)
                MPI_Send(&linha, sizeof(linha), MPI_BYTE, i, 0, MPI_COMM_WORLD);
        }
        status_fim = 1;

        for (int i = 1; i < size; i++)
            MPI_Send(&status_fim, 1, MPI_INT, i, 0, MPI_COMM_WORLD);

        arqB.close();
        arqX.close();
    }
    else {
        int BSize;
        MPI_Recv(&BSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        vector<Linha> B;
        B.resize(BSize);

        MPI_Scatter(&B.front(), BSize * sizeof(Linha), MPI_BYTE, &B.front(), BSize * sizeof(Linha), MPI_BYTE, 0, MPI_COMM_WORLD);

        Linha linha;

        MPI_Recv(&status_fim, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        while(!status_fim) {
            MPI_Recv(&linha, sizeof(linha), MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            classificar(linha, &B);
            MPI_Recv(&status_fim, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }


    MPI_Finalize();
    return 0;
}
