#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define SERVER_PORT 1100           // Defina a porta do servidor
#define SERVER_ADDRESS "127.0.0.1" // Substitua pelo endereço do servidor

// Função que realiza a operação aritmética
double realizar_operacao(char *operacao, double operando1, double operando2)
{
    if (strcmp(operacao, "ADD") == 0)
    {
        return operando1 + operando2;
    }
    else if (strcmp(operacao, "SUB") == 0)
    {
        return operando1 - operando2;
    }
    else if (strcmp(operacao, "MUL") == 0)
    {
        return operando1 * operando2;
    }
    else if (strcmp(operacao, "DIV") == 0)
    {
        if (operando2 != 0)
            return operando1 / operando2;
        else
            return 0.0; // Divisão por zero
    }
    else
    {
        return 0.0; // Operação inválida
    }
}

void *handle_client(void *arg)
{
    int client_sock = *((int *)arg);
    char buffer[1024];
    char operacao[4];
    double operando1, operando2, resultado;

    // Receber a operação e operandos do cliente
    recv(client_sock, buffer, sizeof(buffer), 0);
    sscanf(buffer, "%s %lf %lf", operacao, &operando1, &operando2);

    // Realizar a operação aritmética
    resultado = realizar_operacao(operacao, operando1, operando2);

    // Enviar o resultado ao cliente
    sprintf(buffer, "%.2f", resultado);
    send(client_sock, buffer, strlen(buffer), 0);

    printf("Cliente desconectado...\n");
    // Fechar o socket do cliente
    close(client_sock);
}

int main()
{
    int server_sock, client_sock;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    pthread_t thread_id;

    // Criar o socket
    server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
    {
        perror("Erro ao criar socket");
        exit(1);
    }

    // Definir as informações do servidor
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(SERVER_PORT);

    // Vincular o socket ao endereço e porta
    if (bind(server_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("Erro ao vincular");
        close(server_sock);
        exit(1);
    }

    // Escutar conexões
    if (listen(server_sock, 5) < 0)
    {
        perror("Erro ao escutar");
        close(server_sock);
        exit(1);
    }

    printf("Servidor de operações aritméticas rodando na porta %d...\n", SERVER_PORT);

    // Aceitar conexões e tratar cada cliente em uma nova thread
    while (1)
    {
        client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_sock < 0)
        {
            perror("Erro ao aceitar conexão");
            continue;
        }
        printf("Cliente conectado...\n");

        if (pthread_create(&thread_id, NULL, handle_client, (void *)&client_sock) != 0)
        {
            perror("Erro ao criar thread");
        }
    }

    // Fechar o socket do servidor
    close(server_sock);
    return 0;
}