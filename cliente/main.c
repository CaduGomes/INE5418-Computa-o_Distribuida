#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define SERVER_PORT 1100 // Defina a porta do servidor
#define SERVER_ADDRESS "127.0.0.1" // Substitua pelo endereço do servidor

int main() {
    int sock;
    struct sockaddr_in server_addr;
    char message[1024];
    char buffer[1024];
    int bytes_received;

    // Criar o socket
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Erro ao criar socket");
        exit(1);
    }

    // Definir as informações do servidor
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_ADDRESS, &server_addr.sin_addr) <= 0) {
        perror("Endereço inválido/ não suportado");
        exit(1);
    }

    // Conectar ao servidor
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Erro ao conectar ao servidor");
        close(sock);
        exit(1);
    }

    // Solicitar entrada do usuário
    printf("Digite uma mensagem: ");
    fgets(message, sizeof(message), stdin);
    message[strcspn(message, "\n")] = '\0';  // Remover o '\n'

    // Enviar mensagem ao servidor
    send(sock, message, strlen(message), 0);

    // Receber mensagem de retorno (echo) do servidor
    bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received < 0) {
        perror("Erro ao receber resposta");
        close(sock);
        exit(1);
    }

    // Adicionar terminador nulo ao final da resposta e imprimir
    buffer[bytes_received] = '\0';
    printf("Resposta do servidor: %s\n", buffer);

    // Fechar o socket
    close(sock);
    return 0;
}
