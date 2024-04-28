//Include de les llibreries necessàries 
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <stdbool.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <signal.h>
#include <arpa/inet.h>
#include <ctype.h>

#define MAX_LENGTH 100

//Definició estctures
typedef struct {
    char name[20];
    char mac[13];
    int udpPort;
    int tcpPort;
} Server;

typedef struct {
    unsigned char pdu;
    char mac[13];
    char randomNumber[9];
    char data[80];
} PaquetUDP;

typedef struct {
    unsigned char pdu;
    char mac[13];
    char randomNumber[9];
    char dispositiu[8];
    char valor[7];
    char data[80];
} PaquetTCP;

typedef struct{
    char tipus[MAX_LENGTH];
    unsigned char valor;
} tipusPaquet;

typedef struct
{
    PaquetUDP paquet;
    struct sockaddr_in clientAddr;
    socklen_t clientLength;
} subsTrheadArgs;

typedef struct{
    char name[8];
    char valor[7];
} controller;

typedef struct {
    char name[20];
    char mac[13];
    char estat[20];
    char situacio[30];
    char elements[50];
    char *randAssignat;
    int tcpPort;
    float lastHelloSent;
    controller controllers[6];
} Client;

 //Definicó funcions utilitzades
void carregarTipusPaquets();
void readConfig();
void readServerConfig();
void createUdpSocket();
void createTcpSocket();
tipusPaquet getPackage(unsigned char value);
bool equals(char *str1, char *str2);
bool sendUDP(PaquetUDP package, struct sockaddr_in *clientAddr, socklen_t clientLength, int sock);
bool sendTCP(PaquetTCP package, int sock);
PaquetUDP crearPaquetUDP(tipusPaquet tipus, char rand[], char motiu[]);
PaquetTCP crearPaquetTCP(tipusPaquet tipus, char rand[], char motiu[], char dispositiu[], char valor[]);
char *getRandomNumber();
bool authorizedClient(char mac[], char estat[]);
void* readUDP(void * arg);
void createSubscriptionUdpSocket();
void sendSubsAck(int clientIndex, struct sockaddr_in addr_for_subscription , socklen_t addres_size);
void sigintHandler();
void quitServer();
void setElements(char *data, int indexClient);
void setElementsInArray(int indexClient, char *substring);
void getData(char *data, int indexClient);
void *sendHello(void *args);
int getNumeroClient(char mac[]);
void printClientTable();
void list();
void completeSubscription(int clientIndex, struct sockaddr_in subsricption_addr, socklen_t addres_size);
void *acceptSubscription(void *args);
void *keepAliveClient(void *args);
void readConsoleInput();
void printPaquetrebut(PaquetUDP package);
void printPaquetEnviat(PaquetUDP package);
void printPaquetEnviatTCP(PaquetTCP package);
void printPaquetRebutTCP(PaquetTCP package);
void udpToBytes(PaquetUDP *packet, char *bytes);
int kill(pid_t pid, int signal);
int wait(pid_t pid);
void *getClientData(void *args); 
bool existsSensor(char *nom, int indexClient);
bool writeFile(char *filename, char *dispositiu, char *valor, char *tipus);
void getTcpPort(char *data);
bool ctrlExists(char *ctrl);
int getClientFromCtrlr(char *name);
void setData(char *ctrlr, char *name, char *value);
void getClientDataTcp(char *ctrlr, char *name);
void createTcpDataSocket();
void getOrSet(char *buffer);


pthread_t threads[4];
tipusPaquet paquets[14];
Client clients[6];
//Variables globals
Server server;
int tcp_sock, udp_sock;
struct sockaddr_in tcp_server_addr, udp_server_addr;
int numeroClients;
int subscriptionUdpSocket; //UDP sock for each subscription
int clientSocket; //socket for each subscription
struct sockaddr_in subsricption_addr; //Struct for each subscritption
int udp_sock; //Main udp port
int dataTcpPort; //port for the transference data between server and client
int dataTcp; //fd for the transference between server and client
pid_t pidUDP;
pid_t pidAlive; 
bool created = false;
bool debugMode = false;

int main(int argc, char *argv[]) {
    if (argc == 2 && strcmp(argv[1], "-d") == 0) {
        debugMode = true;
    }
    //Definico del handler del SIGINT per acabar de manera controlada
    if(signal(SIGINT, sigintHandler) == SIG_ERR){
        exit(EXIT_FAILURE);
    }
    //Lectura de les configuracions i creació dels sockets
    srand(time(0));
    carregarTipusPaquets();
    readServerConfig();
    createUdpSocket();
    createTcpSocket();
    readConfig();
    //Creació del thread que s'encarregarà de gestionar els paquets UDP rebuts
    //pthread_t udp_thread;
    if(pthread_create(&threads[0], NULL, readUDP, NULL) < 0){
        perror("Error thread");
        exit(EXIT_FAILURE);
    }
    //Creacio del thread que comprovarà que els clients segueixin enviant paquets de HELLO
    //pthread_t keepAliveThread;
    if(pthread_create(&threads[1], NULL, keepAliveClient, NULL) < 0){
        perror("Error thread");
        exit(EXIT_FAILURE);
    }
    //Creacio del thread que llegirà les comandes de terminal
    //pthread_t readInputThread;
    /*if(pthread_create(&threads[2], NULL, readConsoleInput, NULL) < 0){
        perror("Error thread");
        exit(EXIT_FAILURE);
    }*/

    //pthread_t receiveClientData;
    if(pthread_create(&threads[3], NULL, getClientData, NULL) < 0){
        perror("Error thread");
        exit(EXIT_FAILURE);
    }
    readConsoleInput();

    pause();
    return 0;
}

//Lectura dels fitxer dels paquets per emmagtzemar-los a l'estructura
void carregarTipusPaquets(){
    FILE *archiuPaquets = fopen("paquets.txt", "r");
    if (archiuPaquets == NULL) {
        printf("No s'ha pogut obrir l'arxiu paquets.txt");
        exit(EXIT_FAILURE);
    }

    char linea[MAX_LENGTH];
    int index = 0;
    while (fgets(linea, sizeof(linea), archiuPaquets) != NULL && index < 14) {

        unsigned int valor;
        char nombre[MAX_LENGTH];
        if (sscanf(linea, "0x%x %s", &valor, nombre) == 2) {
            paquets[index].valor = (unsigned char)valor;
            strncpy(paquets[index].tipus, nombre, MAX_LENGTH - 1);
            paquets[index].tipus[MAX_LENGTH - 1] = '\0';
            index++;
        }
    }

    fclose(archiuPaquets);
}

//Creacio socket UDP
void createUdpSocket(){
    /*Crear socket UDP*/
    if ((udp_sock = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
        printf("Error al crear socket UDP");
        exit(EXIT_FAILURE);
    }

    /*Configurar port UDP*/
    memset(&udp_server_addr, 0, sizeof(udp_server_addr));
    udp_server_addr.sin_family = AF_INET;
    udp_server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    udp_server_addr.sin_port = htons(2018);

    /*Enllaçar socket UDP*/
    if (bind(udp_sock, (struct sockaddr *)&udp_server_addr, sizeof(udp_server_addr)) == -1) {
        printf("Error sokcet UDP\n");
        exit(EXIT_FAILURE);
    }	
}

//Creació del socket TCP
void createTcpSocket(){
    /*Crear socket TCP*/
    if ((tcp_sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        printf("Error al crear socket TCP");
        exit(EXIT_FAILURE);
    }

    /*Configurar port TCP*/
    memset(&tcp_server_addr, 0, sizeof(tcp_server_addr));
    tcp_server_addr.sin_family = AF_INET;
    tcp_server_addr.sin_addr.s_addr = INADDR_ANY;
    tcp_server_addr.sin_port = htons(server.tcpPort);
    

    /*Enllaçar socket TCP*/
    if (bind(tcp_sock, (struct sockaddr *)&tcp_server_addr, sizeof(tcp_server_addr)) == -1) {
        perror("Error socket TCP");
        exit(EXIT_FAILURE);
    }

    if (listen(tcp_sock, 6) == -1) {
        perror("Error al escoltar peticions TCP");
        exit(EXIT_FAILURE);
    }
}

//Lectura configuració controladros i s'emmagatzema en un estructura
void readConfig(){
    FILE *controllers = fopen("controllers.dat", "r");
    if (controllers == NULL) {
        printf("No s'ha pogut obrir l'arxiu controllers.dat");
        exit(EXIT_FAILURE);
    }
    printf("CONTROLADORS REGISTRATS: \n");
    char *token;
    char linea[MAX_LENGTH];
    int nameOrMac = 0;
    for(int i = 0; i < 6; i++){ 
        fgets(linea, MAX_LENGTH, controllers);
        linea[strcspn(linea, "\n")] = '\0';
        token = strtok(linea, ",");
        while (token != NULL) {
            if(nameOrMac == 0){
                strcpy(clients[numeroClients].name,token);
                printf("NAME: %s, ", clients[numeroClients].name);
                nameOrMac++;
            } else {
                strcpy(clients[numeroClients].mac,token);
                printf("MAC: %s\n", clients[numeroClients].mac);
                nameOrMac = 0;
            }
            token = strtok(NULL, ","); 
        }
        strcpy(clients[numeroClients].estat, "DISCONNECTED");
        clients[i].randAssignat = 0;
        strcpy(clients[i].elements, "");
        strcpy(clients[i].situacio, "");
        numeroClients++;
    }
    fclose(controllers);
}

//Lectrua de les dades del servidor
void readServerConfig(){
    FILE *config= fopen("server.cfg", "r");
    
    if (config == NULL) {
        printf("No s'ha pogut obrir l'arxiu server.cfg");
        exit(1);
    }
    char linea[MAX_LENGTH];
    while (fgets(linea, sizeof(linea), config)) {
        char *token = strtok(linea, "=");
        if (token != NULL) {
            if (strcmp(token, "Name ") == 0) {
                token = strtok(NULL, "\n");
                strcpy(server.name, token);
            } else if (strcmp(token, "MAC ") == 0) {
                token = strtok(NULL, "\n");
                strcpy(server.mac, token);
            } else if (strcmp(token, "UDP-port ") == 0) {
                token = strtok(NULL, "\n");
                server.udpPort = atoi(token);
            } else if (strcmp(token, "TCP-port ") == 0) {
                token = strtok(NULL, "\n");
                server.tcpPort = atoi(token);
            }
        }
    }
    fclose(config);
}

//Funció per gestionar els paquets UDP  que arriben
void* readUDP(void* arg){
    struct sockaddr_in clientAddr;
    socklen_t clientSize = sizeof(clientAddr);
    PaquetUDP package;
    while(true){ 
        if(recvfrom(udp_sock, &package, sizeof(PaquetUDP), 0, (struct sockaddr *) &clientAddr, &clientSize) < 0){
            printf("No s'ha pogut rebre el paquet UDP");
        } else {
            char *tipus = getPackage(package.pdu).tipus;
            printf("Rebut paquet UDP, creat proces per atendre'l\n");
            int clientIndex = getNumeroClient(package.mac);
            printPaquetrebut(package);
            if(equals(tipus, paquets[0].tipus)){ //En el cas de que el paquet rebut sigui SUBS_REQ
                pthread_t acceptSubscriptionThread; //Creació del thread que gestionarà el proces de subscripció
                if(authorizedClient(package.mac, "DISCONNECTED")){ //Comprobem que el client que envia les dades es trobi registrat i el seu estat siguie el corresponent
                    strcpy(clients[clientIndex].estat, "WAIT_INFO");
                    getData(package.data, clientIndex);
                    subsTrheadArgs *args = (subsTrheadArgs*)malloc(sizeof(subsTrheadArgs));//Guardem un espai de memoria per passar al thread l'estructura necessària
                    args->paquet = package;
                    args->clientAddr = clientAddr;
                    args->clientLength = clientSize;
                    if(pthread_create(&acceptSubscriptionThread, NULL, acceptSubscription, (void *) args) < 0){ //Continuació proces de subscripció
                        perror("Error thread");
                        exit(EXIT_FAILURE);
                    }
                } else {
                    PaquetUDP subs_rej = crearPaquetUDP(paquets[2], "00000000", "client no vàlid");
                    sendUDP(subs_rej, &clientAddr, clientSize, udp_sock);
                    strcpy(clients[clientIndex].estat, "DISCONNECTED");
                }
            } else if(equals(tipus, paquets[6].tipus)){ //En el cas de que el paquet rebut sigui SUBS_REQ
                pthread_t helloThread; //Thread que s'encarregarà de contestar el HELLO
                if(authorizedClient(package.mac, "SUBSCRIBED") || authorizedClient(package.mac, "SEND_HELLO")){ //Copmrobem que el client sigui autoritzat
                    subsTrheadArgs *args = (subsTrheadArgs*)malloc(sizeof(subsTrheadArgs));
                    args->paquet = package;
                    args->clientAddr = clientAddr;
                    args->clientLength = clientSize;
                    if(pthread_create(&helloThread, NULL, sendHello, (void *) args) < 0){ //Enviament HELLO
                        perror("Error thread");
                        exit(EXIT_FAILURE);
                    }
                }
            } else if(equals(tipus,paquets[7].tipus)){ //En cas de rebre HELLO_REJ es desconectarà el client
                strcpy(clients[clientIndex].estat, "DISCONNECTED");
            }
        }
    }
}

void *acceptSubscription(void *args){
    subsTrheadArgs *arg = (subsTrheadArgs *) args; //Es desempaquet l'estructura passada al thread per poder accedir als camps
    createSubscriptionUdpSocket();
    int clientIndex = getNumeroClient(arg->paquet.mac);
    if (clientIndex < 0){
        PaquetUDP paquet = crearPaquetUDP(paquets[2], "00000000", "client no vàlid" ); //Si  el client no existeix s'envia un SUBS_REJ
        sendUDP(paquet, &arg->clientAddr, arg->clientLength, udp_sock);
    } else {
        sendSubsAck(clientIndex, arg->clientAddr, arg->clientLength); //En cas de que sigui correcte es seguiex amb el proces de subscripció
    }
    return NULL;
}

 //Es mostra per pantalla el paquet que entra com a paràmetre i que s'ha rebut previament
void printPaquetrebut(PaquetUDP package){
    time_t rawtime;
    time(&rawtime);

    //Fiquem el temps actual en un format que es pugui mostrar
    struct tm *timeinfo;
    char buffer[80];
    timeinfo = localtime(&rawtime);
    strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
    if(debugMode){
        printf("%s DEBUG: => Paquet rebut amb pdu: %s, mac: %s, random: %s, data: %s\n", buffer, getPackage(package.pdu).tipus, package.mac, package.randomNumber, package.data);
    } else {
        printf("%s MSG: => Paquet rebut amb pdu: %s\n", buffer, getPackage(package.pdu).tipus);
    }
}

//Es mostra per pantalla el paquet que entra com a paràmetre i que s'ha enviat
void printPaquetEnviat(PaquetUDP package){
    time_t rawtime;
    time(&rawtime);

    //Fiquem el temps actual en un format que es pugui mostrar
    struct tm *timeinfo;
    char buffer[80];
    timeinfo = localtime(&rawtime);
    strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
    if(debugMode){
        printf("%s DEBUG: => Paquet enviat amb pdu: %s, mac: %s, random: %s, data: %s\n", buffer,  getPackage(package.pdu).tipus, package.mac, package.randomNumber, package.data);
    } else {
        printf("%s MSG: => Paquet rebut amb pdu: %s\n", buffer, getPackage(package.pdu).tipus);
    }
}

void printPaquetEnviatTCP(PaquetTCP package){
    time_t rawtime;
    time(&rawtime);

    //Fiquem el temps actual en un format que es pugui mostrar
    struct tm *timeinfo;
    char buffer[80];
    timeinfo = localtime(&rawtime);
    strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
    if(debugMode){
        printf("%s DEBUG: => Paquet enviat amb pdu: %s, mac: %s, random: %s, dispositiu: %s, valor: %s, data: %s\n", buffer,  getPackage(package.pdu).tipus, package.mac, package.randomNumber,package.dispositiu, package.valor, package.data);
    } else {
        printf("%s MSG: => Paquet rebut amb pdu: %s\n", buffer, getPackage(package.pdu).tipus);
    }
}

void printPaquetRebutTCP(PaquetTCP package){
    time_t rawtime;
    time(&rawtime);

    //Fiquem el temps actual en un format que es pugui mostrar
    struct tm *timeinfo;
    char buffer[80];
    timeinfo = localtime(&rawtime);
    strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
    if(debugMode){
        printf("%s DEBUG: => Paquet rebut amb pdu: %s, mac: %s, random: %s, dispositiu: %s, valor: %s, data: %s\n", buffer,  getPackage(package.pdu).tipus, package.mac, package.randomNumber,package.dispositiu, package.valor, package.data);
    } else {
        printf("%s MSG: => Paquet rebut amb pdu: %s\n", buffer, getPackage(package.pdu).tipus);
    }
}

//Rep com a paràmetre el valor d'un paquet i retornar el tipus consultant a l'estructura de paquets, en cas de que no existeixi es retorna ERROR
tipusPaquet getPackage(unsigned char value){
    for(int i = 0; i < 15; i ++){
        if(paquets[i].valor == value){
            return paquets[i];
        }
    }
    tipusPaquet paquetInexistent;
    strcpy(paquetInexistent.tipus, "ERROR");
    paquetInexistent.valor = 99;
    return paquetInexistent;
}

//Funció per poder compara string de manera més fàcil
bool equals(char *str1, char *str2) {
    return strcmp(str1, str2) == 0;
}

//Es comproba que el client amb la mac i estat que entren per paràmetre està registrat a l'estructura amb aquestes dades
bool authorizedClient(char mac[], char estat[]){
    for(int i = 0; i < numeroClients; i++){
        /*printf("MAC CLIENT: %s\n", clients[i].mac);
        printf("MAC REBUDA: %s\n", mac);
        printf("CLIENT ESTAT: %s\n", clients[i].estat);*/
        if(equals(clients[i].mac, mac) && equals(clients[i].estat, estat)){
            return true;
        }
    } 
    return false;
}

//Es crea el socket per finalitzar el proces de subscripció
void createSubscriptionUdpSocket(){
    struct sockaddr_in addr;
    /*Crear socket UDP*/
    if ((subscriptionUdpSocket = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
        printf("Error al crear socket UDP");
        exit(EXIT_FAILURE);
    }
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(0);
    /*Enllaçar socket UDP*/
    if (bind(subscriptionUdpSocket, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        printf("Error sokcet UDP");
        perror("ERROR");
        exit(EXIT_FAILURE);
    }	
}

//Es contesta el SUBS_REQ rebut
void sendSubsAck(int clientIndex, struct sockaddr_in addr_for_subscription , socklen_t addres_size){
    char *newRand = getRandomNumber(); //Es genera un numero aleatori pel client
    struct sockaddr_in newAddress;
    socklen_t addr_len = sizeof(newAddress);
    if (getsockname(subscriptionUdpSocket, (struct sockaddr *)&newAddress, &addr_len) < 0) { //Es guarden les dades del socket creat
        perror("Error al obtener la dirección del socket");
        exit(EXIT_FAILURE);
    }
    char portString[6];
    sprintf(portString, "%hu", ntohs(newAddress.sin_port)); //Es crea un array de caracters per poder enviar el nou port al client
    PaquetUDP paquetSubsAck = crearPaquetUDP(paquets[1], newRand, portString); //Es crea un paquet amb el nou numero aleatori i el port a les dades
    PaquetUDP paquetRebut;
    clients[clientIndex].randAssignat = newRand; //S'assigna el numero aleatori al client
    sendUDP(paquetSubsAck, &addr_for_subscription, addres_size, udp_sock);
    recvfrom(subscriptionUdpSocket, &paquetRebut, sizeof(PaquetUDP), 0, (struct sockaddr *) &newAddress, &addr_len);
    getTcpPort(paquetRebut.data);
    setElements(paquetRebut.data, clientIndex); //Al paquet es rep la informacio dels elements dels controladros i es guarden al registre de clients
    printPaquetrebut(paquetRebut);
    if(authorizedClient(paquetRebut.mac, "WAIT_INFO") && equals(clients[clientIndex].randAssignat, paquetRebut.randomNumber) && paquetRebut.pdu == paquets[3].valor){ //En cas de que s'hagi rebut SUBS_INFO es continua amb el proces de subscripcio
        completeSubscription(clientIndex, addr_for_subscription, sizeof(addr_for_subscription));
    } else {
        PaquetUDP subs_rej = crearPaquetUDP(paquets[2], "00000000", "client no vàlid");
        sendUDP(subs_rej, &addr_for_subscription, addres_size, udp_sock);
        close(subscriptionUdpSocket);
    }
}

//Es finalitza el proces de subsripció
void completeSubscription(int clientIndex, struct sockaddr_in subsricption_addr, socklen_t addres_size){
    strcpy(clients[clientIndex].estat, "SUBSCRIBED"); //Es canvia l'estat del client
    struct sockaddr_in newAddress;
    socklen_t addr_len = sizeof(newAddress);
    if (getsockname(tcp_sock, (struct sockaddr *)&newAddress, &addr_len) < 0) { //Es guarden les dades del socket creat
        perror("Error al obtener la dirección del socket");
        exit(EXIT_FAILURE);
    }
    char portString[6];
    sprintf(portString, "%hu", ntohs(newAddress.sin_port)); //S'envia el port TCP al camp de dades
    PaquetUDP paquetInfoAck = crearPaquetUDP(paquets[4], clients[clientIndex].randAssignat, portString); 
    sendUDP(paquetInfoAck, &subsricption_addr, addres_size, udp_sock); //S'envia el paquet de INFO_ACK i finalitza el proces de subscripció
}

//Es gestiona el paquet de HELLO rebut
void *sendHello(void *args){
    subsTrheadArgs *arg = (subsTrheadArgs*) args; //Es desempaquet l'esctructura del thread
    int indexClient = getNumeroClient(arg->paquet.mac);
    PaquetUDP helloSent = crearPaquetUDP(paquets[6], clients[indexClient].randAssignat, ""); 
    strcpy(helloSent.data, arg->paquet.data);
    if(equals(clients[indexClient].estat, "SEND_HELLO")){ //Si el client estaba en l'estat SEND_HELLO simplement s'envia el paquet i es reinicia el comptador
        clients[indexClient].lastHelloSent = 0;
        sendUDP(helloSent, &arg->clientAddr, arg->clientLength, udp_sock);
    } else if(equals(clients[indexClient].estat, "SUBSCRIBED")){ //Si el client estaba en SUBSCRIBED, es passa a SEND_HELLO i s'inicia el comptador
        strcpy(clients[indexClient].estat, "SEND_HELLO");
        clients[indexClient].lastHelloSent = 0;
        sendUDP(helloSent, &arg->clientAddr, arg->clientLength, udp_sock);
    }
    return NULL;
}

//Funció per comprobar que els clients segueixen enviant paquets de HELLO
void *keepAliveClient(void *args){
    while (true){
        sleep(1); //El sleep d'1 segon ens ajuda a comptar els paquets de HELLO no rebuts
        for(int i = 0; i < numeroClients; i++){
            if(clients[i].lastHelloSent > 6 && equals(clients[i].estat, "SEND_HELLO")){ //Comprobem que es mirin només el clients que es troben en l'estat de SEND_HELLO
                strcpy(clients[i].estat, "DISCONNECTED"); //Si no esta enviant paquets es desconecta el client
                clients[i].randAssignat = 0;
                strcpy(clients[i].situacio, "");
                strcpy(clients[i].elements, "");
                time_t rawtime;
                time(&rawtime);

                struct tm *timeinfo;
                char buffer[80];
                timeinfo = localtime(&rawtime);
                strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
                printf("%s Client amb nom: %s, desconnectat ja que no s'ha rebut HELLO en més de 6 segons\n",buffer, clients[i].name);
            }
            clients[i].lastHelloSent++;
        }
    }
    
}

//Es llegeixe les comandes de terminal
void readConsoleInput(){
    char buffer[MAX_LENGTH];
    char action[20];
    char ctrlr[20];
    char name[20]; 
    char value[20];
    while (true){
        fgets(buffer, MAX_LENGTH, stdin);
        sscanf(buffer, "%s %s %s %s", action, ctrlr, name, value);
        if(equals(action, "list")){
            list();
        } else if(equals(action, "quit")){
            quitServer();
        } else if(equals(action, "set")){
            setData(ctrlr, name, value);
        } else if(equals(action, "get")){
            getClientDataTcp(ctrlr, name);
        } else {
            printf("Comanda no vàlida: %s\n", buffer);
        }
    }
}


//Es crea el paquet UDP amb les dades que entren per paràmetre
PaquetUDP crearPaquetUDP(tipusPaquet tipus, char rand[], char motiu[]){
    PaquetUDP paquet;
    //strcpy(paquet.mac, server.mac);
    strncpy(paquet.mac, "21AE345FD321", 13);
    strncpy(paquet.randomNumber, rand, 9);
    strncpy(paquet.data, motiu, 80);
    paquet.pdu = tipus.valor;
    return paquet;
}

//A partir d'una mac es retornar en quin posició de clients està registrat
int getNumeroClient(char mac[]){
    for(int i = 0; i < numeroClients; i++){
        if(equals(clients[i].mac, mac)){
            return i;
        }
    }
    return -1;
}

//Retorna un numero aleatori
char *getRandomNumber() {
    char *random = malloc(9);
    int randomNum = rand() % 99999999;
    sprintf(random, "%08d", randomNum);
    return random;
}

//Es guarda a l'estructura del client corresponent la situació
void getData(char *data, int indexClient){
    char *substring;
    substring = strchr(data, ',');
    substring++;
    strcpy(clients[indexClient].situacio, substring);
}

//Es guarden els elementns al client corresponent
void setElements(char *data, int indexClient){
    char *substring;
    substring = strchr(data, ',');
    substring++;
    strcpy(clients[indexClient].elements, substring);
    setElementsInArray(indexClient, substring);
}

void setElementsInArray(int indexClient, char *substring){
    char *token = strtok(substring, ";");
    int i = 0;
    while (token != NULL) {
        strcpy(clients[indexClient].controllers[i].name, token);
        token = strtok(NULL, ";");
        i++;
    }
}

//Es mostren per pantalla els controladors al rebre la comanda list
void list(){
    printf("| NOM          | DIRECCIÓ IP  | MAC           | RAND ASSIGNAT | ESTAT         | SITUACIÓ      | ELEMENTS                                |\n");
    printf("+--------------+--------------+---------------+---------------+---------------+---------------+-----------------------------------------+\n");
    printClientTable();
}

void printClientTable() {
    for (int i = 0; i < numeroClients; i++) {
        printf("| %-12s | %-12s | %-13s | %-13s | %-13s | %-13.12s | %-39s |\n", //Configuració per deixar els espais necessaria a l'hora de imprimir la taula per pantalla
               clients[i].name, "127.0.0.1", clients[i].mac, clients[i].randAssignat,
               clients[i].estat, clients[i].situacio, clients[i].elements);
    }
}

//en rebre un quit es finalitza el programa tancat els sockets
void quitServer(){
    printf("quit rebut, finalizant processos i sortint\n");
    close(udp_sock);
    close(subscriptionUdpSocket);
    close(tcp_sock);
    exit(0);
}

//En rebre Ctrl+C es tanquen els sockets i es finalitza
void sigintHandler(){
    printf(" Ctrl+C rebut, finalitzant...\n"); 
    close(udp_sock);
    close(subscriptionUdpSocket);
    close(tcp_sock);
    exit(0);
}


//S'empaqueta un paquet UDP en una estructura de Bytes per enviar-la pel socket i que la pugui rebre el client
void udpToBytes(PaquetUDP *packet, char *bytes) {
    int offset = 0;
    bytes[offset] = packet->pdu;
    offset += sizeof(packet->pdu);
    memcpy(bytes + offset, packet->mac, sizeof(packet->mac));
    offset += sizeof(packet->mac);
    memcpy(bytes + offset, packet->randomNumber, sizeof(packet->randomNumber));
    offset += sizeof(packet->randomNumber);
    memcpy(bytes + offset, packet->data, sizeof(packet->data));
}

//S'envia el paquet desitjat al socket amb la direcció que entra per paràmetre
bool sendUDP(PaquetUDP package, struct sockaddr_in *clientAddr, socklen_t clientLength, int sock) {
    char paquetArray[103];
    udpToBytes(&package, paquetArray);
    if (sendto(sock, paquetArray, sizeof(paquetArray), 0 , (struct sockaddr *) clientAddr, clientLength) < 0) { //Si no es pot enviar es mostra missatge d'error
        printf("UDP error: couldn't send %s\n", getPackage(package.pdu).tipus);
        perror("ERROR");
        return false;
    } else {
        printPaquetEnviat(package);
        return true;
    }
}

//S'accepten connexions dels clients per a que enviin dades
void *getClientData(void *args) {
    socklen_t clientAddrLen = sizeof(tcp_server_addr);
    while(true){
        if ((clientSocket = accept(tcp_sock, (struct sockaddr *)&tcp_server_addr, &clientAddrLen)) == -1) {
            perror("Error al acceptar la solicitud d'un client");
            continue;
        }
        PaquetTCP paquet;
        if( recv(clientSocket, &paquet, sizeof(paquet), 0) < 0){
            perror("No s'han pogut rebre les dades del client");
        }
        int clientIndex = getNumeroClient(paquet.mac);
        if(authorizedClient(paquet.mac, "SEND_HELLO") && equals(getPackage(paquet.pdu).tipus, "SEND_DATA") && existsSensor(paquet.dispositiu, clientIndex)){ 
            char filename[55];
            sprintf(filename, "%s-%s.data", clients[clientIndex].name, clients[clientIndex].situacio);
            if(!writeFile(filename, paquet.dispositiu, paquet.valor, getPackage(paquet.pdu).tipus)){
                PaquetTCP dataNack = crearPaquetTCP(paquets[12], clients[clientIndex].randAssignat, "no s'ha pogut escriure al fitxer", paquet.dispositiu, paquet.valor);
                sendTCP(dataNack, clientSocket);
            } else {
                PaquetTCP dataAck = crearPaquetTCP(paquets[11], clients[clientIndex].randAssignat, "valor rebut", paquet.dispositiu, paquet.valor);
                sendTCP(dataAck, clientSocket);
            }
        } else{
            PaquetTCP dataRej = crearPaquetTCP(paquets[13], clients[clientIndex].randAssignat, "dades incorrectes", paquet.dispositiu, paquet.valor);
            sendTCP(dataRej, clientSocket);
            strcpy(clients[clientIndex].estat, "DISCONNECTED");
        }
        close(clientSocket);
    }
}

//Es crear una estructura de tipus paquet TCP
PaquetTCP crearPaquetTCP(tipusPaquet tipus, char rand[], char motiu[], char dispositiu[], char valor[]){
    PaquetTCP paquet;
    //strcpy(paquet.mac, server.mac);
    strncpy(paquet.mac, "21AE345FD321", 13);
    strncpy(paquet.randomNumber, rand, 9);
    strncpy(paquet.data, motiu, 80);
    strncpy(paquet.dispositiu, dispositiu, 8);
    strncpy(paquet.valor, valor, 7);
    paquet.pdu = tipus.valor;
    return paquet;
}

//Es passa un paquet TCP a bytes per enviar-lo pel socket i que el client el pugui decodificar fàcilment
void tcpToBytes(PaquetTCP *packet, char *bytes) {
    int offset = 0;
    bytes[offset] = packet->pdu;
    offset += sizeof(packet->pdu);
    memcpy(bytes + offset, packet->mac, sizeof(packet->mac));
    offset += sizeof(packet->mac);
    memcpy(bytes + offset, packet->randomNumber, sizeof(packet->randomNumber));
    offset += sizeof(packet->randomNumber);
    memcpy(bytes + offset, packet->dispositiu, sizeof(packet->dispositiu));
    offset += sizeof(packet->dispositiu);
    memcpy(bytes + offset, packet->valor, sizeof(packet->valor));
    offset += sizeof(packet->valor);
    memcpy(bytes + offset, packet->data, sizeof(packet->data));
}

//S'envia un paquet de tipud TCP al socket indicat
bool sendTCP(PaquetTCP package, int sock) {
    char paquetArray[118];
    tcpToBytes(&package, paquetArray);
    if (send(sock, paquetArray, sizeof(paquetArray), 0) < 0) { //Si no es pot enviar es mostra missatge d'error
        printf("TCP error: couldn't send %s\n", getPackage(package.pdu).tipus);
        perror("ERROR");
        return false;
    } else {
        printPaquetEnviatTCP(package);
        return true;
    }
}

//A partir d'un nom de sensor i del numero de client que l'hauria de contenir, es comprova si existeix
bool existsSensor(char nom[], int indexClient){
    bool exists = true;
    char *sensor;
    for(int i = 0; i < 6; i++){
        exists = true;
        sensor =  clients[indexClient].controllers[i].name;
        for(int j = 0; j < strlen(sensor); j++){
            if(sensor[j] != nom[j]){
                exists = false;
            }   
        }
        if(exists == true){
            return exists;
        }
    }
    return exists;
}

//S'escriuen al fitxer les dades rebudesd amb el format adequat
bool writeFile(char *filename, char *dispositiu, char *valor, char *tipus){
    char *token = strtok(filename," ");
    strcat(token,".data");
    FILE *dataFile = fopen(token, "a");
    if (dataFile == NULL) {
        perror("No s'ha pogut crear l'arxiu per escriure les dades");
    }
    time_t temps_actual;
    struct tm *temps_info;
    char data_hora[20];

    time(&temps_actual);
    temps_info = localtime(&temps_actual);
    strftime(data_hora, sizeof(data_hora), "%Y-%m-%d,%H:%M:%S", temps_info);
    char completeData[MAX_LENGTH];
    sprintf(completeData, "%s;%s;%s;%s", data_hora, tipus, dispositiu, valor);
    if(fprintf(dataFile, "%s\n", completeData) < 0){
        fclose(dataFile);
        return false;
    } else {
        fclose(dataFile);
        return true;
    }
}

//Si es rep la comadna get, i el nom del controlador i el sensor existeixen, s'envia un paquet TCP al client i la resposta en cas de que sigui existosa s'escriu al fitxer
void getClientDataTcp(char *ctrlr, char *name){
    int numClient = getClientFromCtrlr(ctrlr);
    PaquetTCP paquetRebut;
    createTcpDataSocket();
    if(existsSensor(name, numClient) && numClient < 6 && numClient >= 0){
        PaquetTCP getData = crearPaquetTCP(paquets[10], clients[numClient].randAssignat, "", name, "");
        sendTCP(getData,dataTcp);
        if(recv(dataTcp,&paquetRebut, sizeof(paquetRebut),0) < 0){
            printf("Operació fallida\n");
        } else {
            printPaquetRebutTCP(paquetRebut);
            if(authorizedClient(paquetRebut.mac, "SEND_HELLO") && equals(getPackage(paquetRebut.pdu).tipus,"DATA_ACK")){
                char filename[55];
                sprintf(filename, "%s-%s.data", clients[numClient].name, clients[numClient].situacio);
                if(!writeFile(filename, paquetRebut.dispositiu, paquetRebut.valor, getPackage(getData.pdu).tipus)){
                    printf("No s'han pogut escriure les dades al fitxer");
                }
            } else if(equals(getPackage(paquetRebut.pdu).tipus, "DATA_NACK")){
                printf("Operació fallida, DATA_NACK rebut\n");
            } else {
                strcpy(clients[numClient].estat, "DISCONNECTED");
            }
        }
    } else{
        printf("Us: get <nom-controlador> <nom-dispositiu>\n");
    }
    close(dataTcp);
    readConsoleInput();
}

//Si es rep la comanda set, s'envia el paquet al client i la resposta s'escriu al fitxer
void setData(char *ctrlr, char *name, char *value){
    int numClient = getClientFromCtrlr(ctrlr);
    PaquetTCP paquetRebut;
    createTcpDataSocket();
    if(name[strlen(name)- 1] == 'I'){
        if(existsSensor(name, numClient) && numClient < 6 && numClient >= 0){
            PaquetTCP setData = crearPaquetTCP(paquets[9], clients[numClient].randAssignat, "", name, value);
            sendTCP(setData,dataTcp);
            if(recv(dataTcp,&paquetRebut, sizeof(paquetRebut),0) < 0){
                printf("Operació fallida\n");
            } else {
                printPaquetRebutTCP(paquetRebut);
                if(authorizedClient(paquetRebut.mac, "SEND_HELLO") && equals(getPackage(paquetRebut.pdu).tipus,"DATA_ACK")){
                    char filename[55];
                    sprintf(filename, "%s-%s.data", clients[numClient].name, clients[numClient].situacio);
                    if(!writeFile(filename, paquetRebut.dispositiu, paquetRebut.valor, getPackage(setData.pdu).tipus)){
                        printf("No s'han pogut escriure les dades al fitxer");
                    }
                } else if(equals(getPackage(paquetRebut.pdu).tipus, "DATA_NACK")){
                    printf("Operació fallida, DATA_NACK rebut\n");
                } else {
                    strcpy(clients[numClient].estat, "DISCONNECTED");
                }
            }
        } else{
            printf("Us: set <nom-controlador> <nom-dispositiu> <valor>\n");
        }
    } else {
        printf("L'element anomenat: %s és un sensor i no permet establir el seu valor\n", name);
    }
    close(dataTcp);
    readConsoleInput();
}

//Es rep la posició del client en el array a partir del seu nom
int getClientFromCtrlr(char *ctrlr){
    for(int i = 0; i < 6; i++){
        if(equals(clients[i].name, ctrlr)){
            return i;
        }
    }
    return -1;
}

//Es comproba si existeix un client a partir del seuu nom
bool ctrlExists(char *ctrl){
    for(int i = 0; i < 6; i++){
        if(equals(clients[i].name, ctrl)){
            return true;
        }
    }
    return false;
}

//A partir de les dades rebudes en el paquet es guardar el valor del port TCP per la connexio
void getTcpPort(char *data){
    char numero[20];
    int i = 0;
    for (i = 0; data[i] != '\0'; ++i) {
        if (!isdigit(data[i])) {
            break;
        }
    }
    strncpy(numero, data, i);
    numero[i] = '\0';
    dataTcpPort = atoi(numero);
}

//Es crea el socket per enviar les comandes de get i set al client
void createTcpDataSocket(){
    created = true;
    struct sockaddr_in dataTcpAddr;
    if ((dataTcp = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        printf("Error al crear socket TCP");
        exit(EXIT_FAILURE);
    }

    /*Configurar port TCP*/
    memset(&dataTcpAddr, 0, sizeof(dataTcpAddr));
    dataTcpAddr.sin_family = AF_INET;
    dataTcpAddr.sin_addr.s_addr = INADDR_ANY;
    dataTcpAddr.sin_port = htons(dataTcpPort);
    

    /*Enllaçar socket TCP*/
    if (connect(dataTcp, (struct sockaddr *)&dataTcpAddr, sizeof(dataTcpAddr)) == -1) {
        perror("Error socket TCP");
        exit(EXIT_FAILURE);
    }
}

