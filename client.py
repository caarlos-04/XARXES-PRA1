#!/usr/bin/env python3

import socket
import threading
import time
import struct
import sys
import os
import signal
from clases.clases import Client
import subprocess

#Diccionaris per distingir els dieferents tipus de paquets, un altre per plenar amb els valors del controlador i finalment una llista amb les possibles accions
subs_packages = {"SUBS_REQ" : "00", "SUBS_ACK" : "01", "SUBS_REJ" : "02", "SUBS_INFO" : "03", "INFO_ACK" : "04", "SUBS_NACK" : "05"}
hello_packages = {"HELLO" : "10", "HELLO_REJ" : "11"}
data_packages = {"SEND_DATA" : "20",  "SET_DATA" : "21",  "GET_DATA" : "22",  "DATA_ACK" : "23", "DATA_NACK" : "24",  "DATA_REJ" : "25"}
valores = {}
accions = ["stat", "send", "set", "quit"]

#Temporitzadors i valors ja establerts
T,U,N,O,P,Q = 1, 2, 7, 3, 3, 3

def main():
    #Es llegeix la configuracio de l'arxiu i s'obra el socket per a connexio UDP
    global SERVER_ADDR, server_traduit
    read_config()
    setup()
    server_traduit = server_ip
    if server_ip == 'localhost':
        server_traduit = '127.0.0.1'
    SERVER_ADDR = (server_traduit, udp)
    global numProcessos
    numProcessos = 0
    routine()

def setup():
    global sockUdp
    sockUdp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sockUdp.bind((server_ip, 0))
    #Tractament de senyals entre processos
    signal.signal(signal.SIGUSR1, sigusr_handler)
    #signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGUSR2, sigusr2_handler)

def read_config():
    global readFromServerTcp, server_ip, udp, client, elements
    if len(sys.argv) >= 3 and sys.argv[1] == "-c":
        file_path = sys.argv[2]
    else:
        file_path = 'client.cfg'
    with open(file_path, 'r') as file:
        lines = file.readlines()

    name = lines[0].strip().split(' = ')[1]
    situation = lines[1].strip().split(' = ')[1]
    elements = lines[2].strip().split(' = ')[1]
    #Els elements s'afegeixen al diccionari buit amb el valor "None" aixi més endavant es podran modificar
    addToDict(elements)
    mac = lines[3].strip().split(' = ')[1]
    readFromServerTcp = int(lines[4].strip().split(' = ')[1])
    server_ip = lines[5].strip().split(' = ')[1]
    udp = int(lines[6].strip().split(' = ')[1])
    #Es crea una instancia de client importat de la classe client
    client = Client(name, situation, mac)
    
def addToDict(elements_str):
    global valores
    for element in elements_str.split(";"):
        valores[element] = None
    
def routine():
    #Es fa un fork, el proces pare s'encarrega de arribar a l'estat SEND_HELLO i mantenir comunicació periòdica
    #El fill es el que llegira de la terminal i enviara dades al servidor
    #El que fem per arribar a la comunicacio periodica es concatenar una serie de funcions que retornen un bool, de manera que si tot va be
    #mantindrem la connexio amb el servidor, si alguna cosa falla es tancara el client
    
    global readFromServerThread, subscription
    
    subscription = False

    readFromServerThread = threading.Thread(target=read_from_server)
    readFromServerThread.daemon = True
    readFromInputThread = threading.Thread(target=read_from_input)
    readFromInputThread.daemon = True
    readFromInputThread.start()
    readFromServerThread.start()

    register_to_server()

    
def register_to_server():
    global numPaquets, numProcessos
    #Actualitzem l'estat del client i creem un paquet amb la funció "create_udp_package" i l'enviem
    client.set_status("NOT_SUBSCRIBED")
    print(calc_time() + ": MSG => Client en estat " + client.status + ", Processos de subscripcio:" + str(numProcessos))
    data = (client.name + "," + client.situation).ljust(80)
    paquet = create_udp_package(subs_packages["SUBS_REQ"], data)
    numBytes = sockUdp.sendto(paquet, SERVER_ADDR)
    if len(sys.argv) >= 2 and sys.argv[1] == "-d":
        print(calc_time() + ": DEBUG => Paquet enviat: bytes = " + str(numBytes) + ", tipus paquet = SUBS_REQ, MAC = " + client.mac + ", num = " + str(client.random_number) + ", dades = " + client.name + "," + client.situation)
    numPaquets = 1
    subs_req_accepted(data, paquet)

def subs_req_accepted(data, paquet):
    global numPaquets
    #En aquest estat fem el bucle per saber quants SUBS_REQ enviem al servidor
    client.set_status("WAIT_ACK_SUBS")
    print(calc_time() + ": MSG => Client en estat " + client.status)
    sockUdp.settimeout(2)
    timeAcc = 1
    acumulador = 0
    server_adrr = None
    data = None
    while True:
        try:
            data, server_adrr = sockUdp.recvfrom(103)
            
        except socket.timeout:
            pass

        time.sleep(timeAcc + acumulador)

        if numProcessos > O:
            sockUdp.close()
            print(calc_time() + ": MSG => No s'ha pogut establir connexió amb el servidor")
            exit()

        if numPaquets <= N and (timeAcc + acumulador) < 4 and numPaquets > O:
            #Es va modificant el temps de enviament entre paquets com s'especifica a l'enunciat
            acumulador += timeAcc

        if numPaquets > N:
            #En el cas de enviar mes de 7 paquets, es comença un nou proces de subscripcio
            restart_register_to_server()

        if server_adrr == SERVER_ADDR:
            #Comprovem que l'adreça del servidor sigui la correcta i seguim concatenant funcions que retornen un bool
            checkData(data)

        if data is None:
            numBytes = sockUdp.sendto(paquet, SERVER_ADDR)
            #Opcio del debug per mostrar els paquets rebuts i enviats
            if len(sys.argv) >= 2 and sys.argv[1] == "-d":
                print(calc_time() + ": DEBUG => Paquet enviat: bytes = " + str(numBytes) + ", tipus paquet = SUBS_REQ, MAC = " + client.mac + ", num = " + str(client.random_number) + ", dades = " + client.name + "," + client.situation)
    
            numPaquets += 1

def checkData(data):
    global macReceived
    #Es decodifica el paquet i es comprova que sigui correcte per poder seguir avançant fins la comunicacio periodica
    #Els elif i else ens defineixen les casuistiques en cas de que el paquet sigui erroni
    packReceived, macReceived, randReceived, infoReceived = decode_udp_package(data)
    if len(sys.argv) >= 2 and sys.argv[1] == "-d":
        print(calc_time() + ": DEBUG => Paquet rebut WAIT_SUBS_ACK: bytes = " + str(len(data)) + ", tipus paquet = " + str(packReceived) + ", MAC = " + macReceived + ", num = " + str(randReceived) + ", dades = " + infoReceived.strip("\x00"))
    client.set_random_number(randReceived)
    string_pack = str(packReceived)
    if string_pack == subs_packages["SUBS_ACK"]:
        wait_ack_info(infoReceived, macReceived)
    elif string_pack == subs_packages["SUBS_NACK"]:
        routine()
    elif string_pack ==subs_packages["SUBS_REJ"]:
        restart_register_to_server()
    else:
        restart_register_to_server()
    
def wait_ack_info(infoReceived, macReceived):
    global tcpPort, elements, sockUdp, serverAddr
    client.set_status("WAIT_ACK_INFO")
    print(calc_time() + ": MSG => Client en estat WAIT_ACK_INFO")
    udpAddres = infoReceived.strip('\x00')
    dades = str(readFromServerTcp) + "," + elements
    paquet = create_udp_package(subs_packages["SUBS_INFO"], dades)
    serverAddr = (server_traduit, int(udpAddres))
    numBytes = sockUdp.sendto(paquet, serverAddr)
    try:
        data, server_adrr = sockUdp.recvfrom(103)
        packReceived, newMacReceived, newRandReceived, tcpPort = decode_udp_package(data)
        if len(sys.argv) >= 2 and sys.argv[1] == "-d":
            print(calc_time() + ": DEBUG => Paquet rebut WAIT_ACK_INFO: bytes = " + str(len(data)) + ", tipus paquet = " + str(packReceived) + ", MAC = " + newMacReceived + ", num = " + str(client.random_number) + ", dades = " + infoReceived.strip("\x00"))
    
        #Comprovem que el paquet sigui correcte. Si ho es, totes les funcions que hem passat retornaran True i llavors entrarem al if definit a la
        #funcio routine() i es fara el fork, la comunicacio periodica, etc.
        #En cas de que no sigui correcte o es torna al mateix proces de subscripcio o es crea un noum depen del cas que es doni
        if str(packReceived) == subs_packages["INFO_ACK"] and newMacReceived == macReceived and newRandReceived == client.random_number:
            send_hello_packages()
        elif str(packReceived) == subs_packages["SUBS_NACK"]:
            routine()
        elif str(packReceived) == subs_packages["SUBS_REJ"]:
            restart_register_to_server()
        else:
            restart_register_to_server()
    except socket.timeout:
        restart_register_to_server()

def restart_register_to_server():
    #El que fa aquesta funcio es tornar a crear un nou proces de subscripcio, per tant torna el client a disconnected i afegeix un proces al comptador
    time.sleep(U)
    global numProcessos
    client.set_status("DISCONNECTED")
    client.reset_random_number()
    numProcessos += 1
    routine()

def decode_udp_package(data: str):
    #Aquesta funcio decodifica els paquets udp amb la clase struct, de manera que definint l'estructura del paquet i els camps que te, podem dividir els bytes rebuts
    #despres es decodifiquen amb la funcio decode() i es retornen 
    package_field, mac_address_field, random_number_field, info_field = struct.unpack('c13s9s80s', data)
    pack = package_field.hex()
    mac = mac_address_field.decode('utf-8')
    random_number = random_number_field.decode('utf-8')
    coded_info = info_field[0:6]
    info = coded_info.decode('utf-8')
    return pack, mac, random_number, info

def create_udp_package(tipusPaquet: str, info: str):
    #Aquesta funcio ens ajuda a construir els paquets. Definim els tamany dels camps i passem la informacio que volem codificar
    hex_type = int(tipusPaquet, 16)
    packed_type = struct.pack('B', hex_type)
    paquet = struct.pack('c13s9s80s', packed_type, client.mac.encode(), client.random_number.encode(), info.encode())
    return paquet

def send_hello_packages():
    global subscription
    #Creem els paquets hello per ternir-los ja definits i primer mirarem que el servidor ens contesti al primer paquet.
    #En cas de que ho faci entrarem en l'estat send_hello sino farem la gestio pertinent
    print(calc_time() + ": MSG => Client en estat SUBSCRIBED")
    print(calc_time() + ": MSG => S'ha creat proces per enviar paquets de HELLO")
    client.set_status("SUBSCRIBED")
    subscription = True
    paquetHello = create_udp_package(hello_packages["HELLO"], client.name + "," + client.situation)
    if send_first_hello(paquetHello):
        send_hello(paquetHello)
    else:
        restart_register_to_server()
        
def send_first_hello(paquet):
    #S'envia el primer pafquet de hello i s'espera la resposta, si tot va be es retorna true. Si no va be, s'envia un senyal al proces fill per 
    #a que acabi de manera controlada ja que s'haura de tornar a realitzar el proces de subscripcio
    sockUdp.settimeout(2)
    serverAddr = (server_ip, udp)
    numBytes = sockUdp.sendto(paquet, serverAddr)
    if len(sys.argv) >= 2 and sys.argv[1] == "-d":
        print(calc_time() + ": DEBUG => Paquet enviat: bytes = " + str(numBytes) + ", tipus paquet = HELLO, MAC = " + macReceived + ", num = " + str(client.random_number) + ", dades = " + client.name + "," + client.situation)
    while(True):    
        try:
            dataRcvd, serverAddr = sockUdp.recvfrom(103)
        except socket.timeout:
           break
    packReceived, newMacReceived, newRandReceived, newInfoReceived = decode_udp_package(dataRcvd)
    if len(sys.argv) >= 2 and sys.argv[1] == "-d":
        print(calc_time() + ": DEBUG => Paquet rebut SUBSCRIBED: bytes = " + str(len(dataRcvd)) + ", tipus paquet = " + str(packReceived) + ", MAC = " + newMacReceived + ", num = " + str(newRandReceived) + ", dades = " + newInfoReceived.strip("\x00"))
    if str(packReceived) == hello_packages["HELLO"] and newMacReceived == macReceived and newRandReceived == client.random_number:
        return True
    elif str(packReceived) == hello_packages["HELLO_REJ"]:
        restart_register_to_server()
    else:
        paquetHelloRej = create_udp_package(hello_packages["HELLO_REJ"], client.random_number, client.name + "," + client.situation)
        sockUdp.sendto(paquetHelloRej, serverAddr)
        restart_register_to_server()

def send_hello(paquet):
    global serverAddr
    client.set_status("SEND_HELLO")
    print(calc_time() + ": MSG => Client en estat SEND_HELLO")
    paquetsNoRebuts = 0
    sockUdp.settimeout(1)
    #Tenim un bucle infinit que es el que s'encarrega de la comunicació periodica, on envia un paquet i espera a rebre un del servidor
    #tambe es te un comptador dels paquets que no es reben per iniciar un nou proces de subscripcio en cas de que faci falta
    #En cas de que es tingui que fer un nou proces de subscripcio, igual que abans, s'envia un senyal al proces fill per a que acabi de manera controlada
    while True:
        time.sleep(1.5)
        #read_from_server() funcio que serviria per rebre dades del servidor pero la implementacio no funciona :)
        numBytes = sockUdp.sendto(paquet, (server_ip,udp))
        if len(sys.argv) >= 2 and sys.argv[1] == "-d":
            print(calc_time() + ": DEBUG => Paquet enviat: bytes = " + str(numBytes) + ", tipus paquet = HELLO, MAC = " + client.mac + ", num = " + str(client.random_number) + ", dades = " + client.name + "," + client.situation)
        if(paquetsNoRebuts >= 3):
            restart_register_to_server()
        try:
            dataRcvd, serverAddr = sockUdp.recvfrom(103)
            packReceived, newMacReceived, newRandReceived, newInfoReceived = decode_udp_package(dataRcvd)
            if len(sys.argv) >= 2 and sys.argv[1] == "-d" and len(dataRcvd) > 0:
                print(calc_time() + ": DEBUG => Paquet rebut SEND_HELLO: bytes = " + str(len(dataRcvd)) + ", tipus paquet = " + str(packReceived) + ", MAC = " + newMacReceived + ", num = " + str(newRandReceived) + ", dades = " + newInfoReceived.strip("\x00"))
            if str(packReceived) == hello_packages["HELLO"] and newMacReceived == macReceived and newRandReceived == client.random_number:
                pass
            elif str(packReceived) == hello_packages["HELLO_REJ"]:
                restart_register_to_server()
        except socket.timeout:
            paquetsNoRebuts += 1

def read_from_input():
    #Es la funcio que fa el proces fill on esta llegint de termial en un bucle infinit
    #Es llegeix la linea de terminal, si l'acció es troba entre les possbiles es tracta la linea sencera i es fa l'accio
    #en cas de que no hi sigui es mostra per missatge
    while True:
        if subscription:
            try:
                line = input()
                action = line.split(" ")[0]
                if(action in accions):
                    if action == "quit":
                        quit_client()
                    elif action == "send":
                        if len(line.split(" ")) > 1:
                            cntrlrName = line.split(" ")[1]
                            if cntrlrName not in valores:
                                print("Nom no vàlid: " + cntrlrName)
                            else:
                                send(cntrlrName)
                        else:
                            print(calc_time() + ": MSG => Us: send <valor>")
                    elif action == "stat":
                        print_stat()
                    elif action == "set":
                        if len(line.split(" ")) > 2:
                            cntlrName = line.split(" ")[1]
                            value = line.split(" ")[2]
                            if cntlrName not in valores:
                                print("Nom no vàlid: " + cntlrName)
                            else:
                                set_value(cntlrName, value)
                        else:
                            print(calc_time() + ": MSG => Us: set <nom> <valor>")
                else:
                    print(calc_time() + ": MSG => Accion no valida:" + action)
            except EOFError:
                pass
    
def quit_client():
    #Envia un sigterm al proces pare per a que acabi i tambe acaba
    print(calc_time() + ": MSG => Quit rebut, acabant processos...")
    pid = os.getpid()
    sockUdp.close()
    readTcp.close()
    #os.kill(pid, signal.SIGUSR1)
    os.kill(pid, signal.SIGTERM)
    close_threads()
    exit()

def send(cntrlName):
    #Es crea el socket per enviar les dades al servidor i el paquet
    #un cop enviat s'espera la resposta del servidor
    #quan s'ha rebut es tracta i s'actua en consequencia
    intTcp = int(tcpPort.strip("\x00"))
    print("PORT: " + tcpPort)
    sendTcp = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    sendTcp.connect((server_ip, intTcp)) #socket para enviar, puerto recicibido en el paquete en el estado wait_ack_info
    paquet = create_tcp_package(data_packages["SEND_DATA"], client.random_number, valores[cntrlName], cntrlName, "")
    sendTcp.settimeout(3)
    
    try:
        sendTcp.send(paquet)
    except socket.error:
        time.sleep(1)
    if len(sys.argv) >= 2 and sys.argv[1] == "-d":
        print(calc_time() + ": DEBUG => Paquet enviat en estat SEND_HELLO: bytes = " + str(len(paquet)) + ", tipus paquet = " + data_packages["SEND_DATA"] + ", MAC = " + client.mac + ", num = " + str(client.random_number) + ", dispositiu = " + cntrlName + ", valor = "+ valores[cntrlName] + ", info = \"\"")
    
    try:
        data = sendTcp.recv(118)
    except socket.timeout:
        print(calc_time() + ": MSG => No s'ha rebut resposta del sevidor, tancant port TCP...")
        sendTcp.close()
    
    pack, newMacReceived, newRandRcvd, nameRcvd, valRcvd, info = decode_tcp_package(data)
    if len(sys.argv) >= 2 and sys.argv[1] == "-d":
            print(calc_time() + ": DEBUG => Paquet rebut en estat SEND_HELLO: bytes = " + str(len(data)) + ", tipus paquet = " + data_packages["DATA_ACK"] + ", MAC = " + newMacReceived + ", num = " + str(client.random_number) + ", dispositiu = " + nameRcvd +  ", valor =" + valRcvd + ", info = " + info)
    
    if pack == data_packages["DATA_ACK"] and newMacReceived == macReceived and newRandRcvd == client.random_number:
        print(calc_time() + ": MSG => Dades actualitzades al servidor correctament")
    
    elif(pack == data_packages["DATA_NACK"]):
        print(calc_time() + ": MSG => Rebut paquet DATA_NACK, reenviament de les dades en desenvolupament...")
        sendTcp.close()
    
    elif(pack == data_packages["DATA_REJ"]):
        parentPid = os.getppid()
        os.kill(parentPid, signal.SIGUSR2)
        exit()    
    
    else:
        parentPid = os.getppid()
        os.kill(parentPid, signal.SIGUSR2)
        exit()  

def print_stat():
    #Es mostren per pantalla els sensors i els seus valors
    print("-------------------------------------------------------")
    print("MAC: " + str(client.mac) + "   Name:" + str(client.name) + "   Name:" + str(client.situation))
    print()
    print("     ESTAT: " + client.status)
    print()
    for key in valores:
        print("      " + key + ": " + str(valores[key]) + "     ")
    print("------------------------------------------------------")

def set_value(cntrlrName, value):
    #Es modifica el valor del sensor que es destija
    if cntrlrName in valores:
        valores[cntrlrName] = value
    else:
        print(calc_time() + ": MSG => Not a valid name")

def read_from_server():
    global readTcp, server_socket
    readTcp = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    try:
        readTcp.bind((server_ip, readFromServerTcp)) 
    except OSError:
        pass
    readTcp.listen(1)
    while True:
        if subscription:      
            
            try:
                server_socket, server_tcp_address = readTcp.accept()
                    
                server_socket.settimeout(3)

                data = server_socket.recv(118)
                    
                if data:
                    pack, newMacReceived, newRandRcvd, nameRcvd, valRcvd, info = decode_tcp_package(data)
                    if len(sys.argv) >= 2 and sys.argv[1] == "-d":
                        print(calc_time() + ": DEBUG => Paquet rebut en estat SEND_HELLO: bytes = " + str(len(data)) + ", tipus paquet = " + str(pack) + ", MAC = " + newMacReceived + ", num = " + str(client.random_number) + ", dispositiu = " + nameRcvd +  ", valor = " + valRcvd + ", info = ")
                    trimmed_data = nameRcvd.rstrip('\x00')
                    if(pack == data_packages["SET_DATA"]) and newMacReceived == macReceived and newRandRcvd == client.random_number:
                        print(calc_time() + ": MSG => Rebut paquet SET_DATA, actualitzant dades del controlador " + nameRcvd)
                        set_value(trimmed_data, valRcvd)
                        paquet = create_tcp_package(data_packages["DATA_ACK"], client.random_number, valRcvd, nameRcvd, info)
                        server_socket.send(paquet)
                        server_socket.close()
                    elif(pack == data_packages["GET_DATA"] and newMacReceived == macReceived and newRandRcvd == client.random_number):
                        if(valores[trimmed_data]) is None:
                            valueSent = " "
                        else:
                            valueSent = valores[trimmed_data]
                        paquet = create_tcp_package(data_packages["DATA_ACK"], client.random_number, valueSent, nameRcvd, info)
                        server_socket.send(paquet)
                        if len(sys.argv) >= 2 and sys.argv[1] == "-d":
                            print(calc_time() + ": DEBUG => Paquet enviat en estat SEND_HELLO: bytes = " + str(len(paquet)) + ", tipus paquet = " + data_packages["DATA_ACK"] + ", MAC = " + newMacReceived + ", num = " + str(client.random_number) + ", dispositiu = " + nameRcvd +  ", valor = " + valueSent + ", info = " + info)
                        server_socket.close()
                    elif(data_packages["GET_DATA"] or data_packages["SET_DATA"]) and newMacReceived == client.mac and newRandRcvd == client.random_number and nameRcvd not in valores:
                        paquet = create_tcp_package(data_packages["DATA_REJ"], client.random_number, valRcvd, nameRcvd, "dades incorrectes")
                        server_socket.send(paquet)
                        if len(sys.argv) >= 2 and sys.argv[1] == "-d":
                            print(calc_time() + ": DEBUG => Paquet enviat en estat SEND_HELLO: bytes = " + str(len(paquet)) + ", tipus paquet = " + data_packages["DATA_NACK"] + ", MAC = " + newMacReceived + ", num = " + str(client.random_number) + ", dispositiu = " + nameRcvd +  ", valor =" + valRcvd + ", info = dades incorrectes")
                        server_socket.close()
                    else:
                        
                        server_socket.close()
            except socket.timeout:
                server_socket.close()


def create_tcp_package(tipusPaquet, randNum, valor, cntrlName, info):
    #Fa el mateix que la funcio que crea els paquets UDP pero amb un format de paquet diferent
    hex_type = int(tipusPaquet, 16)
    packed_type = struct.pack('B', hex_type)
    paquet = struct.pack('c13s9s8s7s80s', packed_type, client.mac.encode(), randNum.encode(), cntrlName.encode(), valor.encode(), info.encode())
    return paquet

def decode_tcp_package(data: str):
    #Fa el mateix que la funcio que decodifica els paquets UDP pero amb un format de paquet diferent
    package_field, mac_address_field, random_number_field, name_field, value_field, info_field = struct.unpack('c13s9s8s7s80s', data)
    pack = package_field.hex()
    newMac = mac_address_field.decode('utf-8')
    randRcvd = random_number_field.decode('utf-8')
    nameRcvd = name_field.decode('utf-8')
    #valRcvd = value_field.decode('latin-1')
    valRcvd = value_field.decode('utf-8', errors='ignore')
    """if(pack == data_packages["SET_DATA"] or pack == data_packages["GET_DATA"]):
        info = ""
    else:
        cleaned_data = info_field[:20]
        info = cleaned_data.decode('utf-8')"""
    info = info_field.decode('utf-8', errors='ignore')
    return pack, newMac, randRcvd, nameRcvd, valRcvd, info

def calc_time():
    #Retorna la marca de temps per als missatges
    return time.strftime("%H:%M:%S")

#Handlers dels diferents tipus de senyals que utilitza el programa
def sigusr_handler(signalRcvd, frame):
    sockUdp.close()
    readTcp.close()

def sigterm_handler(signalRcvd, frame):
    exit()

#def sigint_handler(signalRcvd, frame):
    print(calc_time() + ": MSG.  => Finalització per ^C")
    sockUdp.close()
    for thread in threading.enumerate():
            if thread.is_alive():
                thread.join()
    exit()

def sigusr2_handler(signalRcvd, frame):
    restart_register_to_server()

def close_threads():
    for thread in threading.enumerate():
        if thread.is_alive() and thread != threading.current_thread():
            thread.join()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C rebut, finalitzant...")
        sockUdp.close()
        readTcp.close()
        exit()

#FALTA SOLUCIONAR ADDRES ASLREADY IN USE AL HACER EL QUIT I SOLUCIONAR QUE NO SE CUELEN LETRAS EN EL SET, LO OTRO TODO OK
#ARREGLAR QUIT DESPUES DE RECIBIR INFORMACION