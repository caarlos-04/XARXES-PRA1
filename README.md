# 🖧 XARXES-PRA1

This repository contains **Practice 1** for the Computer Networks (Xarxes) course in the Computer Engineering degree at the Universitat de Lleida (UdL).  
The main goal is to develop a client-server application that enables communication between processes using sockets.

---

## 📁 Project Structure
XARXES-PRA1/

├── client.py # Client implemented in Python

├── server.c # Server implemented in C

├── Makefile # Server compilation

---

## 🚀 How to Run

### 1. Compile the server

```bash
make
```
This will generate an executable called server.

### 2. Run the server
```bash
./server
```
### 3. Run the client

```bash
python3 client.py
```
Make sure the server is running before launching the client. It allows at most 5 clients connected at the same time.

---

## 🛠️ Technologies Used

  Languages: C, Python

  Networking: TCP/IP Sockets

  Tools: Makefile

  ---

## 👨‍💻 Author

  Carlos Mazarico

  Computer Engineering student @ Universitat de Lleida (UdL)

  📫 Contact: cmazarico@gmail.com
  
---

## 📌 Notes

This project was developed as part of a university assignment.
Feel free to suggest improvements.
