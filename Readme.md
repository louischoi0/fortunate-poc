# Fortunate: A verifiable framework generating random state based on probability with multi nodes

---

Randomness is a big part of games or even real life. we buy lottery and enter a draw to win.  However for applicants there is no way to verify faireness in foker game or raffles or rpg games. and process is not transparent to user. And there has been many attempts to manipulate system (like tazza in korean word). So fortunate solve this problems as to provide random state api which is verifiable.



## Notes

---

This repository is a Proof-of-Concept repository. this project will be re-programmed with rust.



## Concepts

---

Fortunate have many components to provide verifiable random state api (Pool, Node, Event, Fortunate Server, BlockStorage .. etc). Each have their own functions. 

Node is one of basic components. And it emit signal with their own interval (not fixed). its signal can be serialized and is tracked by pool component. Pool manage nodes`s state and signals, and it stores historical data to block storage.

if a api user want to implement a function to decide true or false at 1/(2^10) probability, user call fortunate api. 

Then fortunate selects 10 nodes randomly (with specific algorithms in fortunate) and it check whether every nodes have true value in 1/2 flag. (node has1/2 flag, 1/4 flag, 1/8 flag, 10/flag)

it is expressed as an fortunate event. every events have a sign key and a hash that user provided . and later other users can verify with them. (block storage server does this.)

Fortunate Server is rest api server.



## API Features

---

- TF API
- OON API
- Range API
- Verify API



## Getting Started

---

```
git clone https://github.com/louischoi0/fortunate-poc
cd fortunate-poc/src/
python3 standalone.py
```





## Future Improvement

---







## Fortunate Explorer Web Server

---

