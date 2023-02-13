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

## components

---



BitWindow

BitWindow is a data structure with 0/1 values and fixed size. Fortunate Server uses BitWindow to ensure random state values generated is distributed as expected. Fortunate server initialize a bunch of BitWindow. Each BitWindow has various configuration and interval to shuffle elements. BitWindow uses Fisher Yates algorithm to shuffle.



2. Node Component and Signal & Signal Block

Node is a key and basic component in Fortunate. Node emits signals on their own interval and algorithms. Node has many types such as BitArray, Range, Sequential.

Node has a cursors pointing to BitWindow and consume 0/1 values with this cursors. This value provide seed to generate Node Signal.

For first,  let`s take a look at BitArray Node. this node emits bitarray signal which represent 0/1 value with 1/2^n possiblity. BitArray Node has a seed state and a function that projects a seed state to bit array.
Signal has uuid combined with epoch, timestamp, nonce and a signal can be identified with this key.

Fortunate tracks every node`s signal and store in Node signal block. Node signal block has epoch and it is unique at each block. Fortunate finalizes a signal block every 5 min and epoch key represent the period for this 5 minutes. a finalized signal block hashes their serialized buffer and a next block reference this.

If a service user provides actions based on possiblity to their end user such as item reinforcement or picking card in foker game, the state of events depends on BitArray node signals values. if a action that end user required arises with 1/2^16 possiblity, Fortunate searches node signals in finalized node signal block (it has previous epoch not current epoch because current epoch block would be not finalized) on way fortunate action planner indicate. In some case fortunate might search 4 signals 1/16 flag value and multiply it. and method to calculate possiblity depends on action planner and not determinstic.



2. Event and Event Block

Event is generated from Node signal and Event has this information. 


3. Event Payload 

Fortunate has ability to identify Service Provider  and Service Consumer in given context or event as using Event Payload. 

4. Action Planner 

Action Planner is a component planning a strategy for which signals be referenced in a given event. If action is supposed to arise with 1/(2^4*10^3) possibility in this event, Planner collects signals. In some case it references first bit flags of 4 signals and 13th bit flags of a signal. And then check whether all flag have true value. In other case, it references just one signal and its 2^4 flag (4th flag in signal) and one and its 10^3 flag (13th flag in signal). Plan is not deterministic. Action planner have seed state and plan depend on this value and its algorithm to plan.


5. Meta Block

This block stores meta data such as Node seed value and Node algorithms, a seed value and algorithms of Action planners,



## Key Formats

---


S01 Node Signal Key —

55422A :: RDvpL6 :: 1676249501563073 :: 1000000000 :: 000
epoch(6) + ts(16) + nodeuuid(6) + 2 flag(10) + 10 flag(3)


NodeSignalKeyRefSer

epoch(6) :: ts(16) :: ref_index(3) :: flags(rest)




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

