# MVCC

## *WORK IN PROGRESS*

You probably want to check [CaskDB](https://github.com/avinassh/py-caskdb)

## Errata

[Internet is wholesome: MVCC edition](https://avi.im/blag/2023/internet-mvcc/)

## Design decisions

- Attempting to write a lockless linked list. Check package lockless
- I have avoided generics to keep things very simple. Downside is, the value always has to be a `int` type.
- The code is a mess. I wanted to write this as quickly as possible, so very less thought is given for maintainability. Thats 
    why you see node, row, row version etc interchanged.


A go implementation of Hekaton MVCC based on the paper [High-Performance Concurrency Control
Mechanisms for Main-Memory Databases](https://vldb.org/pvldb/vol5/p298_per-akelarson_vldb2012.pdf).
