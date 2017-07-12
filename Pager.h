#pragma once
#include<vector>
#include<memory>
#include<iostream>
#include<fstream>
#include<string>
#include<set>
#include<windows.h>
#include <assert.h>
#include<stdint.h>
#include <functional> 
#include<random>
#include<time.h>

#include"mdbdef.h"
using namespace std;
class mdbFile;
class Cache;

int BusyHandler(int cnt)
{
	cnt++;
	Sleep(500);
	return cnt;
}
class pghdr {
public:
	int pgno;
	bool injournal = false;//是否已经在回滚日志中
	bool isLeaf = true;
	int freeSpace = 0;
	int iCellNum = 0;
	int ptrOfFirstCell = 0;
	//bool needSync = false;//是否需要写入到回滚日志中，在对某一页进行写操作之前，一定要将该页写入回滚日志中(flush)
	bool dirty = false;//是否是脏页
	//int nRef = 0;

	unsigned char image[4096];

	pghdr* pNextHash = nullptr;
	pghdr* pPreHash = nullptr;
	short lockType = SHARED_LOCK;
	pghdr();
	//~pghdr();
};


class Pager {
public:
	string tableName;
	unsigned char state;
	bool journalIsOpen = false;
	HANDLE jfd;//journal file description
	mdbFile *fd;
	int(*xBusyHandler)(int cnt);
	int root;
	int iFreePgNum;
	int freePageHead;
	pghdr* page1;
	Cache* cache;

	static int formatPghdr(pghdr *pg);
	static int findMidPosOfPage(pghdr* pg);

	static int pagerGet(pghdr** ppPg,Pager* pPger);
	static int pagerWrite(pghdr** ppPg, Pager * pPger);
	static int unlockPager();
	Pager(string path);
	~Pager();
};
