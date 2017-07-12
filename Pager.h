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
	bool injournal = false;//�Ƿ��Ѿ��ڻع���־��
	bool isLeaf = true;
	int freeSpace = 0;
	int iCellNum = 0;
	int ptrOfFirstCell = 0;
	//bool needSync = false;//�Ƿ���Ҫд�뵽�ع���־�У��ڶ�ĳһҳ����д����֮ǰ��һ��Ҫ����ҳд��ع���־��(flush)
	bool dirty = false;//�Ƿ�����ҳ
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