#pragma once
#include"DataManager.h"

class Cache
{
public:
	int pageSize;
	static int fecthPage(pghdr** pPage, int pgno, Cache* cache);
	int buildPage1(pghdr* pPage);
	int buildInternalPage(pghdr* pPage);
	int buildLeafPage(pghdr* pPage);
	static int fetchPageFromFreePageList(Pager* pPger);

	static int AllocateNewPage(pghdr ** pg, Pager * pger, bool isLeaf);
};