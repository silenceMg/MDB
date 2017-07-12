#include"Pager.h"
#include"mdbFile.h"
#include"Cache.h"
#include"LockManager.h"

Pager::Pager(string path)
{
	xBusyHandler = BusyHandler;
	fd = new mdbFile(path);
}


int Pager::formatPghdr(pghdr * pg)
{
	if (pg->image[0] == 0x0d)
		pg->isLeaf = true;
	else
		pg->isLeaf = false;

	pg->freeSpace = pg->image[FREE_SPACE_OFFSET] << 8 + pg->image[FREE_SPACE_OFFSET + 1];
	pg->iCellNum = pg->image[CELL_NUM_OFFSET] << 8 + pg->image[CELL_NUM_OFFSET + 1];
	return MDB_OK;
}

int Pager::findMidPosOfPage(pghdr * pg)
{

	int cellPtrPos = 11;
	int cellPos = 0;
	if (pg->isLeaf)
		cellPtrPos = 3;
	for (int i = 0; i < pg->iCellNum; i++)
	{
		cellPos = pg->image[cellPtrPos] << 8 + pg->image[cellPtrPos + 1];
		if (cellPos < 2040)
			break;
		cellPtrPos += 2;
	}
	return cellPtrPos;
}




////使某页可读，即获取shared_lock
//int Pager::pagerGet(pghdr ** ppPg, Pager * pPger)
//{
//	LockManager::pagerWaitOnLock(pPger, SHARED_LOCK);
//	Cache::fecthPage(ppPg, pPger->cache);
//	return 0;
//}
////使某页可写，即获取reserved_lock
//int Pager::pagerWrite(pghdr ** ppPg, Pager * pPger)
//{
//	LockManager::pagerWaitOnLock(pPger, RESERVED_LOCK);
//	Cache::fecthPage(ppPg, pPger->cache);
//	return 0;
//}



pghdr::pghdr()
{
	memset(image,0,4096);
}
