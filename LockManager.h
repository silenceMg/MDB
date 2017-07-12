#pragma once
#include"mdbFile.h"
#include"mdbdef.h"
#include"Pager.h"

class LockManager
{
private:

public:
	static int pagerWaitOnLock(Pager *pPager, int locktype);
	static int pagerUnlock(Pager *pPager);
	static int getReadLock(mdbFile* pFile);
	static int unlockReadLock(mdbFile* pFile);
	static int winLock(mdbFile* pFile, int locktype);
	static int winUnlock(mdbFile* pFile, int locktype);
};