#include"mdbFile.h"
#include"LockManager.h"
#include<thread>




//获取一个文件的锁,如果忙则重复该操作
int LockManager::pagerWaitOnLock(Pager *pPager, int locktype) {
	int rc = MDB_BUSY;
	int cnt = 0;
	if (pPager->state >= locktype) {
		rc = MDB_OK;
	}
	else
	{
		do {
			rc = winLock(pPager->fd, locktype);
		} while (rc == MDB_BUSY && (cnt=pPager->xBusyHandler(cnt))&&cnt<3 );

		if (rc == MDB_OK)
		{
			//设置pager的状态
			pPager->state = locktype;
		}
	}
	return rc;
}

int LockManager::pagerUnlock(Pager * pPager)
{
	int res = 0;
	res = LockManager::winUnlock(pPager->fd,NO_LOCK);
	return res;
}

int LockManager::getReadLock(mdbFile* pFile) {
	int res;
	OVERLAPPED ovlp;
	memset(&ovlp, 0, sizeof(OVERLAPPED));
	ovlp.Offset = SHARED_FIRST;
	ovlp.OffsetHigh = 0;
	ovlp.hEvent = 0;
	res = LockFileEx(pFile->h, LOCKFILE_FAIL_IMMEDIATELY, 0, SHARED_SIZE, 0, &ovlp);
	return res;
}

int LockManager::unlockReadLock(mdbFile* pFile) {
	int res;
	OVERLAPPED ovlp;
	memset(&ovlp, 0, sizeof(OVERLAPPED));
	ovlp.Offset = SHARED_FIRST;
	ovlp.OffsetHigh = 0;
	ovlp.hEvent = 0;
	res = UnlockFileEx(pFile->h, 0, SHARED_SIZE, 0, &ovlp);

	return res;
}

int LockManager::winLock(mdbFile* id, int locktype) {
	int rc = MDB_OK;    //本函数的返回码
	int res = 1;        //windows lock相关函数的返回值
	int newLocktype;
	int gotPendingLock = 0;//pending锁
	mdbFile* pFile = id;
	assert(id!=NULL);
	//当前的锁的等级>=要申请的锁的等级,则返回
	if (pFile->locktype >= locktype) {
		return MDB_OK;
	}

	//保证锁的顺序的正确性
	assert(pFile->locktype != NO_LOCK || locktype == SHARED_LOCK);//断言不可能出现这种情况：当前锁是no_lock,申请的锁却不是shared_lock
	assert(locktype != PENDING_LOCK);//断言要申请的锁不是pending_lock
	assert(pFile->locktype == SHARED_LOCK || locktype != RESERVED_LOCK);//断言不可能出现这种情况：当前锁不是shared_lock,要申请的锁却是reserveed_lock

	newLocktype = pFile->locktype;

	/*两种情况: 
	(1)如果当前文件处于无锁状态(获取读锁--读事务和写事务在最初阶段都要经历的阶段)
	(2)处于RESERVED_LOCK，且请求的锁为EXCLUSIVE_LOCK(写事务),则对执行加PENDING_LOCK
	*/

	//获取pending_lock
	if (pFile->locktype == NO_LOCK
		|| (locktype == EXCLUSIVE_LOCK && pFile->locktype == RESERVED_LOCK)
		) {
		int cnt = 3;
		//加pending锁
		while (cnt-->0 && (res = LockFile(pFile->h, PENDING_BYTE, 0, 1, 0)) == 0) {
			/* Try 3 times to get the pending lock.  The pending lock might be
			** held by another reader process who will release it momentarily.
			*/
			cerr << "Could not get a PENDING lock." << endl;
			Sleep(1);
		}
		//标记gotPendingLock为1,在后面要释放PENDING锁
		gotPendingLock = res;
	}


	/*获取shared_lock
	此时,事务应该持有PENDING锁,而PENDING锁作为事务从UNLOCKED到
	SHARED_LOCKED的一个过渡,所以事务由PENDING->SHARED
	此时,实际上锁处于两个状态:PENDING和SHARED,
	直到后面释放PENDING锁后,才真正处于SHARED状态
	*/

	if (locktype == SHARED_LOCK && res) {
		assert(pFile->locktype == NO_LOCK);
		res = getReadLock(pFile);
		if (res) {
			newLocktype = SHARED_LOCK;
		}
	}


	//获取reserveed_lock,此时事务持有SHARED_LOCK,变化过程为SHARED->RESERVED。
	if (locktype == RESERVED_LOCK && res) {
		assert(pFile->locktype == SHARED_LOCK);
		//加RESERVED锁
		res = LockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
		if (res) {
			newLocktype = RESERVED_LOCK;
		}
	}
	//获取pending_lock
	if (locktype == EXCLUSIVE_LOCK && res) {
		//这里没有实际的加锁操作，只是把锁的状态改为PENDING状态
		newLocktype = PENDING_LOCK;
		//设置了gotPendingLock,后面就不会释放PENDING锁了,
		//相当于加了PENDING锁,实际上是在开始处加的PENDING锁
		gotPendingLock = 0;
	}

	//获取exclusive_lock
	//当一个事务执行该代码时,它应该满足以下条件:
	//(1)锁的状态为:PENDING (2)是一个写事务
	if (locktype == EXCLUSIVE_LOCK && res) {
		assert(pFile->locktype >= SHARED_LOCK);
		res = unlockReadLock(pFile);
		res = LockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);//lockFile是阻塞的
		if (res) {
			newLocktype = EXCLUSIVE_LOCK;
		}
		else {
			cerr << "Could not get a EXCLUSIVE lock. Error code is " << GetLastError() << endl;
		}
	}

    //如果还持有pending_lock，释放
	if (gotPendingLock && locktype == SHARED_LOCK) {
		UnlockFile(pFile->h, PENDING_BYTE, 0, 1, 0);
		gotPendingLock = 0;
	}

	if (res) {
		rc = MDB_OK;
	}
	else {
		cerr << "LOCK FAILED. " << pFile->h << " Trying for " << locktype << " but got " << newLocktype << endl;
		rc = MDB_BUSY;
	}
	//在这里设置文件锁的状态
	pFile->locktype = newLocktype;
	return rc;
}

int LockManager::winUnlock(mdbFile* id, int locktype) {
	int type;
	mdbFile* pFile = id;
	int rc = MDB_OK;
	assert(pFile != 0);
	assert(locktype <= SHARED_LOCK);
	//OSTRACE(("UNLOCK file=%p, oldLock=%d(%d), newLock=%d\n",
	//	pFile->h, pFile->locktype, pFile->sharedLockByte, locktype));
	type = pFile->locktype;
	if (type >= EXCLUSIVE_LOCK) {
		UnlockFile(&pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
		if (locktype == SHARED_LOCK && !getReadLock(pFile)) {
			/* This should never happen.  We should always be able to
			** reacquire the read lock */
		}
	}
	if (type >= RESERVED_LOCK) {
		unlockReadLock(pFile);
	}
	if (locktype == NO_LOCK && type >= SHARED_LOCK) {
		unlockReadLock(pFile);
	}
	if (type >= PENDING_LOCK) {
		unlockReadLock(pFile);
	}
	pFile->locktype = (unsigned char)locktype;
	return rc;
}

//int main()
//{
//	OVERLAPPED ovlp;
//	memset(&ovlp, 0, sizeof(OVERLAPPED));
//	ovlp.Offset = 0;
//	ovlp.OffsetHigh = 0;
//ovlp.hEvent = 0;
//	int res1 = 1;
//	int res2 = 1;
//	int res3 = 1;
//	HANDLE hFile;
//	
//	DWORD  dwBytesRead, dwBytesWritten, dwPos;
//	BYTE   buff[4096];
//
//	// Open the existing file.
//
//	hFile = CreateFile(TEXT("one.txt"), // open One.txt
//		GENERIC_READ,             // open for reading
//		FILE_SHARE_READ | FILE_SHARE_WRITE,                        // do not share
//		NULL,                     // no security
//		OPEN_EXISTING,            // existing file only
//		FILE_ATTRIBUTE_NORMAL,    // normal file
//		NULL);                    // no attr. template
//
//	if (hFile == INVALID_HANDLE_VALUE)
//	{
//		cout << "Could not open One.txt.";
//		return 0;
//	}
//
//	res1 = LockFileEx(hFile, LOCKFILE_FAIL_IMMEDIATELY, 0, 10, 0, &ovlp);
//	res2 = LockFileEx(hFile, LOCKFILE_FAIL_IMMEDIATELY, 0, 10, 0, &ovlp);
//	res3 = UnlockFileEx(hFile, 0, 10, 0, &ovlp);
//	// Close both files.
//
//	CloseHandle(hFile);
//	return 0;
//}

void thread1()
{
	Pager p("my");
	if (MDB_OK == LockManager::pagerWaitOnLock(&p,SHARED_LOCK))
		cout << "t1 got shared_lock\n";
	if (MDB_OK == LockManager::pagerWaitOnLock(&p, RESERVED_LOCK))
		cout << "t1 got EX_lock\n";
	if (MDB_OK == LockManager::pagerWaitOnLock(&p, RESERVED_LOCK))
		cout << "t1 get another reserved\n";
	Sleep(5000);
	if (MDB_OK == LockManager::winUnlock(p.fd, NO_LOCK))
		cout << "t1 unlocked shared_lock\n";
}
void thread2()
{
	Pager p("my");
	Sleep(1000);
	if (MDB_OK == LockManager::pagerWaitOnLock(&p, SHARED_LOCK))
		cout << "t2 got shared_lock\n";
	else
		cout << "t2 can not\n";

	if (MDB_OK == LockManager::winUnlock(p.fd, NO_LOCK))
		cout << "t2 unlocked shared_lock\n";
}
int main()
{
	thread t1(thread1);
	//thread t2(thread2);
	t1.join();
	//t2.join();
	cin.get();
	return 0;
}





