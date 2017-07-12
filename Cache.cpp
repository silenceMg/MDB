#include"Cache.h"

int Cache::fecthPage(pghdr ** pPage, int pgno, Cache * cache)
{
	return 0;
}
int Cache::buildPage1(pghdr * pPage)
{
	string str = "MDB format 0";
	int i = 0;
	for (auto &it : str)
	{
		pPage->image[i] = it;
		i++;
	}
	//初始化free page list, number of free page, file change counter
	memset(&(pPage->image[i]), 0, sizeof(unsigned char) * 12);
	i = i + 11;
	pPage->image[i] = 0x01;
	i++;
	//初始化为叶子节点
	pPage->image[i] = 0x0d;
	i++;
	//本页剩余空间
	pPage->image[i] = (0xfe1 >> 8) | 0xff;
	pPage->image[i + 1] = 0xfe1 | 0xff;
	//本页cell数量
	memset(&(pPage->image[i + 2]), 0, sizeof(unsigned char) * 2);
	pPage->image[i + 3] = 0xf;
	pPage->image[i + 4] = 0xff;
	return MDB_OK;
}

int Cache::buildInternalPage(pghdr* pPage)
{
	//内部节点标识
	pPage->image[0] = 0x05;
	//剩余空间
	pPage->image[1] = 0x0f;
	pPage->image[2] = 0xf5;
	//第一个cell偏移量
	pPage->image[5] = pPage->pgno | 0xff00;
	pPage->image[6] = pPage->pgno | 0xff;
	return MDB_OK;
}

int Cache::buildLeafPage(pghdr * pPage)
{
	//叶子节点标识
	pPage->image[0] = 0x05;
	//剩余空间
	pPage->image[1] = 0x0f;
	pPage->image[2] = 0xf9;
	//第一个cell偏移量
	pPage->image[5] = pPage->pgno | 0xff00;
	pPage->image[6] = pPage->pgno | 0xff;
	return MDB_OK;
}

int Cache::fetchPageFromFreePageList(Pager* pPger)
{
	int res = pPger->freePageHead;
	pghdr* pg;
	if (-1 == DataManager::readPageFromCache(pg))
	{
		DataManager::readPageFromFile(pg);
	}
	//更新freePageHead, iFreePageNum
	pPger->iFreePgNum--;
	int tmp = pPger->iFreePgNum;
	for (int i = 0; i < 4; i++)
	{
		pPger->page1->image[16 + i] = pg->image[i];
		pPger->freePageHead |= pg->image[i];
		pPger->freePageHead = pPger->freePageHead << 8;
		pPger->page1->image[19 - i] = tmp & 0xff;
		tmp = tmp >> 8;
	}
	return res;
}

//申请到reserved_lock检查一下数据库page1的rec
int Cache::AllocateNewPage(pghdr ** pg, Pager * pger,bool isLeaf)
{
	int64_t fileSize;
	if (pger->iFreePgNum == 0)
	{
		mdbFile::winFileSize(pger->fd, &fileSize);//申请新的空间地址
		pg->pgno = fileSize / 4096 + 1;
	}
	else
	{
		pg->pgno = Pager::fetchPageFromFreePageList(pger);
	}
	return 0;
}