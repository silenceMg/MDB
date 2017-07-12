#include"pageBTree.h"
#include"mdbFile.h"
#include"LockManager.h"
#include"DataManager.h"
#include"Cache.h"
//FILEP BPlusTree::writeNewPageAtTail(Pager *pPager)  const //在磁盘上分配一块B+树节点空间,add to file's tail
//{
//	pghdr r;
//	LockManager::pagerWaitOnLock(pPager, EXCLUSIVE_LOCK);
//	mdbFile::winWrite(pPager->fd, r.image, 0xfff, r.pgno * 0xfff);
//	LockManager::pagerUnlock(pPager);
//	return 0;
//}

void BPlusTree::buildNewBPTree(Pager *pPager)   //建立一棵空B+树  
{
	//FILEP ROOT = allocNewPageAtTail();
	pghdr r;
	LockManager::pagerWaitOnLock(pPager, EXCLUSIVE_LOCK);
	mdbFile::winWrite(pPager->fd, r.image, 0xfff, r.pgno * 0xfff);
	LockManager::pagerUnlock(pPager);
}

BPlusTree::BPlusTree()
{
	//Bfile = fopen("Bfile", "rb+");     //打开B+树文件  

}

BPlusTree :: ~BPlusTree()
{
	//fclose(Bfile);
}



void BPlusTree::Build_BPlus_Tree()   //建立一棵空B+树  
{
	//ROOT = GetBPlusNode();
	pghdr r;
	//r.Pointer[MAX_KEY] = 0;
	//r.nkey = 0;
	//r.isleaf = true;
	LockManager::pagerWaitOnLock(pPger, EXCLUSIVE_LOCK);
	mdbFile::winWrite(pPger->fd, r.image, 0xfff, r.pgno * 0xfff);
	LockManager::pagerUnlock(pPger);
}



void BPlusTree::bpTreeInsert(int32_t k, string v)        //向B+树插入关键字  
{
	pghdr* r = new pghdr();//root
	Cache::AllocateNewPage(&r,pPger,pPger->cache);

	if (r->freeSpace <= FREESPACE_LIMIT)//如果root节点的关键字域满了
	{
		pghdr* newroot;
		Cache::AllocateNewPage(&newroot,pPger,false);
		/*在newroot中插入一个cell*/
		int root_k;
		int root_v;
		vector<unsigned char> c;
		DataManager::readCellToVec(c,r,5);

		if (r->isLeaf)
		{
			DataManager::solveCell(c, root_k, string());
		}
			
		else
		{
			DataManager::solveCell(c, root_k, root_v);
		}
		c.clear();
		/*跟新page1中的根节点为newroot*/
		pPger->root = newroot->pgno;
		vector<unsigned char> cell;
		DataManager::WrapToCell(cell, root_k, r->pgno);
		DataManager::updateCellInInternalPage(c,pPger->page1,DataManager::findTableCellPos(pPger->page1,pPger->tableName));//添加一个findcellinpage函数来查找指定的表

		DataManager::WrapToCell(c, root_k, r->pgno);
		DataManager::insertCellToPage(c, newroot, 11);
		c.clear();
		split(newroot, r, Pager::findMidPosOfPage(r),11);

		bpTreeInsert_aux(newroot, k, v);//如果root节点的关键字域没满，逐步下降，开始插入
	}
	//截止到这里为止都是在分裂根节点，下面开始逐步下降，遇到需要分裂的内部节点的话就进行分裂操作
	else
		bpTreeInsert_aux(r, k, v);//如果root节点的关键字域没满，逐步下降，开始插入
}



void BPlusTree::bpTreeInsert_aux(pghdr* cur, const int& k,const string& v)
{

	int i = DataManager::findUpperBound(cur,k);
	vector<unsigned char> c;
	int k_cur;
	if (-1 != i && cur->isLeaf)  //在B+树叶节点找到了相同关键字,直接返回
	{
		DataManager::readCellToVec(c,cur,i);
		DataManager::solveCell(c,k_cur,string());
		c.clear();
		if (k == k_cur)
		{
		//关键字插入重复  
		return;
		}
	}

	if (!cur->isLeaf)//如果不是叶节点  
	{
		int v_cur = 0;
		DataManager::readCellToVec(c, cur, i);
		DataManager::solveCell(c, k_cur, v_cur);
		c.clear();
		pghdr* t;
		Cache::fecthPage(&t,v_cur,pPger->cache);

		if (4096 - t->freeSpace > FULL_LIMIT)//如果t已满，则这个节点分裂  
		{
			split(cur,t,Pager::findMidPosOfPage(t),i);
			//如果分裂了，需要重新确定upperBound，更新k_cur和v_cur
			i = DataManager::findUpperBound(cur, k);
			DataManager::readCellToVec(c, cur, i);
			DataManager::solveCell(c, k_cur, v_cur);
			c.clear();
		}
		if (i!=-1)//如果找到了upperBound，那么左侧插入
		{

			pghdr* tmp;
			Cache::fecthPage(&tmp, v_cur, pPger->cache);
			bpTreeInsert_aux(tmp, k, v);
		}
		else//否则，右侧插入
		{
			int rightMostPointer;
			DataManager::writeByteArryToInt(&cur->image[7], rightMostPointer, 4);
			pghdr* tmp;
			Cache::fecthPage(&tmp, rightMostPointer, pPger->cache);
			bpTreeInsert_aux(tmp, k, v);
		}
	}
	else//如果是叶节点,且没有重复的关键字，且如果满的话在上一个递归中已经分裂完毕，直接将关键字插入key数组中  
	{
		DataManager::WrapToCell(c,k,v);
		DataManager::insertCellToPage(c, cur, i);
	}
}

void BPlusTree::split(pghdr * father, pghdr * current, const int cellPointerOfPartition, const int cellPointerOfCurInFather)
{
	if (current->isLeaf)
		split_leafVer(father,current, cellPointerOfPartition, cellPointerOfCurInFather);
	else
		split_internalVer(father, current, cellPointerOfPartition, cellPointerOfCurInFather);
}


/******************************************************************
Function: Split_BPlus_Node
Description:  分裂current节点。current节点有可能是内部节点或者叶子节点，
              如果是内部节点，需要检查是否需要改变最右孩子指针

Input:
father                         父节点
current                        当前节点
cellPointerOfPartition         分裂位置
cellPointerOfCurInFather       father节点中current节点所在的位置
Output:
Return:
Others:
******************************************************************/
void BPlusTree::split_internalVer(pghdr* father, pghdr* current, const int cellPointerOfPartition, const int cellPointerOfCurInFather)
{
	int half = cellPointerOfPartition;
	pghdr* t;
	Cache::AllocateNewPage(&t, pPger, false);
	/*提取分裂位置的信息，存入父节点。先insert再update*/
	vector<unsigned char> cell;
	int k;
	int v;
	int v_tmp = 0;//记录分裂出cell中的子节点指针，用于赋给分裂后的current的左右子节点指针

	/*insert*/
	DataManager::readCellToVec(cell, father, cellPointerOfCurInFather);
	DataManager::solveCell(cell, k, v);
	v_tmp = v;
	DataManager::WrapToCell(cell, k, t->pgno);
	DataManager::insertCellToPage(cell, father, cellPointerOfCurInFather);
	cell.clear();
	/*update*/
	DataManager::readCellToVec(cell, current, half);
	DataManager::solveCell(cell, k, v);
	cell.clear();
	DataManager::WrapToCell(cell, k, current->pgno);
	DataManager::updateCellInInternalPage(cell, father, cellPointerOfCurInFather + 2);
	cell.clear();

	/*把current的half之后的一半拷贝到t中*/
	/*先拷贝cell*/
	short copyStart = current->image[5] << 8 + current->image[5 + 1];
	short copyEnd = current->image[half + 2] << 8 + current->image[half + 3];
	int copyCellNum = current->iCellNum - (half - 7 + 2) / 2;
	copy(current->image[copyStart], current->image[4096 - copyEnd + copyStart], copyEnd - copyStart);

	/*更新current和t的剩余空间*/
	current->freeSpace -= (copyEnd - copyStart + copyCellNum * 2);
	current->image[1] = current->freeSpace & 0xff00;
	current->image[2] = current->freeSpace & 0xff;
	t->freeSpace = 4096 - 7 - copyCellNum * 2 - (copyEnd - copyStart);
	t->image[1] = t->freeSpace & 0xff00;
	t->image[2] = t->freeSpace & 0xff;

	/*再拷贝cellpointer*/
	short cellpointerIter = half + 2;
	int tmp = 0;
	for (int i = 0; i < copyCellNum; i++)
	{
		tmp = current->image[cellpointerIter] << 8 + current->image[cellpointerIter + 1] - (4095 - copyEnd);
		t->image[11 + 2 * i] = tmp & 0xff00;
		t->image[11 + 2 * i + 1] = tmp & 0xff;
		cellpointerIter += 2;
	}

	/*更新cell的数量*/
	current->iCellNum -= copyCellNum;
	current->image[3] = current->iCellNum & 0xff00;
	current->image[4] = current->iCellNum & 0xff;
	t->iCellNum = copyCellNum;
	t->image[3] = t->iCellNum & 0xff00;
	t->image[4] = t->iCellNum & 0xff;

	/*更新第一个cell实体的偏移量*/
	current->image[5] = (copyEnd + 1) & 0xff00;
	current->image[6] = (copyEnd + 1) & 0xff;
	t->image[5] = t->image[cellpointerIter - 2];
	t->image[6] = t->image[cellpointerIter - 1];
	/*修改最右子节点指针*/
	copy(current->image[7],t->image[7],4);
	DataManager::writeToByteArry(&v_tmp,&current->image[7],4);
	DataManager::deleteCellInPage(current,half);
	/*至此分裂已经完成*/

	/*如果current是father的最右子节点，则需要更新father的最右子节点指针而不是执行update操作*/
	if (father->image[cellPointerOfCurInFather + 2] ^ father->image[5] && father->image[cellPointerOfCurInFather + 3] ^ father->image[6])
	{
		/*删除update的cell*/
		DataManager::deleteCellInPage(father, cellPointerOfCurInFather + 2);
		/*更新最右子节点指针*/
		DataManager::writeToByteArry(&t->pgno, &father->image[11], 4);

	}

}


/******************************************************************
Function: split_leafVer
Description:  分裂current节点。本函数处理current节点是叶子节点的情况。
Input:
father           父节点
current          当前节点
cellpointer      分割点
Output:
Return:
Others:
******************************************************************/
void BPlusTree::split_leafVer(pghdr * father, pghdr * current, const int cellPointerOfPartition,const int cellPointerOfCurInFather)
{
	int half = cellPointerOfPartition;
	pghdr* t;
	Cache::AllocateNewPage(&t, pPger, true);
	/*提取分裂位置的信息，存入父节点。先insert再update*/
	vector<unsigned char> cell;
	int k;
	int v;
	string strV;
	/*insert*/
	DataManager::readCellToVec(cell, father, cellPointerOfCurInFather);
	DataManager::solveCell(cell, k, v);
	DataManager::WrapToCell(cell, k, t->pgno);
	DataManager::insertCellToPage(cell,father, cellPointerOfCurInFather);
	cell.clear();
	/*update*/
	DataManager::readCellToVec(cell, current, half);
	DataManager::solveCell(cell, k, strV);
	cell.clear();
	DataManager::WrapToCell(cell, k, current->pgno);
	DataManager::updateCellInInternalPage(cell, father, cellPointerOfCurInFather + 2);
	cell.clear();

	/*把current的half之后的一半拷贝到t中*/
	/*先拷贝cell*/
	short copyStart = current->image[5] << 8 + current->image[5 + 1];
	short copyEnd = current->image[half + 2] << 8 + current->image[half + 3];
	int copyCellNum = current->iCellNum - (half - 7 + 2) / 2;
	copy(current->image[copyStart], current->image[4096- copyEnd + copyStart], copyEnd- copyStart);

	/*更新current和t的剩余空间*/
	current->freeSpace -= (copyEnd - copyStart + copyCellNum * 2);
	current->image[1] = current->freeSpace & 0xff00;
	current->image[2] = current->freeSpace & 0xff;
	t->freeSpace = 4096 - 7 - copyCellNum * 2 - (copyEnd - copyStart);
	t->image[1] = t->freeSpace & 0xff00;
	t->image[2] = t->freeSpace & 0xff;

	/*再拷贝cellpointer*/
	short cellpointerIter = half + 2;
	int tmp = 0;
	for (int i = 0; i < copyCellNum; i++)
	{
		tmp = current->image[cellpointerIter] << 8 + current->image[cellpointerIter + 1] - (4095 - copyEnd);
		t->image[7 + 2 * i] = tmp & 0xff00;
		t->image[7 + 2 * i + 1] = tmp & 0xff;
		cellpointerIter += 2;
	}

	/*更新cell的数量*/
	current->iCellNum -= copyCellNum;
	current->image[3] = current->iCellNum & 0xff00;
	current->image[4] = current->iCellNum & 0xff;
	t->iCellNum = copyCellNum;
	t->image[3] = t->iCellNum & 0xff00;
	t->image[4] = t->iCellNum & 0xff;

	/*更新第一个cell实体的偏移量*/
	current->image[5] = (copyEnd + 1) & 0xff00;
	current->image[6] = (copyEnd + 1) & 0xff;
	t->image[5] = t->image[cellpointerIter - 2];
	t->image[6] = t->image[cellpointerIter - 1];
	/*至此分裂已经完成*/

	/*如果current是father的最右子节点，则需要更新father的最右子节点指针而不是执行update操作*/
	if (father->image[cellPointerOfCurInFather + 2] ^ father->image[5] && father->image[cellPointerOfCurInFather + 3] ^ father->image[6])
	{
		/*删除update的cell*/
		DataManager::deleteCellInPage(father, cellPointerOfCurInFather + 2);
		/*更新最右子节点指针*/
		DataManager::writeToByteArry(&t->pgno, &father->image[11], 4);
	
	}
}



void BPlusTree::bpTreeSearchByIndex(int key, string & str) 
{
	pghdr* a;
	Cache::fecthPage(&a, pPger->root, pPger->cache);
	int nextPgno;
	while (!a->isLeaf);
	{
		bpTreeSearch_aux_internalVer(key, nextPgno);
		Cache::fecthPage(&a, nextPgno, pPger->cache);
	} 
	if (bpTreeSearch_aux_leafVer(key, str) == -1)
		cerr << "No record found!\n";
	else
		cout << key << "," << str << endl;
}

int BPlusTree::bpTreeSearch_aux_internalVer(int key, int & nextPgno)
{
	int rec = -1;

	return rec;
}

void BPlusTree::bpTreeDelete(TRecord &record)    //在B+中删除一个关键字  
{
	bpTreeDelete_aux(ROOT, record);

	BPlusNode rootnode;
	ReadBPlusNode(ROOT, rootnode);

	if (!rootnode.isleaf && rootnode.nkey == 0)    //如果删除关键字后根节点不是叶节点，并且关键字数量为0时根节点也应该被删除  
	{
		//释放ROOT节点占用的空间  
		ROOT = rootnode.Pointer[0];         //根节点下移,B+树高度减1  

	}

}

void BPlusTree::bpTreeDelete_aux(pghdr* current, int& key, vector<unsigned char> & cell)
{
	int i, j;

	BPlusNode x;
	ReadBPlusNode(current, x);
	

	for (i = 0; i < x.nkey && record.key > x.key[i]; i++);

	if (i < x.nkey && x.key[i] == record.key)  //在当前节点找到关键字  
	{

		if (!x.isleaf)     //在内节点找到关键字  
		{
			BPlusNode child;
			ReadBPlusNode(x.Pointer[i], child);

			if (child.isleaf)     //如果孩子是叶节点  
			{
				if (child.nkey > MAX_KEY / 2)      //情况A  
				{
					x.key[i] = child.key[child.nkey - 2];
					child.nkey--;

					WriteBPlusNode(current, x);
					WriteBPlusNode(x.Pointer[i], child);

					return;
				}
				else    //否则孩子节点的关键字数量不过半  
				{
					if (i > 0)      //有左兄弟节点  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)        //情况B  
						{
							borrow(x, child, lbchild, i, current);
						}
						else    //情况C  
						{
							merge(x, child, lbchild, i, current);
						}
					}
					else      //只有右兄弟节点  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)        //情况D  
						{
							borrow(x, child, rbchild, i, current);
						}
						else    //情况E  
						{
							merge(x, child, rbchild, i, current);
						}
					}
				}
			}
			else      //情况F  
			{

				//找到key在B+树叶节点的左兄弟关键字,将这个关键字取代key的位置  

				TRecord trecord;
				trecord.key = record.key;
				SearchResult result;
				Search_BPlus_Tree(trecord, result);

				BPlusNode last;

				ReadBPlusNode(result.Baddress, last);

				x.key[i] = last.key[last.nkey - 2];

				WriteBPlusNode(current, x);


				if (child.nkey > MAX_KEY / 2)        //情况H  
				{
					//孩子节点的关键字数量达到半满，不做处理
				}
				else          //否则孩子节点的关键字数量不过半,则将兄弟节点的某一个关键字移至孩子  
				{
					if (i > 0)  //x.key[i]有左兄弟  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)       //情况I  
						{
							borrow(x, child, lbchild, i, current);
						}
						else        //情况J  
						{
							merge(x, child, lbchild, i, current);
						}

					}
					else        //否则x.key[i]只有右兄弟  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)     //情况K  
						{

							borrow(x, child, rbchild, i, current);

						}
						else        //情况L  
						{
							merge(x, child, rbchild, i, current);

						}
					}
				}
			}

			bpTreeDelete_aux(x.Pointer[i], record);

		}
		else  //情况G  
		{
			for (j = i; j < x.nkey - 1; j++)
			{
				x.key[j] = x.key[j + 1];
				x.Pointer[j] = x.Pointer[j + 1];
			}
			x.nkey--;

			WriteBPlusNode(current, x);

			return;
		}

	}
	else        //在当前节点没找到关键字     
	{
		if (!x.isleaf)    //没找到关键字,则关键字必然包含在以Pointer[i]为根的子树中  
		{
			BPlusNode child;
			ReadBPlusNode(x.Pointer[i], child);

			if (!child.isleaf)      //如果其孩子节点是内节点  
			{
				if (child.nkey > MAX_KEY / 2)        //情况H  
				{

				}
				else          //否则孩子节点的关键字数量不过半,则将兄弟节点的某一个关键字移至孩子  
				{
					if (i > 0)  //x.key[i]有左兄弟  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)       //情况I  
						{
							borrow(x, child, lbchild, i, current);
						}
						else        //情况J  
						{
							merge(x, child, lbchild, i, current);

						}

					}
					else        //否则x.key[i]只有右兄弟  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)     //情况K  
						{

							borrow(x, child, rbchild, i, current);

						}
						else        //情况L  
						{
							merge(x, child, rbchild, i, current);

						}

					}
				}
			}
			else  //否则其孩子节点是外节点  
			{
				if (child.nkey > MAX_KEY / 2)  //情况M  
				{

				}
				else        //否则孩子节点不到半满  
				{
					if (i > 0) //有左兄弟  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)       //情况N  
						{
							borrow(x, child, lbchild, i, current);

						}
						else        //情况O  
						{

							merge(x, child, lbchild, i, current);

						}

					}
					else        //否则只有右兄弟  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)       //情况P  
						{
							borrow(x, child, rbchild, i, current);

						}
						else        //情况Q  
						{
							merge(x, child, rbchild, i, current);
						}
					}
				}
			}
			bpTreeDelete_aux(x.Pointer[i], record);
		}
	}
}



void BPlusTree::borrow(pghdr * cur, pghdr * curChild, pghdr * adjChild, int pointerPosOfCur, bool borrowFromLeft)
{
	if (curChild->isLeaf)
		borrow_leafVer(cur,curChild, adjChild, pointerPosOfCur, borrowFromLeft);
	else
		borrow_internalVer(cur, curChild, adjChild, pointerPosOfCur, borrowFromLeft);
}

void BPlusTree::merge(pghdr * cur, pghdr * curChild, pghdr * adjChild, int pointerPosOfCur, bool mergeToLeft)
{
	if (curChild->isLeaf)
		merge_leafVer(cur, curChild, adjChild, pointerPosOfCur, mergeToLeft);
	else
		merge_internalVer(cur, curChild, adjChild, pointerPosOfCur, mergeToLeft);
}

/******************************************************************
Function: borrow_internalVer
Description:
B+树删除时有可能因为当前节点的孩子节点不够半满而向左右的兄弟孩子
节点借一条数据。本函数处理兄弟孩子节点和当前孩子节点都是内部节点的情况。
Input:
cur              当前节点
curChild         当前节点的孩子节点
adjChild         兄弟孩子节点
pointerPosOfCur  当前节点中，保存孩子节点页号的cell的cellpointer
在本节点中的位置
borrowFromLeft   为真时向左兄弟孩子节点借，为假时向右兄弟孩子节点借
Output:
Return:
Others:
******************************************************************/
void BPlusTree::borrow_internalVer(pghdr * cur, pghdr * curChild, pghdr * adjChild, int pointerPosOfCur, bool borrowFromLeft)
{
	vector<unsigned char> c;
	int k = 0;
	int v = 0;
	int k_adj = 0;
	int v_adj = 0;
	if (borrowFromLeft)/*从左边的叶子节点借*/
	{
		/*cur的左兄弟末尾键移动到curChild,adjChild的末尾键的指针移动到curChild*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur - 2);
		DataManager::solveCell(c, k, v);
		//DataManager::deleteCellInPage(cur, pointerPosOfCur - 2);
		c.clear();
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c, k_adj, v_adj);
		c.clear();
		DataManager::WrapToCell(c, k, v_adj);
		DataManager::insertCellToPage(c, curChild, 11);
		/*adjChild的末尾键移动到cur的左兄弟*/
		c.clear();
		DataManager::WrapToCell(c, k_adj, v);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur - 2);
		/*删除adjChild的最后一个cell*/
		DataManager::deleteCellInPage(adjChild, 5);
	}
	else/*从右边的叶子节点借*/
	{
		/*cur的当前键移动到curChild末尾,adjChild的第一个键的指针移动到curChild末尾*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur);
		DataManager::solveCell(c, k, v);
		c.clear();
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c, k_adj, v_adj);
		c.clear();
		DataManager::WrapToCell(c, k, v_adj);
		DataManager::insertCellToPage(c, curChild, 11);
		/*adjChild的第一个键移动到cur的当前键*/
		c.clear();
		DataManager::WrapToCell(c, k_adj, v);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur - 2);
		/*删除adjChild的第一个cell*/
		DataManager::deleteCellInPage(adjChild, 5);

	}

}
/******************************************************************
Function: merge_internalVer
Description:
B+树删除时有可能因为当前节点的孩子节点不够半满而向左右的兄弟孩子
节点合并。本函数处理兄弟孩子节点和当前孩子节点都是内部节点的情况。
无论adjChild是左兄弟孩子节点还是右兄弟孩子节点，合并方向都是向左。
Input:
cur              当前节点
curChild         当前节点的孩子节点
adjChild         兄弟孩子节点
pointerPosOfCur  当前节点中，保存孩子节点页号的cell的cellpointer
在本节点中的位置
mergeToLeft      为真时当前节点向左合并，为假时向右合并
Output:
Return:
Others:
******************************************************************/
void BPlusTree::merge_internalVer(pghdr * cur, pghdr * curChild, pghdr * adjChild, int pointerPosOfCur, bool mergeToLeft)
{
	int curKey = 0;
	int curValue = 0;
	int nop = 0;
	int adjRightMostPointer = 0;
	/*拷贝时4个指示位置的指针*/
	int adjChildIter_cellPointer = 0;
	int adjChildIter_cell = 0;
	int curChildIter_cellPointer = 0;
	int curChildIter_cell = 0;

	vector<unsigned char> c;
	vector<unsigned char> c_tmp;
	if (mergeToLeft)/*向左合并*/
	{
		/*将cur中pointerPosOfCur - 2所指的cell的key值下移至其孩子lbchild的末尾，
		和lbchild的最右指针一起组成cell添加到lbchild末端*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur - 2);
		DataManager::solveCell(c, curKey, curValue);
		for (int i = 0; i < 4; i++)
		{
			adjRightMostPointer |= adjChild->image[10 - i];
			adjRightMostPointer = adjRightMostPointer << 8;
		}
		DataManager::WrapToCell(c_tmp, curKey, adjRightMostPointer);
		DataManager::insertCellToPage(c_tmp, adjChild, -1);
		/*将child节点拷贝到lbchild的末尾*/
		/*把curChild的所有cell拷贝到adjChild,使用STL算法*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[11] << 8 + curChild->image[11 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*更新剩余空间*/
		adjChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		adjChild->image[1] = adjChild->freeSpace & 0xff00;
		adjChild->image[2] = adjChild->freeSpace & 0xff;

		/*把curChild的所有cellpointer拷贝到adjChild并做修改*/
		adjChildIter_cellPointer = 11 + adjChild->iCellNum * 2;
		curChildIter_cellPointer = 11;
		for (int i = 0; i < curChild->iCellNum; i++)
		{
			curChildIter_cell = curChild->image[curChildIter_cellPointer] << 8 + curChild->image[curChildIter_cellPointer + 1];
			curChildIter_cell -= (4095 - adjChildIter_cell);
			adjChild->image[adjChildIter_cellPointer] = curChildIter_cell & 0xff00;
			adjChild->image[adjChildIter_cellPointer + 1] = curChildIter_cell & 0xff;
			adjChildIter_cellPointer += 2;
			curChildIter_cellPointer += 2;
		}
		/*删除cur中pointerPosOfCur - 2所指的cell*/
		DataManager::deleteCellInPage(cur, pointerPosOfCur - 2);

		/*更新adjChild的最右指针，更新为curChild的最右指针*/
		if (adjChild->image[pointerPosOfCur] == adjChild->image[7] && adjChild->image[pointerPosOfCur + 1] == adjChild->image[8])
		{
			for (int i = 0; i < 4; i++)
			{
				adjChild->image[10 - i] = curChild->image[10 - i];
			}
		}
		/*更新本页中cell的数量*/
		adjChild->iCellNum += curChild->iCellNum;
		adjChild->image[3] = adjChild->iCellNum & 0xff00;
		adjChild->image[4] = adjChild->iCellNum & 0xff;

		/*更新第一个cell实体的偏移量*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];
	}
	else/*向右合并*/
	{
		/*为了编程方便，先合并到curChild，再拷贝到adjChild*/
		/*为了复用代码，交换curChild，adjChild*/
		swap(curChild, adjChild);
		/*将cur中pointerPosOfCur - 2所指的cell的key值下移至其孩子lbchild的末尾，
		和lbchild的最右指针一起组成cell添加到adjChild开头*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur);
		DataManager::solveCell(c, curKey, curValue);
		for (int i = 0; i < 4; i++)
		{
			adjRightMostPointer |= adjChild->image[10 - i];
			adjRightMostPointer = adjRightMostPointer << 8;
		}
		DataManager::WrapToCell(c_tmp, curKey, adjRightMostPointer);
		DataManager::insertCellToPage(c_tmp, adjChild, -1);
		/*将child节点拷贝到adjChild的末尾*/
		/*把curChild的所有cell拷贝到adjChild,使用STL算法*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[11] << 8 + curChild->image[11 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*更新剩余空间*/
		curChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		curChild->image[1] = curChild->freeSpace & 0xff00;
		curChild->image[2] = curChild->freeSpace & 0xff;

		/*把curChild的所有cellpointer拷贝到adjChild并做修改*/
		adjChildIter_cellPointer = 11 + adjChild->iCellNum * 2;
		curChildIter_cellPointer = 11;
		for (int i = 0; i < curChild->iCellNum; i++)
		{
			curChildIter_cell = curChild->image[curChildIter_cellPointer] << 8 + curChild->image[curChildIter_cellPointer + 1];
			curChildIter_cell -= (4095 - adjChildIter_cell);
			adjChild->image[adjChildIter_cellPointer] = curChildIter_cell & 0xff00;
			adjChild->image[adjChildIter_cellPointer + 1] = curChildIter_cell & 0xff;
			adjChildIter_cellPointer += 2;
			curChildIter_cellPointer += 2;
		}
		/*删除cur中pointerPosOfCur所指的cell*/
		DataManager::deleteCellInPage(cur, pointerPosOfCur);
		/*如果adjChild是右节点，有必要的话更新cur的最右节点指针*/

		/*更新adjChild的最右指针，更新为curChild的最右指针*/
		if (adjChild->image[pointerPosOfCur] == adjChild->image[7] && adjChild->image[pointerPosOfCur + 1] == adjChild->image[8])
		{
			for (int i = 0; i < 4; i++)
			{
				adjChild->image[10 - i] = curChild->image[10 - i];
			}
		}
		/*更新本页中cell的数量*/
		curChild->iCellNum += adjChild->iCellNum;
		curChild->image[3] = curChild->iCellNum & 0xff00;
		curChild->image[4] = curChild->iCellNum & 0xff;

		/*更新第一个cell实体的偏移量*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];

		/*拷贝回右节点*/
		swap(adjChild, curChild);
		copy(curChild->image[5], adjChild->image[5], 4091);
	}
	/*清空curChild*/
	curChild->image[0] = 0x00;
	/*TODO: 把清空后的页加入freelist*/
}
/******************************************************************
Function: borrow_leafVer
Description:
	B+树删除时有可能因为当前节点的孩子节点不够半满而向左右的兄弟孩子
节点借一条数据。本函数处理兄弟孩子节点和当前孩子节点都是叶子节点的情况。
Input:
	cur              当前节点
	curChild         当前节点的孩子节点
	adjChild         兄弟孩子节点
	pointerPosOfCur  当前节点中，保存孩子节点页号的cell的cellpointer
					 在本节点中的位置
	borrowFromLeft   为真时向左兄弟孩子节点借，为假时向右兄弟孩子节点借
Output:
Return:
Others:
******************************************************************/
void BPlusTree::borrow_leafVer(pghdr* cur, pghdr* curChild, pghdr* adjChild, int pointerPosOfCur, bool borrowFromLeft)
{
	int curKey = 0;
	int curValue = 0;
	int nop = 0;
	vector<unsigned char> c;
	vector<unsigned char> c_tmp;
	if (borrowFromLeft)/*从左边的叶子节点借*/
	{
		/*借用一个cell*/
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::insertCellToPage(c, curChild, 7);
		/*adjChild删除最后一个cell，即删除最后一个cellpointer就好了*/
		DataManager::deleteCellInPage(adjChild, 5);
		/*更新cur节点(父节点)，父节点永远是内部节点*/
		DataManager::readCellToVec(c_tmp, cur, pointerPosOfCur);
		DataManager::solveCell(c_tmp, curKey, curValue);
		DataManager::solveCell(c, curKey, nop);
		c.clear();
		DataManager::WrapToCell(c, curKey, curValue);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur);
	}
	else/*从右边的叶子节点借*/
	{
		/*借用一个cell*/
		DataManager::readCellToVec(c, adjChild, 7);
		DataManager::insertCellToPage(c, curChild, -1);
		/*adjChild删除最后一个cell，即删除最后一个cellpointer就好了*/
		DataManager::deleteCellInPage(adjChild, 7);
		/*更新cur节点(父节点)，父节点永远是内部节点*/
		DataManager::readCellToVec(c_tmp, cur, pointerPosOfCur);
		DataManager::solveCell(c_tmp, curKey, curValue);
		DataManager::solveCell(c, curKey, nop);
		c.clear();
		DataManager::WrapToCell(c, curKey, curValue);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur);
	}
}
/******************************************************************
Function: merge_leafVer
Description:
B+树删除时有可能因为当前节点的孩子节点不够半满而和左右的兄弟孩子
节点合并。本函数处理兄弟孩子节点和当前孩子节点都是叶子节点的情况。
Input:
cur              当前节点
curChild         当前节点的孩子节点
adjChild         兄弟孩子节点
pointerPosOfCur  当前节点中，保存孩子节点页号的cell的cellpointer
在本节点中的位置
mergeToLeft   为真时当前孩子节点向左兄弟孩子节点合并，为假时当前孩子节点向右兄
			  弟孩子节点合并
Output:
Return:
Others:
******************************************************************/
void BPlusTree::merge_leafVer(pghdr * cur, pghdr * curChild, pghdr * adjChild, int pointerPosOfCur, bool mergeToLeft)
{
	int curKey = 0;
	int curValue = 0;
	int nop = 0;
	/*拷贝时4个指示位置的指针*/
	int adjChildIter_cellPointer = 0;
	int adjChildIter_cell = 0;
	int curChildIter_cellPointer = 0;
	int curChildIter_cell = 0;

	vector<unsigned char> c;
	vector<unsigned char> c_tmp;
	if (mergeToLeft)/*向左边的叶子节点合并*/
	{
		/*把curChild的所有cell拷贝到adjChild,使用STL算法*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[7] << 8 + curChild->image[7 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*更新剩余空间*/
		adjChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		adjChild->image[1] = adjChild->freeSpace & 0xff00;
		adjChild->image[2] = adjChild->freeSpace & 0xff;

		/*把curChild的所有cellpointer拷贝到adjChild并做修改*/
		adjChildIter_cellPointer = 7 + adjChild->iCellNum * 2;
		curChildIter_cellPointer = 7;
		for (int i = 0; i < curChild->iCellNum; i++)
		{
			curChildIter_cell = curChild->image[curChildIter_cellPointer] << 8 + curChild->image[curChildIter_cellPointer + 1];
			curChildIter_cell -= (4095 - adjChildIter_cell);
			adjChild->image[adjChildIter_cellPointer] = curChildIter_cell & 0xff00;
			adjChild->image[adjChildIter_cellPointer + 1] = curChildIter_cell & 0xff;
			adjChildIter_cellPointer += 2;
			curChildIter_cellPointer += 2;
		}

		/*更新本页中cell的数量*/
		adjChild->iCellNum += curChild->iCellNum;
		adjChild->image[3] = adjChild->iCellNum & 0xff00;
		adjChild->image[4] = adjChild->iCellNum & 0xff;

		/*更新第一个cell实体的偏移量*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];

		/*如果有必要的话，更新cur节点(父节点)的最右孩子指针*/
		if (cur->image[pointerPosOfCur] == cur->image[7] && cur->image[pointerPosOfCur + 1] == cur->image[8])
		{
			int pgnoTmp = adjChild->pgno;
			for (int i = 0; i < 4; i++)
			{
				cur->image[10 - i] = pgnoTmp & 0xff;
				pgnoTmp = pgnoTmp >> 8;
			}
		}


		/*更新cur节点(父节点)，父节点永远是内部节点*/
		//DataManager::deleteCellInPage(cur, pointerPosOfCur);
		DataManager::readCellToVec(c_tmp, cur, pointerPosOfCur);
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c_tmp, curKey, curValue);
		DataManager::solveCell(c, curKey, nop);
		c.clear();
		DataManager::WrapToCell(c, curKey, curValue);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur);
	}
	else/*向右合并。为了编程方便，先向左合并，然后拷贝cell到右节点*/
	{
		/*交换指针，复用代码*/
		swap(adjChild, curChild);
		/*把adjChild的所有cell拷贝到curChild,使用STL算法*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[7] << 8 + curChild->image[7 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*更新剩余空间*/
		curChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		curChild->image[1] = curChild->freeSpace & 0xff00;
		curChild->image[2] = curChild->freeSpace & 0xff;

		/*把adjChild的所有cellpointer拷贝到curChild并做修改*/
		adjChildIter_cellPointer = 7 + adjChild->iCellNum * 2;
		curChildIter_cellPointer = 7;
		for (int i = 0; i < curChild->iCellNum; i++)
		{
			curChildIter_cell = curChild->image[curChildIter_cellPointer] << 8 + curChild->image[curChildIter_cellPointer + 1];
			curChildIter_cell -= (4095 - adjChildIter_cell);
			adjChild->image[adjChildIter_cellPointer] = curChildIter_cell & 0xff00;
			adjChild->image[adjChildIter_cellPointer + 1] = curChildIter_cell & 0xff;
			adjChildIter_cellPointer += 2;
			curChildIter_cellPointer += 2;
		}


		/*更新本页中cell的数量*/
		curChild->iCellNum = adjChild->iCellNum;
		curChild->image[3] = curChild->iCellNum & 0xff00;
		curChild->image[4] = curChild->iCellNum & 0xff;

		/*更新第一个cell实体的偏移量*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];

		/*拷贝回右节点*/
		swap(adjChild, curChild);
		copy(curChild->image[5], adjChild->image[5], 4091);

		/*更新cur节点(父节点)，父节点永远是内部节点*/
		//DataManager::deleteCellInPage(cur, pointerPosOfCur);
		DataManager::readCellToVec(c_tmp, cur, pointerPosOfCur);
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c_tmp, curKey, curValue);
		DataManager::solveCell(c, curKey, nop);
		c.clear();
		DataManager::WrapToCell(c, curKey, curValue);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur);

	}
	/*清空curChild*/
	curChild->image[0] = 0x00;
	/*TODO: 把清空后的页加入freelist*/
}




//inline FILEP BPlusTree::GetBPlusNode()  const //在磁盘上分配一块B+树节点空间,add to file's tail
//{
//	fseek(Bfile, 0, SEEK_END);
//
//	return  ftell(Bfile);
//}
//
//inline void BPlusTree::ReadPageByPgNo(pghdr *r) const //读取address地址上的一块B+树节点  
//{
//	LockManager::pagerWaitOnLock(pPager, SHARED_LOCK);
//	mdbFile::winRead(pPager->fd, r->image, 0xfff, r->pgno * 0xfff);
//	LockManager::pagerUnlock(pPager);
//	Pager::formatPghdr(r);
//}
//
//
//inline void BPlusTree::WriteBPlusNode(const FILEP address, const BPlusNode &r) //将一个B+树节点写入address地址  
//{
//	fseek(Bfile, address, SEEK_SET);
//	fwrite((char*)(&r), sizeof(BPlusNode), 1, Bfile);
//}



int main()
{
	BPlusTree tree;

	tree.Build_BPlus_Tree();      //建树  

	TRecord record;   SearchResult result;

	int time1 = clock();

	int i;
	for (i = 0; i < 4; i++)
	{
		record.key = i;

		tree.bpTreeInsert(record);
		//  printf("%d\n",i );  
	}

	for (i = 3; i > 0; i--)
	{
		record.key = i;
		tree.bpTreeDelete(record);
		tree.Search_BPlus_Tree(record, result);
		if (result.exist)
		{
			break;
			printf("%d\n", i);
		}

	}

	cout << clock() - time1 << endl;
	system("pause");
	tree.EnumLeafKey();

	tree.~BPlusTree();
	system("pause");
	return 0;
}