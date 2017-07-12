#include"pageBTree.h"
#include"mdbFile.h"
#include"LockManager.h"
#include"DataManager.h"
#include"Cache.h"
//FILEP BPlusTree::writeNewPageAtTail(Pager *pPager)  const //�ڴ����Ϸ���һ��B+���ڵ�ռ�,add to file's tail
//{
//	pghdr r;
//	LockManager::pagerWaitOnLock(pPager, EXCLUSIVE_LOCK);
//	mdbFile::winWrite(pPager->fd, r.image, 0xfff, r.pgno * 0xfff);
//	LockManager::pagerUnlock(pPager);
//	return 0;
//}

void BPlusTree::buildNewBPTree(Pager *pPager)   //����һ�ÿ�B+��  
{
	//FILEP ROOT = allocNewPageAtTail();
	pghdr r;
	LockManager::pagerWaitOnLock(pPager, EXCLUSIVE_LOCK);
	mdbFile::winWrite(pPager->fd, r.image, 0xfff, r.pgno * 0xfff);
	LockManager::pagerUnlock(pPager);
}

BPlusTree::BPlusTree()
{
	//Bfile = fopen("Bfile", "rb+");     //��B+���ļ�  

}

BPlusTree :: ~BPlusTree()
{
	//fclose(Bfile);
}



void BPlusTree::Build_BPlus_Tree()   //����һ�ÿ�B+��  
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



void BPlusTree::bpTreeInsert(int32_t k, string v)        //��B+������ؼ���  
{
	pghdr* r = new pghdr();//root
	Cache::AllocateNewPage(&r,pPger,pPger->cache);

	if (r->freeSpace <= FREESPACE_LIMIT)//���root�ڵ�Ĺؼ���������
	{
		pghdr* newroot;
		Cache::AllocateNewPage(&newroot,pPger,false);
		/*��newroot�в���һ��cell*/
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
		/*����page1�еĸ��ڵ�Ϊnewroot*/
		pPger->root = newroot->pgno;
		vector<unsigned char> cell;
		DataManager::WrapToCell(cell, root_k, r->pgno);
		DataManager::updateCellInInternalPage(c,pPger->page1,DataManager::findTableCellPos(pPger->page1,pPger->tableName));//����һ��findcellinpage����������ָ���ı�

		DataManager::WrapToCell(c, root_k, r->pgno);
		DataManager::insertCellToPage(c, newroot, 11);
		c.clear();
		split(newroot, r, Pager::findMidPosOfPage(r),11);

		bpTreeInsert_aux(newroot, k, v);//���root�ڵ�Ĺؼ�����û�������½�����ʼ����
	}
	//��ֹ������Ϊֹ�����ڷ��Ѹ��ڵ㣬���濪ʼ���½���������Ҫ���ѵ��ڲ��ڵ�Ļ��ͽ��з��Ѳ���
	else
		bpTreeInsert_aux(r, k, v);//���root�ڵ�Ĺؼ�����û�������½�����ʼ����
}



void BPlusTree::bpTreeInsert_aux(pghdr* cur, const int& k,const string& v)
{

	int i = DataManager::findUpperBound(cur,k);
	vector<unsigned char> c;
	int k_cur;
	if (-1 != i && cur->isLeaf)  //��B+��Ҷ�ڵ��ҵ�����ͬ�ؼ���,ֱ�ӷ���
	{
		DataManager::readCellToVec(c,cur,i);
		DataManager::solveCell(c,k_cur,string());
		c.clear();
		if (k == k_cur)
		{
		//�ؼ��ֲ����ظ�  
		return;
		}
	}

	if (!cur->isLeaf)//�������Ҷ�ڵ�  
	{
		int v_cur = 0;
		DataManager::readCellToVec(c, cur, i);
		DataManager::solveCell(c, k_cur, v_cur);
		c.clear();
		pghdr* t;
		Cache::fecthPage(&t,v_cur,pPger->cache);

		if (4096 - t->freeSpace > FULL_LIMIT)//���t������������ڵ����  
		{
			split(cur,t,Pager::findMidPosOfPage(t),i);
			//��������ˣ���Ҫ����ȷ��upperBound������k_cur��v_cur
			i = DataManager::findUpperBound(cur, k);
			DataManager::readCellToVec(c, cur, i);
			DataManager::solveCell(c, k_cur, v_cur);
			c.clear();
		}
		if (i!=-1)//����ҵ���upperBound����ô������
		{

			pghdr* tmp;
			Cache::fecthPage(&tmp, v_cur, pPger->cache);
			bpTreeInsert_aux(tmp, k, v);
		}
		else//�����Ҳ����
		{
			int rightMostPointer;
			DataManager::writeByteArryToInt(&cur->image[7], rightMostPointer, 4);
			pghdr* tmp;
			Cache::fecthPage(&tmp, rightMostPointer, pPger->cache);
			bpTreeInsert_aux(tmp, k, v);
		}
	}
	else//�����Ҷ�ڵ�,��û���ظ��Ĺؼ��֣���������Ļ�����һ���ݹ����Ѿ�������ϣ�ֱ�ӽ��ؼ��ֲ���key������  
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
Description:  ����current�ڵ㡣current�ڵ��п������ڲ��ڵ����Ҷ�ӽڵ㣬
              ������ڲ��ڵ㣬��Ҫ����Ƿ���Ҫ�ı����Һ���ָ��

Input:
father                         ���ڵ�
current                        ��ǰ�ڵ�
cellPointerOfPartition         ����λ��
cellPointerOfCurInFather       father�ڵ���current�ڵ����ڵ�λ��
Output:
Return:
Others:
******************************************************************/
void BPlusTree::split_internalVer(pghdr* father, pghdr* current, const int cellPointerOfPartition, const int cellPointerOfCurInFather)
{
	int half = cellPointerOfPartition;
	pghdr* t;
	Cache::AllocateNewPage(&t, pPger, false);
	/*��ȡ����λ�õ���Ϣ�����븸�ڵ㡣��insert��update*/
	vector<unsigned char> cell;
	int k;
	int v;
	int v_tmp = 0;//��¼���ѳ�cell�е��ӽڵ�ָ�룬���ڸ������Ѻ��current�������ӽڵ�ָ��

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

	/*��current��half֮���һ�뿽����t��*/
	/*�ȿ���cell*/
	short copyStart = current->image[5] << 8 + current->image[5 + 1];
	short copyEnd = current->image[half + 2] << 8 + current->image[half + 3];
	int copyCellNum = current->iCellNum - (half - 7 + 2) / 2;
	copy(current->image[copyStart], current->image[4096 - copyEnd + copyStart], copyEnd - copyStart);

	/*����current��t��ʣ��ռ�*/
	current->freeSpace -= (copyEnd - copyStart + copyCellNum * 2);
	current->image[1] = current->freeSpace & 0xff00;
	current->image[2] = current->freeSpace & 0xff;
	t->freeSpace = 4096 - 7 - copyCellNum * 2 - (copyEnd - copyStart);
	t->image[1] = t->freeSpace & 0xff00;
	t->image[2] = t->freeSpace & 0xff;

	/*�ٿ���cellpointer*/
	short cellpointerIter = half + 2;
	int tmp = 0;
	for (int i = 0; i < copyCellNum; i++)
	{
		tmp = current->image[cellpointerIter] << 8 + current->image[cellpointerIter + 1] - (4095 - copyEnd);
		t->image[11 + 2 * i] = tmp & 0xff00;
		t->image[11 + 2 * i + 1] = tmp & 0xff;
		cellpointerIter += 2;
	}

	/*����cell������*/
	current->iCellNum -= copyCellNum;
	current->image[3] = current->iCellNum & 0xff00;
	current->image[4] = current->iCellNum & 0xff;
	t->iCellNum = copyCellNum;
	t->image[3] = t->iCellNum & 0xff00;
	t->image[4] = t->iCellNum & 0xff;

	/*���µ�һ��cellʵ���ƫ����*/
	current->image[5] = (copyEnd + 1) & 0xff00;
	current->image[6] = (copyEnd + 1) & 0xff;
	t->image[5] = t->image[cellpointerIter - 2];
	t->image[6] = t->image[cellpointerIter - 1];
	/*�޸������ӽڵ�ָ��*/
	copy(current->image[7],t->image[7],4);
	DataManager::writeToByteArry(&v_tmp,&current->image[7],4);
	DataManager::deleteCellInPage(current,half);
	/*���˷����Ѿ����*/

	/*���current��father�������ӽڵ㣬����Ҫ����father�������ӽڵ�ָ�������ִ��update����*/
	if (father->image[cellPointerOfCurInFather + 2] ^ father->image[5] && father->image[cellPointerOfCurInFather + 3] ^ father->image[6])
	{
		/*ɾ��update��cell*/
		DataManager::deleteCellInPage(father, cellPointerOfCurInFather + 2);
		/*���������ӽڵ�ָ��*/
		DataManager::writeToByteArry(&t->pgno, &father->image[11], 4);

	}

}


/******************************************************************
Function: split_leafVer
Description:  ����current�ڵ㡣����������current�ڵ���Ҷ�ӽڵ�������
Input:
father           ���ڵ�
current          ��ǰ�ڵ�
cellpointer      �ָ��
Output:
Return:
Others:
******************************************************************/
void BPlusTree::split_leafVer(pghdr * father, pghdr * current, const int cellPointerOfPartition,const int cellPointerOfCurInFather)
{
	int half = cellPointerOfPartition;
	pghdr* t;
	Cache::AllocateNewPage(&t, pPger, true);
	/*��ȡ����λ�õ���Ϣ�����븸�ڵ㡣��insert��update*/
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

	/*��current��half֮���һ�뿽����t��*/
	/*�ȿ���cell*/
	short copyStart = current->image[5] << 8 + current->image[5 + 1];
	short copyEnd = current->image[half + 2] << 8 + current->image[half + 3];
	int copyCellNum = current->iCellNum - (half - 7 + 2) / 2;
	copy(current->image[copyStart], current->image[4096- copyEnd + copyStart], copyEnd- copyStart);

	/*����current��t��ʣ��ռ�*/
	current->freeSpace -= (copyEnd - copyStart + copyCellNum * 2);
	current->image[1] = current->freeSpace & 0xff00;
	current->image[2] = current->freeSpace & 0xff;
	t->freeSpace = 4096 - 7 - copyCellNum * 2 - (copyEnd - copyStart);
	t->image[1] = t->freeSpace & 0xff00;
	t->image[2] = t->freeSpace & 0xff;

	/*�ٿ���cellpointer*/
	short cellpointerIter = half + 2;
	int tmp = 0;
	for (int i = 0; i < copyCellNum; i++)
	{
		tmp = current->image[cellpointerIter] << 8 + current->image[cellpointerIter + 1] - (4095 - copyEnd);
		t->image[7 + 2 * i] = tmp & 0xff00;
		t->image[7 + 2 * i + 1] = tmp & 0xff;
		cellpointerIter += 2;
	}

	/*����cell������*/
	current->iCellNum -= copyCellNum;
	current->image[3] = current->iCellNum & 0xff00;
	current->image[4] = current->iCellNum & 0xff;
	t->iCellNum = copyCellNum;
	t->image[3] = t->iCellNum & 0xff00;
	t->image[4] = t->iCellNum & 0xff;

	/*���µ�һ��cellʵ���ƫ����*/
	current->image[5] = (copyEnd + 1) & 0xff00;
	current->image[6] = (copyEnd + 1) & 0xff;
	t->image[5] = t->image[cellpointerIter - 2];
	t->image[6] = t->image[cellpointerIter - 1];
	/*���˷����Ѿ����*/

	/*���current��father�������ӽڵ㣬����Ҫ����father�������ӽڵ�ָ�������ִ��update����*/
	if (father->image[cellPointerOfCurInFather + 2] ^ father->image[5] && father->image[cellPointerOfCurInFather + 3] ^ father->image[6])
	{
		/*ɾ��update��cell*/
		DataManager::deleteCellInPage(father, cellPointerOfCurInFather + 2);
		/*���������ӽڵ�ָ��*/
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

void BPlusTree::bpTreeDelete(TRecord &record)    //��B+��ɾ��һ���ؼ���  
{
	bpTreeDelete_aux(ROOT, record);

	BPlusNode rootnode;
	ReadBPlusNode(ROOT, rootnode);

	if (!rootnode.isleaf && rootnode.nkey == 0)    //���ɾ���ؼ��ֺ���ڵ㲻��Ҷ�ڵ㣬���ҹؼ�������Ϊ0ʱ���ڵ�ҲӦ�ñ�ɾ��  
	{
		//�ͷ�ROOT�ڵ�ռ�õĿռ�  
		ROOT = rootnode.Pointer[0];         //���ڵ�����,B+���߶ȼ�1  

	}

}

void BPlusTree::bpTreeDelete_aux(pghdr* current, int& key, vector<unsigned char> & cell)
{
	int i, j;

	BPlusNode x;
	ReadBPlusNode(current, x);
	

	for (i = 0; i < x.nkey && record.key > x.key[i]; i++);

	if (i < x.nkey && x.key[i] == record.key)  //�ڵ�ǰ�ڵ��ҵ��ؼ���  
	{

		if (!x.isleaf)     //���ڽڵ��ҵ��ؼ���  
		{
			BPlusNode child;
			ReadBPlusNode(x.Pointer[i], child);

			if (child.isleaf)     //���������Ҷ�ڵ�  
			{
				if (child.nkey > MAX_KEY / 2)      //���A  
				{
					x.key[i] = child.key[child.nkey - 2];
					child.nkey--;

					WriteBPlusNode(current, x);
					WriteBPlusNode(x.Pointer[i], child);

					return;
				}
				else    //�����ӽڵ�Ĺؼ�������������  
				{
					if (i > 0)      //�����ֵܽڵ�  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)        //���B  
						{
							borrow(x, child, lbchild, i, current);
						}
						else    //���C  
						{
							merge(x, child, lbchild, i, current);
						}
					}
					else      //ֻ�����ֵܽڵ�  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)        //���D  
						{
							borrow(x, child, rbchild, i, current);
						}
						else    //���E  
						{
							merge(x, child, rbchild, i, current);
						}
					}
				}
			}
			else      //���F  
			{

				//�ҵ�key��B+��Ҷ�ڵ�����ֵܹؼ���,������ؼ���ȡ��key��λ��  

				TRecord trecord;
				trecord.key = record.key;
				SearchResult result;
				Search_BPlus_Tree(trecord, result);

				BPlusNode last;

				ReadBPlusNode(result.Baddress, last);

				x.key[i] = last.key[last.nkey - 2];

				WriteBPlusNode(current, x);


				if (child.nkey > MAX_KEY / 2)        //���H  
				{
					//���ӽڵ�Ĺؼ��������ﵽ��������������
				}
				else          //�����ӽڵ�Ĺؼ�������������,���ֵܽڵ��ĳһ���ؼ�����������  
				{
					if (i > 0)  //x.key[i]�����ֵ�  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)       //���I  
						{
							borrow(x, child, lbchild, i, current);
						}
						else        //���J  
						{
							merge(x, child, lbchild, i, current);
						}

					}
					else        //����x.key[i]ֻ�����ֵ�  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)     //���K  
						{

							borrow(x, child, rbchild, i, current);

						}
						else        //���L  
						{
							merge(x, child, rbchild, i, current);

						}
					}
				}
			}

			bpTreeDelete_aux(x.Pointer[i], record);

		}
		else  //���G  
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
	else        //�ڵ�ǰ�ڵ�û�ҵ��ؼ���     
	{
		if (!x.isleaf)    //û�ҵ��ؼ���,��ؼ��ֱ�Ȼ��������Pointer[i]Ϊ����������  
		{
			BPlusNode child;
			ReadBPlusNode(x.Pointer[i], child);

			if (!child.isleaf)      //����亢�ӽڵ����ڽڵ�  
			{
				if (child.nkey > MAX_KEY / 2)        //���H  
				{

				}
				else          //�����ӽڵ�Ĺؼ�������������,���ֵܽڵ��ĳһ���ؼ�����������  
				{
					if (i > 0)  //x.key[i]�����ֵ�  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)       //���I  
						{
							borrow(x, child, lbchild, i, current);
						}
						else        //���J  
						{
							merge(x, child, lbchild, i, current);

						}

					}
					else        //����x.key[i]ֻ�����ֵ�  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)     //���K  
						{

							borrow(x, child, rbchild, i, current);

						}
						else        //���L  
						{
							merge(x, child, rbchild, i, current);

						}

					}
				}
			}
			else  //�����亢�ӽڵ�����ڵ�  
			{
				if (child.nkey > MAX_KEY / 2)  //���M  
				{

				}
				else        //�����ӽڵ㲻������  
				{
					if (i > 0) //�����ֵ�  
					{
						BPlusNode lbchild;
						ReadBPlusNode(x.Pointer[i - 1], lbchild);

						if (lbchild.nkey > MAX_KEY / 2)       //���N  
						{
							borrow(x, child, lbchild, i, current);

						}
						else        //���O  
						{

							merge(x, child, lbchild, i, current);

						}

					}
					else        //����ֻ�����ֵ�  
					{
						BPlusNode rbchild;
						ReadBPlusNode(x.Pointer[i + 1], rbchild);

						if (rbchild.nkey > MAX_KEY / 2)       //���P  
						{
							borrow(x, child, rbchild, i, current);

						}
						else        //���Q  
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
B+��ɾ��ʱ�п�����Ϊ��ǰ�ڵ�ĺ��ӽڵ㲻�������������ҵ��ֵܺ���
�ڵ��һ�����ݡ������������ֵܺ��ӽڵ�͵�ǰ���ӽڵ㶼���ڲ��ڵ�������
Input:
cur              ��ǰ�ڵ�
curChild         ��ǰ�ڵ�ĺ��ӽڵ�
adjChild         �ֵܺ��ӽڵ�
pointerPosOfCur  ��ǰ�ڵ��У����溢�ӽڵ�ҳ�ŵ�cell��cellpointer
�ڱ��ڵ��е�λ��
borrowFromLeft   Ϊ��ʱ�����ֵܺ��ӽڵ�裬Ϊ��ʱ�����ֵܺ��ӽڵ��
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
	if (borrowFromLeft)/*����ߵ�Ҷ�ӽڵ��*/
	{
		/*cur�����ֵ�ĩβ���ƶ���curChild,adjChild��ĩβ����ָ���ƶ���curChild*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur - 2);
		DataManager::solveCell(c, k, v);
		//DataManager::deleteCellInPage(cur, pointerPosOfCur - 2);
		c.clear();
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c, k_adj, v_adj);
		c.clear();
		DataManager::WrapToCell(c, k, v_adj);
		DataManager::insertCellToPage(c, curChild, 11);
		/*adjChild��ĩβ���ƶ���cur�����ֵ�*/
		c.clear();
		DataManager::WrapToCell(c, k_adj, v);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur - 2);
		/*ɾ��adjChild�����һ��cell*/
		DataManager::deleteCellInPage(adjChild, 5);
	}
	else/*���ұߵ�Ҷ�ӽڵ��*/
	{
		/*cur�ĵ�ǰ���ƶ���curChildĩβ,adjChild�ĵ�һ������ָ���ƶ���curChildĩβ*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur);
		DataManager::solveCell(c, k, v);
		c.clear();
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c, k_adj, v_adj);
		c.clear();
		DataManager::WrapToCell(c, k, v_adj);
		DataManager::insertCellToPage(c, curChild, 11);
		/*adjChild�ĵ�һ�����ƶ���cur�ĵ�ǰ��*/
		c.clear();
		DataManager::WrapToCell(c, k_adj, v);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur - 2);
		/*ɾ��adjChild�ĵ�һ��cell*/
		DataManager::deleteCellInPage(adjChild, 5);

	}

}
/******************************************************************
Function: merge_internalVer
Description:
B+��ɾ��ʱ�п�����Ϊ��ǰ�ڵ�ĺ��ӽڵ㲻�������������ҵ��ֵܺ���
�ڵ�ϲ��������������ֵܺ��ӽڵ�͵�ǰ���ӽڵ㶼���ڲ��ڵ�������
����adjChild�����ֵܺ��ӽڵ㻹�����ֵܺ��ӽڵ㣬�ϲ�����������
Input:
cur              ��ǰ�ڵ�
curChild         ��ǰ�ڵ�ĺ��ӽڵ�
adjChild         �ֵܺ��ӽڵ�
pointerPosOfCur  ��ǰ�ڵ��У����溢�ӽڵ�ҳ�ŵ�cell��cellpointer
�ڱ��ڵ��е�λ��
mergeToLeft      Ϊ��ʱ��ǰ�ڵ�����ϲ���Ϊ��ʱ���Һϲ�
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
	/*����ʱ4��ָʾλ�õ�ָ��*/
	int adjChildIter_cellPointer = 0;
	int adjChildIter_cell = 0;
	int curChildIter_cellPointer = 0;
	int curChildIter_cell = 0;

	vector<unsigned char> c;
	vector<unsigned char> c_tmp;
	if (mergeToLeft)/*����ϲ�*/
	{
		/*��cur��pointerPosOfCur - 2��ָ��cell��keyֵ�������亢��lbchild��ĩβ��
		��lbchild������ָ��һ�����cell���ӵ�lbchildĩ��*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur - 2);
		DataManager::solveCell(c, curKey, curValue);
		for (int i = 0; i < 4; i++)
		{
			adjRightMostPointer |= adjChild->image[10 - i];
			adjRightMostPointer = adjRightMostPointer << 8;
		}
		DataManager::WrapToCell(c_tmp, curKey, adjRightMostPointer);
		DataManager::insertCellToPage(c_tmp, adjChild, -1);
		/*��child�ڵ㿽����lbchild��ĩβ*/
		/*��curChild������cell������adjChild,ʹ��STL�㷨*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[11] << 8 + curChild->image[11 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*����ʣ��ռ�*/
		adjChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		adjChild->image[1] = adjChild->freeSpace & 0xff00;
		adjChild->image[2] = adjChild->freeSpace & 0xff;

		/*��curChild������cellpointer������adjChild�����޸�*/
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
		/*ɾ��cur��pointerPosOfCur - 2��ָ��cell*/
		DataManager::deleteCellInPage(cur, pointerPosOfCur - 2);

		/*����adjChild������ָ�룬����ΪcurChild������ָ��*/
		if (adjChild->image[pointerPosOfCur] == adjChild->image[7] && adjChild->image[pointerPosOfCur + 1] == adjChild->image[8])
		{
			for (int i = 0; i < 4; i++)
			{
				adjChild->image[10 - i] = curChild->image[10 - i];
			}
		}
		/*���±�ҳ��cell������*/
		adjChild->iCellNum += curChild->iCellNum;
		adjChild->image[3] = adjChild->iCellNum & 0xff00;
		adjChild->image[4] = adjChild->iCellNum & 0xff;

		/*���µ�һ��cellʵ���ƫ����*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];
	}
	else/*���Һϲ�*/
	{
		/*Ϊ�˱�̷��㣬�Ⱥϲ���curChild���ٿ�����adjChild*/
		/*Ϊ�˸��ô��룬����curChild��adjChild*/
		swap(curChild, adjChild);
		/*��cur��pointerPosOfCur - 2��ָ��cell��keyֵ�������亢��lbchild��ĩβ��
		��lbchild������ָ��һ�����cell���ӵ�adjChild��ͷ*/
		DataManager::readCellToVec(c, cur, pointerPosOfCur);
		DataManager::solveCell(c, curKey, curValue);
		for (int i = 0; i < 4; i++)
		{
			adjRightMostPointer |= adjChild->image[10 - i];
			adjRightMostPointer = adjRightMostPointer << 8;
		}
		DataManager::WrapToCell(c_tmp, curKey, adjRightMostPointer);
		DataManager::insertCellToPage(c_tmp, adjChild, -1);
		/*��child�ڵ㿽����adjChild��ĩβ*/
		/*��curChild������cell������adjChild,ʹ��STL�㷨*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[11] << 8 + curChild->image[11 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*����ʣ��ռ�*/
		curChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		curChild->image[1] = curChild->freeSpace & 0xff00;
		curChild->image[2] = curChild->freeSpace & 0xff;

		/*��curChild������cellpointer������adjChild�����޸�*/
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
		/*ɾ��cur��pointerPosOfCur��ָ��cell*/
		DataManager::deleteCellInPage(cur, pointerPosOfCur);
		/*���adjChild���ҽڵ㣬�б�Ҫ�Ļ�����cur�����ҽڵ�ָ��*/

		/*����adjChild������ָ�룬����ΪcurChild������ָ��*/
		if (adjChild->image[pointerPosOfCur] == adjChild->image[7] && adjChild->image[pointerPosOfCur + 1] == adjChild->image[8])
		{
			for (int i = 0; i < 4; i++)
			{
				adjChild->image[10 - i] = curChild->image[10 - i];
			}
		}
		/*���±�ҳ��cell������*/
		curChild->iCellNum += adjChild->iCellNum;
		curChild->image[3] = curChild->iCellNum & 0xff00;
		curChild->image[4] = curChild->iCellNum & 0xff;

		/*���µ�һ��cellʵ���ƫ����*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];

		/*�������ҽڵ�*/
		swap(adjChild, curChild);
		copy(curChild->image[5], adjChild->image[5], 4091);
	}
	/*���curChild*/
	curChild->image[0] = 0x00;
	/*TODO: ����պ��ҳ����freelist*/
}
/******************************************************************
Function: borrow_leafVer
Description:
	B+��ɾ��ʱ�п�����Ϊ��ǰ�ڵ�ĺ��ӽڵ㲻�������������ҵ��ֵܺ���
�ڵ��һ�����ݡ������������ֵܺ��ӽڵ�͵�ǰ���ӽڵ㶼��Ҷ�ӽڵ�������
Input:
	cur              ��ǰ�ڵ�
	curChild         ��ǰ�ڵ�ĺ��ӽڵ�
	adjChild         �ֵܺ��ӽڵ�
	pointerPosOfCur  ��ǰ�ڵ��У����溢�ӽڵ�ҳ�ŵ�cell��cellpointer
					 �ڱ��ڵ��е�λ��
	borrowFromLeft   Ϊ��ʱ�����ֵܺ��ӽڵ�裬Ϊ��ʱ�����ֵܺ��ӽڵ��
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
	if (borrowFromLeft)/*����ߵ�Ҷ�ӽڵ��*/
	{
		/*����һ��cell*/
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::insertCellToPage(c, curChild, 7);
		/*adjChildɾ�����һ��cell����ɾ�����һ��cellpointer�ͺ���*/
		DataManager::deleteCellInPage(adjChild, 5);
		/*����cur�ڵ�(���ڵ�)�����ڵ���Զ���ڲ��ڵ�*/
		DataManager::readCellToVec(c_tmp, cur, pointerPosOfCur);
		DataManager::solveCell(c_tmp, curKey, curValue);
		DataManager::solveCell(c, curKey, nop);
		c.clear();
		DataManager::WrapToCell(c, curKey, curValue);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur);
	}
	else/*���ұߵ�Ҷ�ӽڵ��*/
	{
		/*����һ��cell*/
		DataManager::readCellToVec(c, adjChild, 7);
		DataManager::insertCellToPage(c, curChild, -1);
		/*adjChildɾ�����һ��cell����ɾ�����һ��cellpointer�ͺ���*/
		DataManager::deleteCellInPage(adjChild, 7);
		/*����cur�ڵ�(���ڵ�)�����ڵ���Զ���ڲ��ڵ�*/
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
B+��ɾ��ʱ�п�����Ϊ��ǰ�ڵ�ĺ��ӽڵ㲻�������������ҵ��ֵܺ���
�ڵ�ϲ��������������ֵܺ��ӽڵ�͵�ǰ���ӽڵ㶼��Ҷ�ӽڵ�������
Input:
cur              ��ǰ�ڵ�
curChild         ��ǰ�ڵ�ĺ��ӽڵ�
adjChild         �ֵܺ��ӽڵ�
pointerPosOfCur  ��ǰ�ڵ��У����溢�ӽڵ�ҳ�ŵ�cell��cellpointer
�ڱ��ڵ��е�λ��
mergeToLeft   Ϊ��ʱ��ǰ���ӽڵ������ֵܺ��ӽڵ�ϲ���Ϊ��ʱ��ǰ���ӽڵ�������
			  �ܺ��ӽڵ�ϲ�
Output:
Return:
Others:
******************************************************************/
void BPlusTree::merge_leafVer(pghdr * cur, pghdr * curChild, pghdr * adjChild, int pointerPosOfCur, bool mergeToLeft)
{
	int curKey = 0;
	int curValue = 0;
	int nop = 0;
	/*����ʱ4��ָʾλ�õ�ָ��*/
	int adjChildIter_cellPointer = 0;
	int adjChildIter_cell = 0;
	int curChildIter_cellPointer = 0;
	int curChildIter_cell = 0;

	vector<unsigned char> c;
	vector<unsigned char> c_tmp;
	if (mergeToLeft)/*����ߵ�Ҷ�ӽڵ�ϲ�*/
	{
		/*��curChild������cell������adjChild,ʹ��STL�㷨*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[7] << 8 + curChild->image[7 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*����ʣ��ռ�*/
		adjChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		adjChild->image[1] = adjChild->freeSpace & 0xff00;
		adjChild->image[2] = adjChild->freeSpace & 0xff;

		/*��curChild������cellpointer������adjChild�����޸�*/
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

		/*���±�ҳ��cell������*/
		adjChild->iCellNum += curChild->iCellNum;
		adjChild->image[3] = adjChild->iCellNum & 0xff00;
		adjChild->image[4] = adjChild->iCellNum & 0xff;

		/*���µ�һ��cellʵ���ƫ����*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];

		/*����б�Ҫ�Ļ�������cur�ڵ�(���ڵ�)�����Һ���ָ��*/
		if (cur->image[pointerPosOfCur] == cur->image[7] && cur->image[pointerPosOfCur + 1] == cur->image[8])
		{
			int pgnoTmp = adjChild->pgno;
			for (int i = 0; i < 4; i++)
			{
				cur->image[10 - i] = pgnoTmp & 0xff;
				pgnoTmp = pgnoTmp >> 8;
			}
		}


		/*����cur�ڵ�(���ڵ�)�����ڵ���Զ���ڲ��ڵ�*/
		//DataManager::deleteCellInPage(cur, pointerPosOfCur);
		DataManager::readCellToVec(c_tmp, cur, pointerPosOfCur);
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c_tmp, curKey, curValue);
		DataManager::solveCell(c, curKey, nop);
		c.clear();
		DataManager::WrapToCell(c, curKey, curValue);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur);
	}
	else/*���Һϲ���Ϊ�˱�̷��㣬������ϲ���Ȼ�󿽱�cell���ҽڵ�*/
	{
		/*����ָ�룬���ô���*/
		swap(adjChild, curChild);
		/*��adjChild������cell������curChild,ʹ��STL�㷨*/
		adjChildIter_cell = adjChild->image[5] << 8 + adjChild->image[6];
		curChildIter_cell = curChild->image[7] << 8 + curChild->image[7 + 1];
		copy(curChild->image[curChildIter_cell], curChild->image[4095], adjChild->image[adjChildIter_cell - 4095 - curChildIter_cell + 1]);

		/*����ʣ��ռ�*/
		curChild->freeSpace += 4095 - curChildIter_cell + 1 + curChild->iCellNum * 2;
		curChild->image[1] = curChild->freeSpace & 0xff00;
		curChild->image[2] = curChild->freeSpace & 0xff;

		/*��adjChild������cellpointer������curChild�����޸�*/
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


		/*���±�ҳ��cell������*/
		curChild->iCellNum = adjChild->iCellNum;
		curChild->image[3] = curChild->iCellNum & 0xff00;
		curChild->image[4] = curChild->iCellNum & 0xff;

		/*���µ�һ��cellʵ���ƫ����*/
		adjChild->image[5] = adjChild->image[adjChildIter_cellPointer - 2];
		adjChild->image[6] = adjChild->image[adjChildIter_cellPointer - 1];

		/*�������ҽڵ�*/
		swap(adjChild, curChild);
		copy(curChild->image[5], adjChild->image[5], 4091);

		/*����cur�ڵ�(���ڵ�)�����ڵ���Զ���ڲ��ڵ�*/
		//DataManager::deleteCellInPage(cur, pointerPosOfCur);
		DataManager::readCellToVec(c_tmp, cur, pointerPosOfCur);
		DataManager::readCellToVec(c, adjChild, 5);
		DataManager::solveCell(c_tmp, curKey, curValue);
		DataManager::solveCell(c, curKey, nop);
		c.clear();
		DataManager::WrapToCell(c, curKey, curValue);
		DataManager::updateCellInInternalPage(c, cur, pointerPosOfCur);

	}
	/*���curChild*/
	curChild->image[0] = 0x00;
	/*TODO: ����պ��ҳ����freelist*/
}




//inline FILEP BPlusTree::GetBPlusNode()  const //�ڴ����Ϸ���һ��B+���ڵ�ռ�,add to file's tail
//{
//	fseek(Bfile, 0, SEEK_END);
//
//	return  ftell(Bfile);
//}
//
//inline void BPlusTree::ReadPageByPgNo(pghdr *r) const //��ȡaddress��ַ�ϵ�һ��B+���ڵ�  
//{
//	LockManager::pagerWaitOnLock(pPager, SHARED_LOCK);
//	mdbFile::winRead(pPager->fd, r->image, 0xfff, r->pgno * 0xfff);
//	LockManager::pagerUnlock(pPager);
//	Pager::formatPghdr(r);
//}
//
//
//inline void BPlusTree::WriteBPlusNode(const FILEP address, const BPlusNode &r) //��һ��B+���ڵ�д��address��ַ  
//{
//	fseek(Bfile, address, SEEK_SET);
//	fwrite((char*)(&r), sizeof(BPlusNode), 1, Bfile);
//}



int main()
{
	BPlusTree tree;

	tree.Build_BPlus_Tree();      //����  

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