#include"DataManager.h"

void DataManager::Int32ToVar(uint32_t& i, Var& var)
{
	unsigned char* ptr = var.v;
	static const int B = 128;
	if (i < (1 << 7))
	{
		*(ptr++) = i;
		var.size = 1;
	}
	else if (i < (1 << 14))
	{
		*(ptr++) = i | B;
		*(ptr++) = i >> 7;
		var.size = 2;
	}
	else if (i < (1 << 21))
	{
		*(ptr++) = i | B;
		*(ptr++) = (i >> 7) | B;
		*(ptr++) = i >> 14;
		var.size = 3;
	}
	else if (i < (1 << 28))
	{
		*(ptr++) = i | B;
		*(ptr++) = (i >> 7) | B;
		*(ptr++) = (i >> 14) | B;
		*(ptr++) = i >> 21;
		var.size = 4;
	}
	else
	{
		*(ptr++) = i | B;
		*(ptr++) = (i >> 7) | B;
		*(ptr++) = (i >> 14) | B;
		*(ptr++) = (i >> 21) | B;
		*(ptr++) = i >> 28;
		var.size = 5;
	}

	var.beg = ptr - 1;

}

uint32_t DataManager::VarToInt32(Var& var)
{
	unsigned char* pTmp = var.beg;
	uint32_t Res = 0;

	while (((*pTmp) & 128) == 128)
	{
		Res = Res << 7;
		*pTmp = *pTmp ^ 128;
		Res = Res | (*pTmp);
		pTmp--;
	}

	Res = Res << 7;
	Res = Res | *pTmp;
	return Res;
}

uint32_t DataManager::VarToInt32(unsigned char* v)
{

	uint32_t Res = 0;

	while (((*v) & 128) == 128)
	{
		Res = Res << 7;
		*v = *v ^ 128;
		Res = Res | (*v);
		v--;
	}

	Res = Res << 7;
	Res = Res | *v;
	return Res;
}

uint32_t DataManager::VarToInt32(vector<unsigned char>::iterator& c)
{
	auto pTmp = c;
	uint32_t Res = 0;

	while (((*pTmp) & 128) == 128)
	{
		Res = Res << 7;
		*pTmp = *pTmp ^ 128;
		Res = Res | (*pTmp);
		pTmp--;
	}

	Res = Res << 7;
	Res = Res | *pTmp;
	return Res;
}

void DataManager::getTextType(string &s, uint32_t &tp)
{
	tp = s.size() * 2 + 13;
}

void DataManager::getTextType(Var &v, uint32_t &tp)
{
	tp = v.size;
}

int DataManager::getVarSize(unsigned char* v)
{
	unsigned char* pTmp = v;
	int s = 0;
	while ((*pTmp) & 128 == 128)
	{
		s++;
		pTmp--;
	}
	s++;
	return s;
}

void DataManager::varCpyToCell(Var& var, vector<unsigned char>& c)
{
	for (int i = var.size - 1; i >= 0; i--)
	{
		c.insert(c.begin(), var.v[i]);
	}

}

void DataManager::WrapToCell(vector<unsigned char>& cell, uint32_t key, string value)
{
	//data
	for (auto &it : value)
	{
		cell.push_back(it);
	}
	//data type
	Var v;
	uint32_t type = 0;
	getTextType(value, type);
	Int32ToVar(type, v);
	varCpyToCell(v, cell);
	//payload header
	uint32_t dataSize = 0;
	uint32_t payloadHeader = v.size;
	Var vPayloadHeader;
	Int32ToVar(payloadHeader, vPayloadHeader);
	payloadHeader = payloadHeader + vPayloadHeader.size;
	Int32ToVar(payloadHeader, v);
	varCpyToCell(v, cell);
	dataSize = cell.size();
	//key size
	Int32ToVar(key, v);
	varCpyToCell(v, cell);
	//data size
	Int32ToVar(dataSize, v);
	varCpyToCell(v, cell);
}
void DataManager::WrapToCell(vector<unsigned char>& cell, uint32_t key, uint32_t value)
{
	Var v;
	uint32_t type = 0;
	//data
	Int32ToVar(value, v);
	varCpyToCell(v, cell);
	//data type
	getTextType(v, type);
	Int32ToVar(type, v);
	varCpyToCell(v, cell);
	//payload header
	uint32_t dataSize = 0;
	uint32_t payloadHeader = v.size;
	Var vPayloadHeader;
	Int32ToVar(payloadHeader, vPayloadHeader);
	payloadHeader = payloadHeader + vPayloadHeader.size;
	Int32ToVar(payloadHeader, v);
	varCpyToCell(v, cell);
	dataSize = cell.size();
	//key size
	Int32ToVar(key, v);
	varCpyToCell(v, cell);
	//data size
	Int32ToVar(dataSize, v);
	varCpyToCell(v, cell);
}
int DataManager::readCellToVec(vector<unsigned char>& cell, pghdr* pg, int posOfCellPointer)
{
	short cellPos = 0;
	cellPos = (pg->image[posOfCellPointer] << 8) + pg->image[posOfCellPointer + 1];
	int tmp = getVarSize(&(pg->image[cellPos]));

	for (int i = 0; i < tmp; i++)
	{
		cell.push_back(pg->image[cellPos]);
		cellPos++;
	}
	return MDB_OK;
}
void DataManager::solveCell(vector<unsigned char>& cell, int& key, string& value)
{
	auto pTmp = cell.begin();

	//data size
	if ((*(pTmp + 1) & 128) == 128)
	{
		pTmp++;
		for (; (*pTmp) & 128 == 128; pTmp++);
		pTmp--;
	}
	uint32_t dataSize = DataManager::VarToInt32(pTmp);
	pTmp++;

	//key size
	if ((*(pTmp + 1) & 128) == 128)
	{
		pTmp++;
		for (; (*pTmp) & 128 == 128; pTmp++);
		pTmp--;
	}
	key = VarToInt32(pTmp);
	pTmp++;

	//payload header size
	if ((*(pTmp + 1) & 128) == 128)
	{
		pTmp++;
		for (; (*pTmp) & 128 == 128; pTmp++);
		pTmp--;
	}
	uint32_t payloadHeader = VarToInt32(pTmp);

	//data type
	pTmp = pTmp + payloadHeader - 1;
	uint32_t type = VarToInt32(pTmp);
	type = (type - 13) / 2;

	//data value
	pTmp++;
	for (int i = 0; i < type; i++)
	{
		value += (char)*pTmp;
		pTmp++;
	}

}

void DataManager::solveCell(vector<unsigned char>& cell, int & key, int & value)
{
}

//void DataManager::writeCellToPage(vector<unsigned char>& c, pghdr& pg)
//{
//	pg.nRef++;
//	int nPtrPos = FIRST_CELL_POINTER + 2 * pg.iCellNum;
//	int i = pg.ptrOfFirstCell - c.size();
//	for (auto &it : c)
//	{
//		pg.image[i] = it;
//		i++;
//	}
//
//	pg.dirty = true;
//	pg.needSync = true;
//	pg.nRef--;
//
//}
/******************************************************************
Function: insertCellToPage(vector<unsigned char>& c, pghdr * pg, int cellPointer)
Description:
	向节点中插入一个cell。
Input:
	c                cell
	pg               待插入的节点
	cellPointer      插入的目标位置，这个值是cellPointer的位置，需
					 要借此查找到cell的位置。然后需要挪动cellpointer
					 的位置和cell的位置以腾出空间。将c插入本参数指定
					 的cell之前。如果cellPointer==-1，插入的cell附加
					 在本页最后一个cell之后。
Output:
Return:
Others: pg可能是叶子节点也可能是内部节点。
******************************************************************/
void DataManager::insertCellToPage(vector<unsigned char>& c, pghdr * pg, int posOfCellPointer)
{
	int i = 0;
	int cellPointerTmp = 0;
	int cellPos = (pg->image[posOfCellPointer] << 8) + pg->image[posOfCellPointer + 1];
	int begOfCellPointer = pg->isLeaf ? 7 : 11;
	
	if (posOfCellPointer == -1)/*如果在末尾插入*/
	{
		cellPointerTmp = (pg->image[5] << 8) + pg->image[6];
		cellPointerTmp -= c.size();
		pg->image[pg->iCellNum * 2 + begOfCellPointer] = cellPointerTmp & 0xff00;
		pg->image[pg->iCellNum * 2 + begOfCellPointer + 1] = cellPointerTmp & 0xff;
		DataManager::writeCellToPage(c, &(pg->image[cellPointerTmp]));
		/* 如果当前节点是内部节点，那么需要更新最右子节点，目前选择把这个值
		** 的更新操作交给BPTree而不是DataManager
		if(pg->isLeaf)
			......
		*/
	}
	else {/*正常插入*/
		/*挪动并修改cellpointers*/
		for (i = pg->iCellNum * 2; i > 0; i -= 2)
		{
			cellPointerTmp = pg->image[begOfCellPointer + i - 1] << 8 + pg->image[begOfCellPointer + i];
			cellPointerTmp -= c.size();
			pg->image[begOfCellPointer + i + 1] = cellPointerTmp & 0xff00;
			pg->image[begOfCellPointer + i + 2] = cellPointerTmp & 0xff;
		}
		/*挪动cell*/
		cellPos = (pg->image[5] << 8) + pg->image[6];
		for (i = cellPos; i < 4096; i++)
		{
			pg->image[i - c.size()] = pg->image[i];
		}
	}
	/*更新剩余空间*/
	pg->freeSpace -= c.size()-2;
	pg->image[1] = pg->freeSpace & 0xff00;
	pg->image[2] = pg->freeSpace & 0xff;
	/*更新本页中cell的数量*/
	pg->iCellNum++;
	pg->image[3] = pg->iCellNum & 0xff00;
	pg->image[4] = pg->iCellNum & 0xff;
	/*更新第一个cell实体的偏移量*/
	cellPointerTmp = pg->image[5] << 8 + pg->image[6];
	cellPointerTmp -= c.size();
	pg->image[5] = cellPointerTmp & 0xff00;
	pg->image[6] = cellPointerTmp & 0xff;

	///*如果是内部节点，需要考虑cell是否插入到本页末尾，如果是的话需要更改本页的最后孩子节点指针*/
	//if (!pg->isLeaf && pg->image[posOfCellPointer] == pg->image[5] && pg->image[posOfCellPointer + 1] == pg->image[6])
	//{
	//	vector<unsigned char> c;
	//	DataManager::readCellToVec(c, pg, 5);
	//	int k, v;
	//	DataManager::solveCell(c, k, v);
	//	for (int i = 0; i < 4; i++)
	//	{
	//		pg->image[10 - i] = v & 0xff;
	//		v = v >> 8;
	//	}
	//}
}
//B+树delete过程中update内部节点，可能会导致内部节点内容溢出，但是限制的了内部节点最大只能使用4000bytes，会不会溢出是个疑问？
void DataManager::updateCellInInternalPage(vector<unsigned char>& c, pghdr * pg, int posOfCellPointer)
{
	DataManager::deleteCellInPage(pg, posOfCellPointer);
	DataManager::insertCellToPage(c,pg, posOfCellPointer);
}
/******************************************************************
Function: deleteCellInPage(pghdr * pg, int posOfCellPointer)
Description:
	删除节点中的一个cell。
Input:
	pg                    当前节点
	posOfCellPointer      待删除的cell的cellpointer所在的位置
Output:
Return:
Others: pg指向的节点可能是叶子节点也可能是内部节点。
******************************************************************/
void DataManager::deleteCellInPage(pghdr * pg, int posOfCellPointer)
{
	int i = 0;
	int cellPointerTmp = 0;
	int cellPos = (pg->image[posOfCellPointer] << 8) + pg->image[posOfCellPointer + 1];
	int cellSize = DataManager::getVarSize(&(pg->image[cellPos]));
	/*挪动并修改cellpointers*/
	for (i = 2; i < pg->iCellNum * 2 + 7 - posOfCellPointer; i += 2)
	{
		cellPointerTmp = pg->image[posOfCellPointer + i] << 8 + pg->image[posOfCellPointer + i + 1];
		cellPointerTmp += cellSize;
		pg->image[posOfCellPointer + i - 2] = cellPointerTmp & 0xff00;
		pg->image[posOfCellPointer + i - 1] = cellPointerTmp & 0xff;
	}
	/*挪动cell*/
	cellPointerTmp = (pg->image[5] << 8) + pg->image[6];
	for (i = cellPos - 1; i < cellPointerTmp; i--)
	{
		pg->image[i + cellSize] = pg->image[i];
	}

	/*更新剩余空间*/
	pg->freeSpace += cellSize+2;
	pg->image[1] = pg->freeSpace & 0xff00;
	pg->image[2] = pg->freeSpace & 0xff;
	/*更新本页中cell的数量*/
	pg->iCellNum--;
	pg->image[3] = pg->iCellNum & 0xff00;
	pg->image[4] = pg->iCellNum & 0xff;
	/*更新第一个cell实体的偏移量*/
	cellPointerTmp += cellSize;
	pg->image[5] = cellPointerTmp & 0xff00;
	pg->image[6] = cellPointerTmp & 0xff;
	
	///*如果是内部节点，需要考虑当前删除的cell是否是末尾的，如果是的话需要更改本页的最右孩子节点指针*/
	//if (!pg->isLeaf && pg->image[posOfCellPointer] == pg->image[5] && pg->image[posOfCellPointer + 1] == pg->image[6])
	//{
	//	vector<unsigned char> c;
	//	DataManager::readCellToVec(c, pg, 5);
	//	int k, v;
	//	DataManager::solveCell(c, k, v);
	//	for (int i = 0; i < 4; i++)
	//	{
	//		pg->image[10 - i] = v & 0xff;
	//		v = v >> 8;
	//	}
	//}
}
/******************************************************************
Function: writeToByteArry
Description:  把v指向的数据按字节写入dst所指的数组
Input:
v            原始数据指针
dst          目的位置指针
size         拷贝的范围
Output:
Return:
Others:
******************************************************************/
void DataManager::writeToByteArry(const void * v, unsigned char * dst, int size)
{
	unsigned char* t;
	t = (unsigned char*)v;
	for (int i = 0; i < size; i++)
	{
		*dst = *t;
		dst++;
		t++;
	}
}

void DataManager::writeByteArryToInt(unsigned char * v,  int & dst, int size)
{
	for (int i = size - 1; i >= 0; i--)
	{
		dst |= (*v << (i*8));
		v++;
	}
}

void DataManager::writeCellToPage(vector<unsigned char> c, unsigned char * dst)
{
	for (auto &i : c)
	{
		*dst = i;
		dst++;
	}
}

void DataManager::writeToLog() {

}

int DataManager::findTableCellPos(pghdr * pg, const string& tableName)
{
	return 0;
}

void DataManager::writePageBackToFile(pghdr* p)
{

	if (!p->dirty) return;
	if (p->needSync) writeToLog();
	//file.seekg((p->pgno) * 4096);
	//file.write((char*)p->image, 4096);

}

void DataManager::readPageFromFile(pghdr* p)
{
	file.seekg((p->pgno) * 4096);
	//file.read((char*)p->image, 4096);

}

int DataManager::readPageFromCache(pghdr * pPage)
{
	return 0;
}


