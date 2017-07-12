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
	��ڵ��в���һ��cell��
Input:
	c                cell
	pg               ������Ľڵ�
	cellPointer      �����Ŀ��λ�ã����ֵ��cellPointer��λ�ã���
					 Ҫ��˲��ҵ�cell��λ�á�Ȼ����ҪŲ��cellpointer
					 ��λ�ú�cell��λ�����ڳ��ռ䡣��c���뱾����ָ��
					 ��cell֮ǰ�����cellPointer==-1�������cell����
					 �ڱ�ҳ���һ��cell֮��
Output:
Return:
Others: pg������Ҷ�ӽڵ�Ҳ�������ڲ��ڵ㡣
******************************************************************/
void DataManager::insertCellToPage(vector<unsigned char>& c, pghdr * pg, int posOfCellPointer)
{
	int i = 0;
	int cellPointerTmp = 0;
	int cellPos = (pg->image[posOfCellPointer] << 8) + pg->image[posOfCellPointer + 1];
	int begOfCellPointer = pg->isLeaf ? 7 : 11;
	
	if (posOfCellPointer == -1)/*�����ĩβ����*/
	{
		cellPointerTmp = (pg->image[5] << 8) + pg->image[6];
		cellPointerTmp -= c.size();
		pg->image[pg->iCellNum * 2 + begOfCellPointer] = cellPointerTmp & 0xff00;
		pg->image[pg->iCellNum * 2 + begOfCellPointer + 1] = cellPointerTmp & 0xff;
		DataManager::writeCellToPage(c, &(pg->image[cellPointerTmp]));
		/* �����ǰ�ڵ����ڲ��ڵ㣬��ô��Ҫ���������ӽڵ㣬Ŀǰѡ������ֵ
		** �ĸ��²�������BPTree������DataManager
		if(pg->isLeaf)
			......
		*/
	}
	else {/*��������*/
		/*Ų�����޸�cellpointers*/
		for (i = pg->iCellNum * 2; i > 0; i -= 2)
		{
			cellPointerTmp = pg->image[begOfCellPointer + i - 1] << 8 + pg->image[begOfCellPointer + i];
			cellPointerTmp -= c.size();
			pg->image[begOfCellPointer + i + 1] = cellPointerTmp & 0xff00;
			pg->image[begOfCellPointer + i + 2] = cellPointerTmp & 0xff;
		}
		/*Ų��cell*/
		cellPos = (pg->image[5] << 8) + pg->image[6];
		for (i = cellPos; i < 4096; i++)
		{
			pg->image[i - c.size()] = pg->image[i];
		}
	}
	/*����ʣ��ռ�*/
	pg->freeSpace -= c.size()-2;
	pg->image[1] = pg->freeSpace & 0xff00;
	pg->image[2] = pg->freeSpace & 0xff;
	/*���±�ҳ��cell������*/
	pg->iCellNum++;
	pg->image[3] = pg->iCellNum & 0xff00;
	pg->image[4] = pg->iCellNum & 0xff;
	/*���µ�һ��cellʵ���ƫ����*/
	cellPointerTmp = pg->image[5] << 8 + pg->image[6];
	cellPointerTmp -= c.size();
	pg->image[5] = cellPointerTmp & 0xff00;
	pg->image[6] = cellPointerTmp & 0xff;

	///*������ڲ��ڵ㣬��Ҫ����cell�Ƿ���뵽��ҳĩβ������ǵĻ���Ҫ���ı�ҳ������ӽڵ�ָ��*/
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
//B+��delete������update�ڲ��ڵ㣬���ܻᵼ���ڲ��ڵ�����������������Ƶ����ڲ��ڵ����ֻ��ʹ��4000bytes���᲻������Ǹ����ʣ�
void DataManager::updateCellInInternalPage(vector<unsigned char>& c, pghdr * pg, int posOfCellPointer)
{
	DataManager::deleteCellInPage(pg, posOfCellPointer);
	DataManager::insertCellToPage(c,pg, posOfCellPointer);
}
/******************************************************************
Function: deleteCellInPage(pghdr * pg, int posOfCellPointer)
Description:
	ɾ���ڵ��е�һ��cell��
Input:
	pg                    ��ǰ�ڵ�
	posOfCellPointer      ��ɾ����cell��cellpointer���ڵ�λ��
Output:
Return:
Others: pgָ��Ľڵ������Ҷ�ӽڵ�Ҳ�������ڲ��ڵ㡣
******************************************************************/
void DataManager::deleteCellInPage(pghdr * pg, int posOfCellPointer)
{
	int i = 0;
	int cellPointerTmp = 0;
	int cellPos = (pg->image[posOfCellPointer] << 8) + pg->image[posOfCellPointer + 1];
	int cellSize = DataManager::getVarSize(&(pg->image[cellPos]));
	/*Ų�����޸�cellpointers*/
	for (i = 2; i < pg->iCellNum * 2 + 7 - posOfCellPointer; i += 2)
	{
		cellPointerTmp = pg->image[posOfCellPointer + i] << 8 + pg->image[posOfCellPointer + i + 1];
		cellPointerTmp += cellSize;
		pg->image[posOfCellPointer + i - 2] = cellPointerTmp & 0xff00;
		pg->image[posOfCellPointer + i - 1] = cellPointerTmp & 0xff;
	}
	/*Ų��cell*/
	cellPointerTmp = (pg->image[5] << 8) + pg->image[6];
	for (i = cellPos - 1; i < cellPointerTmp; i--)
	{
		pg->image[i + cellSize] = pg->image[i];
	}

	/*����ʣ��ռ�*/
	pg->freeSpace += cellSize+2;
	pg->image[1] = pg->freeSpace & 0xff00;
	pg->image[2] = pg->freeSpace & 0xff;
	/*���±�ҳ��cell������*/
	pg->iCellNum--;
	pg->image[3] = pg->iCellNum & 0xff00;
	pg->image[4] = pg->iCellNum & 0xff;
	/*���µ�һ��cellʵ���ƫ����*/
	cellPointerTmp += cellSize;
	pg->image[5] = cellPointerTmp & 0xff00;
	pg->image[6] = cellPointerTmp & 0xff;
	
	///*������ڲ��ڵ㣬��Ҫ���ǵ�ǰɾ����cell�Ƿ���ĩβ�ģ�����ǵĻ���Ҫ���ı�ҳ�����Һ��ӽڵ�ָ��*/
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
Description:  ��vָ������ݰ��ֽ�д��dst��ָ������
Input:
v            ԭʼ����ָ��
dst          Ŀ��λ��ָ��
size         �����ķ�Χ
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

