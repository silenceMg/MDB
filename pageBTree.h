#pragma once
/**************************************************************************
File name: pageBPTree
Author: silence
Description: B+树算法模块。因为锁是数据库级别的，所以B+树的插入和删除操作开
             始时就要申请锁。
Others:
History:
    <author>   <time>   <version>   <desc>
**************************************************************************/
/*
运行前需在程序目录建立名为Bfile的文件，否则崩溃
*/
using namespace std;

#define PAGE_SIZE 4096
#define HALF_FULL_LIMIT (PAGE_SIZE-16)/2
#define FULL_LIMIT 4000
#define MAX_KEY 5   //B+树的阶,必须为大于3奇数  
#define FREESPACE_LIMIT 10


class pghdr{};

class BPlusTree
{
private:
	//FILEP ROOT;       //树根在文件内的偏移地址  
	//FILE  *Bfile;     //B+树文件的指针  
	//FILE  *Rfile;     //记录文件的指针  
	//int pgnoRoot;
	//int pgnoIndexRoot;

public:
	Pager* pPger;
	void findTable(string tbname, pghdr* pg1,int& pgnoRoot,int& pgnoIndexRoot);

	FILEP writeNewPageAtTail(Pager *pPager)  const;
	void buildNewBPTree(Pager *pPager);

	//void ReadPageByPgNo(pghdr *r) const;//?
	//void WriteBPlusNode(const FILEP, const BPlusNode&);//直接写入cache，实现模块隔离

	void Build_BPlus_Tree();


	void bpTreeInsert(int32_t k, string v);
	void bpTreeInsert_aux(pghdr* cur, const int& k, const string& v);

	void split(pghdr* father, pghdr* current, const int cellPointerOfPartition, const int cellPointerOfCurInFather);
	void split_internalVer(pghdr* father, pghdr* current, const int cellPointerOfPartition, const int cellPointerOfCurInFather);
	void split_leafVer(pghdr* father, pghdr* current, const int cellPointerOfPartition, const int cellPointerOfCurInFather);
	void bpTreeSearchByIndex(int key, string & str);
	int bpTreeSearch_aux_internalVer(int key,int& nextPgno);
	int bpTreeSearch_aux_leafVer(int key, string& str);

	void bpTreeDelete(TRecord&);
	void bpTreeDelete_aux(pghdr* current, const int& key, vector<unsigned char> & cell);

	//void EnumLeafKey();
	void borrow(pghdr* cur, pghdr* curChild, pghdr* adjChild, int pointerPosOfCur, bool borrowFromLeft);
	void merge(pghdr* cur, pghdr* curChild, pghdr* adjChild, int pointerPosOfCur, bool mergeToLeft);
	void borrow_internalVer(pghdr* cur, pghdr* curChild, pghdr* adjChild, int pointerPosOfCur, bool borrowFromLeft);
	void merge_internalVer(pghdr* cur, pghdr* curChild, pghdr* adjChild, int pointerPosOfCur, bool mergeToLeft);
	void borrow_leafVer(pghdr* cur, pghdr* curChild, pghdr* adjChild, int pointerPosOfCur, bool borrowFromLeft);
	void merge_leafVer(pghdr* cur, pghdr* curChild, pghdr* adjChild, int pointerPosOfCur, bool mergeToLeft);
	void upper_bound(int& i, int key, pghdr* pg);

	BPlusTree();
	~BPlusTree();

};
