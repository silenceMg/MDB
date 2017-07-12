#pragma once
#include"Pager.h"

using namespace std;
#define FIRST_CELL_POINTER 7




struct Var {
	unsigned char v[5];
	unsigned char* beg;
	int size;
};


class DataManager {
private:
	fstream file;

public:
	static void Int32ToVar(uint32_t& i, Var& var);
	static uint32_t VarToInt32(Var& var);
	static uint32_t VarToInt32(unsigned char* v);
	static uint32_t VarToInt32(vector<unsigned char>::iterator& c);

	static void getTextType(string &s, uint32_t &tp);
	static void getTextType(Var &v, uint32_t &tp);

	static int getVarSize(unsigned char* v);
	//static int getCellPosFromCellPointer(unsigned char* cPointer);

	static void varCpyToCell(Var& var, vector<unsigned char>& c);
	static void WrapToCell(vector<unsigned char>& cell, uint32_t key, string value);
	static void WrapToCell(vector<unsigned char>& cell, uint32_t key, uint32_t value);

	static int readCellToVec(vector<unsigned char>& cell,pghdr* pg, int posOfCellPointer);
	static void solveCell(vector<unsigned char>& cell, int& key, string& value);
	static void solveCell(vector<unsigned char>& cell, int& key, int& value);

	//static void writeCellToPage(vector<unsigned char>& c, pghdr& pg);
	//dataManager里的insert，update,delete只有B+树删除操作导致的借或者合并节点的时候才会被调用
	static void insertCellToPage(vector<unsigned char>& c, pghdr* pg,int posOfCellPointer);
	static void updateCellInInternalPage(vector<unsigned char>& c, pghdr* pg, int posOfCellPointer);
	static void deleteCellInPage(pghdr* pg, int posOfCellPointer);
	
	static void writeToByteArry(const void* v,unsigned char* dst,int size);
	static void writeByteArryToInt(unsigned char* v, int& dst, int size);
	static void writeCellToPage(vector<unsigned char> c,unsigned char* dst);
	void writeToLog();

	static int findTableCellPos(pghdr * pg, const string& tableName);
	static int findUpperBound(pghdr* pg,int k);//返回值是cellpointerpos，没找到的话返回-1
	void writePageBackToFile(pghdr* p);

	static void readPageFromFile(pghdr* p);
	static int readPageFromCache(pghdr* pPage);

};