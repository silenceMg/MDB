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
	//dataManager���insert��update,deleteֻ��B+��ɾ���������µĽ���ߺϲ��ڵ��ʱ��Żᱻ����
	static void insertCellToPage(vector<unsigned char>& c, pghdr* pg,int posOfCellPointer);
	static void updateCellInInternalPage(vector<unsigned char>& c, pghdr* pg, int posOfCellPointer);
	static void deleteCellInPage(pghdr* pg, int posOfCellPointer);
	
	static void writeToByteArry(const void* v,unsigned char* dst,int size);
	static void writeByteArryToInt(unsigned char* v, int& dst, int size);
	static void writeCellToPage(vector<unsigned char> c,unsigned char* dst);
	void writeToLog();

	static int findTableCellPos(pghdr * pg, const string& tableName);
	static int findUpperBound(pghdr* pg,int k);//����ֵ��cellpointerpos��û�ҵ��Ļ�����-1
	void writePageBackToFile(pghdr* p);

	static void readPageFromFile(pghdr* p);
	static int readPageFromCache(pghdr* pPage);

};