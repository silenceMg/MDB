#pragma once
#include"DataManager.h"


using namespace std;
//typedef CRITICAL_SECTION mdb_mutex;
class mdbFile {
private:

public:
	unsigned char locktype;
	HANDLE h;

    //mdb_mutex mutex;
	static int winCreate(string path, mdbFile* pFile);
	static int winClose(HANDLE h);
	static int winRead(mdbFile* id, void*, int iAmt, int64_t iOfst);
	static int winWrite(mdbFile* id, const void*, int iAmt, int64_t iOfst);
	static int winTruncate(mdbFile* id, int64_t size);
	static int winSync(mdbFile* id);
	static int winFileSize(mdbFile* id, int64_t *pSize);

	static int32_t winSeekFile(mdbFile *pFile, int64_t iOffset, int type = FILE_BEGIN);

	mdbFile(string path);
	~mdbFile();
};