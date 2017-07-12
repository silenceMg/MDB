#include"mdbFile.h"


using namespace std;

#define MX_CLOSE_ATTEMPT 3
//mdbFile::mdbFile(string path)
//{
//	 if (!InitializeCriticalSectionAndSpinCount(&mutex, 0x00000400) )
//        cerr<<"CriticalSection Initialization failed! Error code:"<<GetLastError()<<endl;
//	 winCreate(path);
//}
mdbFile::~mdbFile()
{
	//DeleteCriticalSection(&mutex);

}

int mdbFile::winCreate(string fname,mdbFile* pFile)
{
		pFile->h = CreateFile(TEXT(fname.c_str()), // open One.txt
			GENERIC_READ,             // open for reading
			FILE_SHARE_READ | FILE_SHARE_WRITE,                        // do not share
			NULL,                     // no security
			OPEN_ALWAYS,            // existing file only
			FILE_ATTRIBUTE_NORMAL,    // normal file
			NULL);                    // no attr. template

	return MDB_OK;
}

int mdbFile::winClose(HANDLE h) {
	int rc, cnt = 0;

	do {
		rc = CloseHandle(h);
	} while (rc == 0 && ++cnt < MX_CLOSE_ATTEMPT && (Sleep(100), 1));

	if (rc) {
		h = NULL;
	}
	return rc;
}

/*
** Read data from a file into a buffer.  Return SQLITE_OK if all
** bytes were read successfully and SQLITE_IOERR if anything goes
** wrong.
*/
int mdbFile::winRead(
	mdbFile *id,          /* File to read from */
	void *pBuf,                /* Write content into this buffer */
	int amt,                   /* Number of bytes to read */
	int64_t offset       /* Begin reading at this offset */
) {

	OVERLAPPED overlapped;          /* The offset for ReadFile. */

	mdbFile *pFile = id;  /* file handle */
	DWORD nRead;                    /* Number of bytes actually read from file */
	int nRetry = 0;                 /* Number of retrys */

	assert(id != 0);
	assert(amt > 0);
	assert(offset >= 0);
	//SimulateIOError(return SQLITE_IOERR_READ);
	/*OSTRACE(("READ pid=%lu, pFile=%p, file=%p, buffer=%p, amount=%d, "
		"offset=%lld, lock=%d\n", osGetCurrentProcessId(), pFile,
		pFile->h, pBuf, amt, offset, pFile->locktype));*/


	memset(&overlapped, 0, sizeof(OVERLAPPED));
	overlapped.Offset = (LONG)(offset & 0xffffffff);
	overlapped.OffsetHigh = (LONG)((offset >> 32) & 0x7fffffff);
	ReadFile(pFile->h, pBuf, amt, &nRead, &overlapped);
	return MDB_OK;
}

int mdbFile::winWrite(
	mdbFile *id,               /* File to write into */
	const void *pBuf,               /* The bytes to be written */
	int amt,                        /* Number of bytes to write */
	int64_t offset            /* Offset into the file to begin writing at */
) {
	int rc = 0;                     /* True if error has occurred, else false */
	mdbFile *pFile = id;  /* File handle */

	assert(amt > 0);
	assert(pFile);

	OVERLAPPED overlapped;        /* The offset for WriteFile. */

	uint8_t *aRem = (uint8_t*)pBuf;        /* Data yet to be written */
	int nRem = amt;               /* Number of bytes yet to be written */

	memset(&overlapped, 0, sizeof(OVERLAPPED));
	overlapped.Offset = (LONG)(offset & 0xffffffff);
	overlapped.OffsetHigh = (LONG)((offset >> 32) & 0x7fffffff);

	WriteFile(pFile->h, aRem, nRem, (LPDWORD)nRem, &overlapped);
	return MDB_OK;
}

/*
** Truncate an open file to a specified size
*/
int mdbFile::winTruncate(mdbFile *id, int64_t nByte) {
	mdbFile *pFile = id;  /* File handle object */
	int rc = MDB_OK;             /* Return code for this function */
	DWORD lastErrno;

	assert(pFile);


	/* SetEndOfFile() returns non-zero when successful, or zero when it fails. */
	winSeekFile(pFile, nByte);

	SetEndOfFile(pFile->h);

	return rc;
}
/*
** Make sure all writes to a particular file are committed to disk.
*/
int mdbFile::winSync(mdbFile* id)
{

	mdbFile *pFile = id;
	FlushFileBuffers(pFile->h);
	return MDB_OK;
}

int mdbFile::winFileSize(mdbFile* id, int64_t * pSize)
{

	mdbFile* pFile = id;
	DWORD upperBits;
	DWORD lowerBits;
	DWORD lastErrno;

	lowerBits = GetFileSize(pFile->h, &upperBits);
	*pSize = (((int64_t)upperBits) << 32) + lowerBits;
	return MDB_OK;
}

/*
** Move the current position of the file handle passed as the first
** argument to offset iOffset within the file. If successful, return 0.
** Otherwise, set pFile->lastErrno and return non-zero.
*/
int32_t mdbFile::winSeekFile(mdbFile *pFile, int64_t iOffset, int type) {
	LONG upperBits;                 /* Most sig. 32 bits of new offset */
	LONG lowerBits;                 /* Least sig. 32 bits of new offset */
	DWORD dwRet;                    /* Value returned by SetFilePointer() */
	DWORD lastErrno;                /* Value returned by GetLastError() */



	upperBits = (LONG)((iOffset >> 32) & 0x7fffffff);
	lowerBits = (LONG)(iOffset & 0xffffffff);

	/* API oddity: If successful, SetFilePointer() returns a dword
	** containing the lower 32-bits of the new file-offset. Or, if it fails,
	** it returns INVALID_SET_FILE_POINTER. However according to MSDN,
	** INVALID_SET_FILE_POINTER may also be a valid new offset. So to determine
	** whether an error has actually occurred, it is also necessary to call
	** GetLastError().
	*/
	if(type = FILE_BEGIN)
		dwRet = SetFilePointer(pFile->h, lowerBits, &upperBits, FILE_BEGIN);
	else if(type = FILE_END)
		dwRet = SetFilePointer(pFile->h, lowerBits, &upperBits, FILE_END);
	else 
		dwRet = SetFilePointer(pFile->h, lowerBits, &upperBits, FILE_CURRENT);


	return dwRet;
}


