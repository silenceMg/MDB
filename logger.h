#include<stdint.h>
#include <cstdlib>
#include "DataManager.h"
/*
journal structure:
file header: 
RandomNum   
4bytes      
record:
pgno        InitialPgNum    Image          Checksum 
4bytes      4bytes          4096bytes      4bytes     

pgno, InitialPgNum and Checksum are stored with big-end method.
*/

class Record {
public:
	unsigned int pgno;
	unsigned int initialPgNum;
	unsigned char image[4096];
	unsigned int checkSum = 0;
};

class logger
{
public:

	int64_t	fileSize;// ���ֶ�ֻ�г�ʼ����ʱ��ᱻ����һ��, Log�������������
	unsigned int randomSum;
	fstream file;
	string fPath;

	void CreateLogFile(string const& path);
	static void appendRecord(Record * r,logger* lgr);
	static int calChecksumOfRecord(int lastCheckSum, Record * r);
	int checkLogFile(); //�����־�ļ�
	int recovery();
	unsigned int genRandom();
};