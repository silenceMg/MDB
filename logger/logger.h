#include<string>
#include <mutex>         
#include<iostream>
#include <fstream>
#include<vector>
#include<stdint.h>
#include <cstdlib>
#include <utility>
//#include"int_encoding.h"
using namespace std;

const int _SEED = 13331;
const int _OF_CHECKSUM = 0;
const int _OF_SIZE = _OF_CHECKSUM + 4;
const int _OF_DATA = _OF_SIZE + 4;
const string SUFFIX_LOG = ".log";

namespace Logger
{

	class Item {
	public:
		uint32_t Checksum=0;
		uint32_t DataSize=0;
		string data="";
	};
	class logger
	{
	public:


		mutex lock;
		int64_t pos; // ��ǰ��־ָ���λ��
		int64_t	fileSize;// ���ֶ�ֻ�г�ʼ����ʱ��ᱻ����һ��, Log�������������
		uint32_t xChecksum;
		string filePath;
		string currItem;

		void OpenFile(string const& path);
		void CreateLogFile(string const& path);
		void writeLog(string const& data);
		void Rewind();              // ����־ָ���ƶ�����һ����־��λ��.
		void Close();

		uint32_t checkLogFile(); // �����־�ļ�
		shared_ptr<Item> readNextItem();



		uint32_t calChecksum(uint32_t accumulation, string const& data);
	};
}