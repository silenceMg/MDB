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
		int64_t pos; // 当前日志指针的位置
		int64_t	fileSize;// 该字段只有初始化的时候会被更新一次, Log操作不会更新它
		uint32_t xChecksum;
		string filePath;
		string currItem;

		void OpenFile(string const& path);
		void CreateLogFile(string const& path);
		void writeLog(string const& data);
		void Rewind();              // 将日志指针移动到第一条日志的位置.
		void Close();

		uint32_t checkLogFile(); // 检查日志文件
		shared_ptr<Item> readNextItem();



		uint32_t calChecksum(uint32_t accumulation, string const& data);
	};
}