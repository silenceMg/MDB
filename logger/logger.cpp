#include"logger.h"
namespace Logger
{
	void logger::OpenFile(string const& path)
	{
		filePath = path;

		//检查文件是否存在
		fstream file;
		file.open(path + SUFFIX_LOG, ios::in | ios::out | ios::binary);
		if (!file)
		{
			cerr << "No file!" << endl;
			exit(EXIT_FAILURE);
		}

		//检查文件大小是否正确
		int begin = file.tellg();
		file.seekg(0, ios::end);
		int end = file.tellg();
		if ((end - begin) < 4)
		{
			cerr << "Size error!" << endl;
			exit(EXIT_FAILURE);
		}

		//初始化logger类中的fileSize和xChecksum
		fileSize = end - begin;
		file.seekp(0, ios::beg);
		file.read((char*)(&xChecksum), 4);//读取文件中存储的xChecksum
		file.close();

		//对log文件大小进行核查
		Rewind();
		uint32_t  xChecksum_tmp = checkLogFile();
		Rewind();
		//文件大小与xChecksum记录的大小不一致，删除文件
		if (xChecksum != xChecksum_tmp)
		{

			cerr << "xChecksum error!" << endl << "Bad log file!" << endl;
			//是否Truncate？ 是的，Truncate！
			// TODO
			//由于更新xCheckSum的时候数据库发生崩溃, 则会导致整个log文件不能使用.
			//所以暂时放弃xCheckSum, 之后将xCheckSum改为由booter管理.
			fstream file(path + SUFFIX_LOG, ios::trunc);
			if (file.is_open())
			{
				file.close();
			}
			exit(EXIT_FAILURE);
		}
	}

	void logger::CreateLogFile(string const& path)
	{

		fstream file(path + SUFFIX_LOG, ios::in);
		if (file.is_open())
		{
			cerr << "File already exist!" << endl;
			//exit(EXIT_FAILURE);
		}


		file.open(path + SUFFIX_LOG, ios::app | ios::out | ios::binary);//创建，打开，清除文件
		//生成xChecksum并写入
		xChecksum = 0;
		file.write((char*)(&xChecksum), 4);  //写入xChecksum
		file.close();
		pos = 4;
	}

	void logger::writeLog(string const& data)
	{
		lock.lock();
		fstream file(filePath + SUFFIX_LOG,  ios::out|ios::in | ios::binary);
		if (file.is_open())
		{
			//写入checksum
			uint32_t checksum = calChecksum(0, data);
			file.seekp(0, ios::end);
			file.write((char*)(&checksum), 4);

			//写入size
			uint32_t size = data.size();
			file.write((char*)(&size), 4);

			//写入data
			file.write(data.c_str(), sizeof(char) * size);
			
			//更新xChecksum
			uint32_t xChecksum_tmp;
			file.seekp(0, ios::beg);
			file.read((char*)&xChecksum_tmp, 4);
			xChecksum_tmp=calChecksum(xChecksum_tmp,data);
			file.seekg(0, ios::beg);
			file.write((char*)&xChecksum_tmp, 4);
			

			file.close();
		}

		lock.unlock();
	}

	void logger::Rewind()
	{
		pos = 4;
	}

	void logger::Close()
	{
		filePath = "";
		// TODO
		//只用一个filestream，这里用来关闭？
	}

	uint32_t logger::checkLogFile()//逐条检查log文件
	{
		uint32_t xChecksum_tmp = 0;
		int cnt = 1;
		while (pos != fileSize)
		{
			shared_ptr<Item> Item;
			Item = move(readNextItem());

			uint32_t checksum_tmp= calChecksum(0, Item->data);//计算checksum

			if (checksum_tmp != Item->Checksum)
			{
				cerr << "Checksum error!" << endl << "Error Item:" << cnt << endl;
				exit(EXIT_FAILURE);
			}
			pos += int64_t(_OF_DATA + Item->DataSize);
			xChecksum_tmp = calChecksum(xChecksum_tmp, Item->data);
			cnt++;
		}

		return xChecksum_tmp;
	}

	shared_ptr<Item> logger::readNextItem()
	{
		shared_ptr<Item> Item_tmp(new Item);

		fstream file_tmp(filePath + SUFFIX_LOG,ios::in|ios::binary);
		lock.lock();
		if (file_tmp.is_open())
		{
			file_tmp.seekp(pos, ios::beg);
			file_tmp.read((char*)(&(Item_tmp->Checksum)), 4);
			file_tmp.read((char*)(&(Item_tmp->DataSize)), 4);
			file_tmp.read((char*)Item_tmp->data.c_str(), (int)(Item_tmp->DataSize));
			file_tmp.close();
		}

		lock.unlock();

		return Item_tmp;
	}

	uint32_t logger::calChecksum(uint32_t accumulation, string const& data)
	{
		for (int cnt = 0; cnt < sizeof(data.c_str())-1; cnt++)
		{
			accumulation = accumulation*_SEED + uint32_t(data.c_str()[cnt]);  //取出data的大小，以此来计算Checksum
		}
		return accumulation;
	}
}