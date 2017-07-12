#include"logger.h"

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

	fstream file(path, ios::in);
	if (file.is_open())
	{
		cerr << "Journal already exists!" << endl;
		return;
	}

	file.open(path, ios::out | ios::binary);//创建文件
	//生成RandomSum并写入
	randomSum = genRandom();
	file.write((char*)(&randomSum), 4);  //写入xChecksum
	file.close();
}

void logger::appendRecord(Record * r, logger * lgr)
{
	lgr->file.open(lgr->fPath, ios::out | ios::binary);
	lgr->file.write((char*)&(r->pgno),4);
	lgr->file.write((char*)&(r->initialPgNum), 4);
	lgr->file.write((char*)&(r->image), 4096);
	lgr->file.write((char*)&(r->checkSum), 4);
	lgr->file.close();
}

void logger::writeLog(string const& data)
{
	lock.lock();
	fstream file(filePath + SUFFIX_LOG, ios::out | ios::in | ios::binary);
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
		xChecksum_tmp = calChecksum(xChecksum_tmp, data);
		file.seekg(0, ios::beg);
		file.write((char*)&xChecksum_tmp, 4);


		file.close();
	}

	lock.unlock();
}

void logger::Close()
{
	filePath = "";
	// TODO
	//只用一个filestream，这里用来关闭？
}

int logger::calChecksumOfRecord(int lastCheckSum, Record * r)
{
	unsigned int* aData = (unsigned int*)r->image;
	r->checkSum = r->pgno + r->initialPgNum;
	for (int i = 0; i < 4096; i += 4)
	{
		r->checkSum = lastCheckSum + *aData++;
	}
	return MDB_OK;
}

int logger::checkLogFile()
{
	char buffer[4];
	memset(buffer, 0, 4 * sizeof(unsigned char));
	int checkSum = randomSum;
	file.seekp(4, ios::beg);
	for (int i = 4; i < fileSize; i += 4)
	{
		file.read((char*)&buffer, 4);
		if (i % 4108 == 0)
		{
			if (checkSum!= *((unsigned int*)buffer))
			{
				cerr << "Journal is corrupted!\n";
				exit(1);
			}
		}
		else
		{
			checkSum += *((unsigned int*)buffer);
		}
	}

	return MDB_OK;
}

int logger::recovery()
{

	return 0;
}


unsigned int logger::genRandom()
{
	default_random_engine generator(time(NULL));
	uniform_int_distribution<unsigned int> dis(0, 0xffffffff);
	auto dice = std::bind(dis, generator);
	return dice();
}

