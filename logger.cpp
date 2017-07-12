#include"logger.h"

void logger::OpenFile(string const& path)
{
	filePath = path;

	//����ļ��Ƿ����
	fstream file;
	file.open(path + SUFFIX_LOG, ios::in | ios::out | ios::binary);
	if (!file)
	{
		cerr << "No file!" << endl;
		exit(EXIT_FAILURE);
	}

	//����ļ���С�Ƿ���ȷ
	int begin = file.tellg();
	file.seekg(0, ios::end);
	int end = file.tellg();
	if ((end - begin) < 4)
	{
		cerr << "Size error!" << endl;
		exit(EXIT_FAILURE);
	}

	//��ʼ��logger���е�fileSize��xChecksum
	fileSize = end - begin;
	file.seekp(0, ios::beg);
	file.read((char*)(&xChecksum), 4);//��ȡ�ļ��д洢��xChecksum
	file.close();

	//��log�ļ���С���к˲�
	Rewind();
	uint32_t  xChecksum_tmp = checkLogFile();
	Rewind();
	//�ļ���С��xChecksum��¼�Ĵ�С��һ�£�ɾ���ļ�
	if (xChecksum != xChecksum_tmp)
	{

		cerr << "xChecksum error!" << endl << "Bad log file!" << endl;
		//�Ƿ�Truncate�� �ǵģ�Truncate��
		// TODO
		//���ڸ���xCheckSum��ʱ�����ݿⷢ������, ��ᵼ������log�ļ�����ʹ��.
		//������ʱ����xCheckSum, ֮��xCheckSum��Ϊ��booter����.
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

	file.open(path, ios::out | ios::binary);//�����ļ�
	//����RandomSum��д��
	randomSum = genRandom();
	file.write((char*)(&randomSum), 4);  //д��xChecksum
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
		//д��checksum
		uint32_t checksum = calChecksum(0, data);
		file.seekp(0, ios::end);
		file.write((char*)(&checksum), 4);

		//д��size
		uint32_t size = data.size();
		file.write((char*)(&size), 4);

		//д��data
		file.write(data.c_str(), sizeof(char) * size);

		//����xChecksum
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
	//ֻ��һ��filestream�����������رգ�
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
