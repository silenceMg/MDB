#include"logger.h"
namespace Logger
{
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

		fstream file(path + SUFFIX_LOG, ios::in);
		if (file.is_open())
		{
			cerr << "File already exist!" << endl;
			//exit(EXIT_FAILURE);
		}


		file.open(path + SUFFIX_LOG, ios::app | ios::out | ios::binary);//�������򿪣�����ļ�
		//����xChecksum��д��
		xChecksum = 0;
		file.write((char*)(&xChecksum), 4);  //д��xChecksum
		file.close();
		pos = 4;
	}

	void logger::writeLog(string const& data)
	{
		lock.lock();
		fstream file(filePath + SUFFIX_LOG,  ios::out|ios::in | ios::binary);
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
		//ֻ��һ��filestream�����������رգ�
	}

	uint32_t logger::checkLogFile()//�������log�ļ�
	{
		uint32_t xChecksum_tmp = 0;
		int cnt = 1;
		while (pos != fileSize)
		{
			shared_ptr<Item> Item;
			Item = move(readNextItem());

			uint32_t checksum_tmp= calChecksum(0, Item->data);//����checksum

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
			accumulation = accumulation*_SEED + uint32_t(data.c_str()[cnt]);  //ȡ��data�Ĵ�С���Դ�������Checksum
		}
		return accumulation;
	}
}