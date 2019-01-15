#ifndef YDHDFS2_H
#define YDHDFS2_H

#include "hdfs.h"
#include "mpi.h"
#include <string.h> //memcpy, memchr
#include <stdlib.h> //realloc
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>
#include "global.h"
using namespace std;

#define HDFS_BUF_SIZE 65536
#define LINE_DEFAULT_SIZE 4096
#define HDFS_BLOCK_SIZE 8388608 //8M

const char* newLine = "\n";

//====== get File System ======

hdfsFS getHdfsFS()
{
	hdfsBuilder * bld = hdfsNewBuilder();
	hdfsBuilderSetNameNode(bld, "master");
	hdfsBuilderSetNameNodePort(bld, 9000);
	hdfsFS fs = hdfsBuilderConnect(bld);
	if(!fs) {
		fprintf(stderr, "Failed to connect to HDFS!\n");
		MPI_Abort(MPI_COMM_WORLD, 1);
	}
	return fs;
}

hdfsFS getlocalFS()
{
    hdfsBuilder * bld = hdfsNewBuilder();
	hdfsBuilderSetNameNode(bld, NULL);
	hdfsFS lfs = hdfsBuilderConnect(bld);
    if (!lfs) {
        fprintf(stderr, "Failed to connect to 'local' FS!\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    return lfs;
}

//====== get File Handle ======

hdfsFile getRHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_RDONLY | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s for reading!\n", path);
        exit(-1);
    }
    return hdl;
}

hdfsFile getWHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_WRONLY | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s for writing!\n", path);
        exit(-1);
    }
    return hdl;
}

hdfsFile getRWHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_RDWR | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s!\n", path);
        exit(-1);
    }
    return hdl;
}

//====== Read line ======

//logic:
//buf[] is for batch reading from HDFS file
//line[] is a line buffer, the string length is "length", the buffer size is "size"
//after each readLine(), need to check eof(), if it's true, no line is read due to EOF
struct LineReader {
    //static fields
    char buf[HDFS_BUF_SIZE];
    tSize bufPos;
    tSize bufSize;
    hdfsFS fs;
    hdfsFile handle;
    bool fileEnd;

    //dynamic fields
    char* line;
    int length;
    int size;

    LineReader(hdfsFS& fs, hdfsFile& handle)
        : bufPos(0)
        , length(0)
        , size(LINE_DEFAULT_SIZE)
    {
        this->fs = fs;
        this->handle = handle;
        fileEnd = false;
        fill();
        line = (char*)malloc(LINE_DEFAULT_SIZE * sizeof(char));
    }

    ~LineReader()
    {
        free(line);
    }

    //internal use only!
    void doubleLineBuf()
    {
        size *= 2;
        line = (char*)realloc(line, size * sizeof(char));
    }

    //internal use only!
    void lineAppend(const char* first, int num)
    {
        while (length + num + 1 > size)
            doubleLineBuf();
        memcpy(line + length, first, num);
        length += num;
    }

    //internal use only!
    void fill()
    {
        bufSize = hdfsRead(fs, handle, buf, HDFS_BUF_SIZE);
        if (bufSize == -1) {
            fprintf(stderr, "Read Failure!\n");
            exit(-1);
        }
        bufPos = 0;
        if (bufSize < HDFS_BUF_SIZE)
            fileEnd = true;
    }
	
	bool eof()
    {
        return length == 0 && fileEnd;
    }

    //user interface
    //the line starts at "line", with "length" chars
    void appendLine()
    {
        if (bufPos == bufSize)
            return;
        char* pch = (char*)memchr(buf + bufPos, '\n', bufSize - bufPos);
        if (pch == NULL) {
            lineAppend(buf + bufPos, bufSize - bufPos);
            bufPos = bufSize;
            if (!fileEnd)
                fill();
            else
                return;
            pch = (char*)memchr(buf, '\n', bufSize);
            while (pch == NULL) {
                lineAppend(buf, bufSize);
                if (!fileEnd)
                    fill();
                else
                    return;
                pch = (char*)memchr(buf, '\n', bufSize);
            }
        }
        int validLen = pch - buf - bufPos;
        lineAppend(buf + bufPos, validLen);
        bufPos += validLen + 1; //+1 to skip '\n'
        if (bufPos == bufSize) {
            if (!fileEnd)
                fill();
            else
                return;
        }
    }
	
	void readLine()
    {
        length = 0;
        appendLine();
    }
	
	/* //deprecated
	//Add by Hongzhi Chen, revised by Da Yan
    void readLines(int num)
    {
    	readLine();
		if(eof()) return; //no more line to read, eof() should be kept true
		lineAppend(newLine, 1);
		for(int i=1; i<num-1; i++)
		{
			appendLine();
			lineAppend(newLine, 1);
		}
		if(num > 1) appendLine();
    }
	*/

    char* getLine()
    {
        line[length] = '\0';
        return line;
    }
};

//====== Dir Creation ======
void dirCreate(const char* outdir)
{
    hdfsFS fs = getHdfsFS();
    int created = hdfsCreateDirectory(fs, outdir);
	if (created == -1) {
		fprintf(stderr, "Failed to create folder %s!\n", outdir);
		exit(-1);
	}
    hdfsDisconnect(fs);
}

//====== Dir Check ======
int outDirCheck(const char* outdir, bool print, bool force) //returns -1 if fail, 0 if succeed
{
    hdfsFS fs = getHdfsFS();
    if (hdfsExists(fs, outdir) == 0) {
        if (force) {
            if (hdfsDelete(fs, outdir, 1) == -1) {
                if (print)
                    fprintf(stderr, "Error deleting %s!\n", outdir);
                exit(-1);
            }
            int created = hdfsCreateDirectory(fs, outdir);
            if (created == -1) {
                if (print)
                    fprintf(stderr, "Failed to create folder %s!\n", outdir);
                exit(-1);
            }
        } else {
            if (print)
                fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
            hdfsDisconnect(fs);
            return -1;
        }
    } else {
        int created = hdfsCreateDirectory(fs, outdir);
        if (created == -1) {
            if (print)
                fprintf(stderr, "Failed to create folder %s!\n", outdir);
            exit(-1);
        }
    }
    hdfsDisconnect(fs);
    return 0;
}

//====== Dir Check ======
int dirCheck(const char* indir, const char* outdir, bool print, bool force) //returns -1 if fail, 0 if succeed
{
    hdfsFS fs = getHdfsFS();
    if (hdfsExists(fs, indir) != 0) {
        if (print)
            fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
        hdfsDisconnect(fs);
        return -1;
    }
    if (hdfsExists(fs, outdir) == 0) {
        if (force) {
            if (hdfsDelete(fs, outdir, 1) == -1) {
                if (print)
                    fprintf(stderr, "Error deleting %s!\n", outdir);
                exit(-1);
            }
            int created = hdfsCreateDirectory(fs, outdir);
            if (created == -1) {
                if (print)
                    fprintf(stderr, "Failed to create folder %s!\n", outdir);
                exit(-1);
            }
        } else {
            if (print)
                fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
            hdfsDisconnect(fs);
            return -1;
        }
    } else {
        int created = hdfsCreateDirectory(fs, outdir);
        if (created == -1) {
            if (print)
                fprintf(stderr, "Failed to create folder %s!\n", outdir);
            exit(-1);
        }
    }
    hdfsDisconnect(fs);
    return 0;
}

int dirCheck(vector<string> indirs, const char* outdir, bool print, bool force) //returns -1 if fail, 0 if succeed
{
    hdfsFS fs = getHdfsFS();
    for (int i = 0; i < indirs.size(); i++) {
        const char* indir = indirs[i].c_str();
        if (hdfsExists(fs, indir) != 0) {
            if (print)
                fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
            hdfsDisconnect(fs);
            return -1;
        }
    }
    if (hdfsExists(fs, outdir) == 0) {
        if (force) {
            if (hdfsDelete(fs, outdir, 1) == -1) {
                if (print)
                    fprintf(stderr, "Error deleting %s!\n", outdir);
                exit(-1);
            }
            int created = hdfsCreateDirectory(fs, outdir);
            if (created == -1) {
                if (print)
                    fprintf(stderr, "Failed to create folder %s!\n", outdir);
                exit(-1);
            }
        } else {
            if (print)
                fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
            hdfsDisconnect(fs);
            return -1;
        }
    } else {
        int created = hdfsCreateDirectory(fs, outdir);
        if (created == -1) {
            if (print)
                fprintf(stderr, "Failed to create folder %s!\n", outdir);
            exit(-1);
        }
    }
    hdfsDisconnect(fs);
    return 0;
}

int dirCheck(const char* indir, vector<string> outdirs, bool print, bool force) //returns -1 if fail, 0 if succeed
{
	hdfsFS fs = getHdfsFS();
	if (hdfsExists(fs, indir) != 0) {
		if (print)
			fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
		hdfsDisconnect(fs);
		return -1;
	}
    for(int i = 0; i < outdirs.size(); i++)
    {
    	const char * outdir = outdirs[i].c_str();
    	if (hdfsExists(fs, outdir) == 0) {
			if (force) {
				if (hdfsDelete(fs, outdir, 1) == -1) {
					if (print)
						fprintf(stderr, "Error deleting %s!\n", outdir);
					exit(-1);
				}
				int created = hdfsCreateDirectory(fs, outdir);
				if (created == -1) {
					if (print)
						fprintf(stderr, "Failed to create folder %s!\n", outdir);
					exit(-1);
				}
			} else {
				if (print)
					fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
				hdfsDisconnect(fs);
				return -1;
			}
		} else {
			int created = hdfsCreateDirectory(fs, outdir);
			if (created == -1) {
				if (print)
					fprintf(stderr, "Failed to create folder %s!\n", outdir);
				exit(-1);
			}
		}
    }
    hdfsDisconnect(fs);
    return 0;
}

int dirCheck(const char* outdir, bool force) //returns -1 if fail, 0 if succeed
{
    hdfsFS fs = getHdfsFS();
    if (hdfsExists(fs, outdir) == 0) {
        if (force) {
            if (hdfsDelete(fs, outdir, 1) == -1) {
                fprintf(stderr, "Error deleting %s!\n", outdir);
                exit(-1);
            }
            int created = hdfsCreateDirectory(fs, outdir);
            if (created == -1) {
                fprintf(stderr, "Failed to create folder %s!\n", outdir);
                exit(-1);
            }
        } else {
            fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
            hdfsDisconnect(fs);
            return -1;
        }
    } else {
        int created = hdfsCreateDirectory(fs, outdir);
        if (created == -1) {
            fprintf(stderr, "Failed to create folder %s!\n", outdir);
            exit(-1);
        }
    }
    hdfsDisconnect(fs);
    return 0;
}

int dirCheck(const char* indir) //returns -1 if fail, 0 if succeed
{
    hdfsFS fs = getHdfsFS();
    if (hdfsExists(fs, indir) != 0) {
        fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
        hdfsDisconnect(fs);
        return -1;
    }
    else
    {
    	hdfsDisconnect(fs);
    	return 0;
    }
}

//====== Write line ======

struct LineWriter {
    hdfsFS fs;
    const char* path;
    int me; //-1 if there's no concept of machines (like: hadoop fs -put)
    int nxtPart;
    int curSize;

    hdfsFile curHdl;

    LineWriter(const char* path, hdfsFS fs, int me)
        : nxtPart(0)
        , curSize(0)
    {
        this->path = path;
        this->fs = fs;
        this->me = me;
        curHdl = NULL;
        //===============================
        //if(overwrite==true) readDirForce();
        //else readDirCheck();
        //===============================
        //1. cannot use above, otherwise multiple dir check/delete will be done during parallel writing
        //2. before calling the constructor, make sure "path" does not exist
        nextHdl();
    }

    ~LineWriter()
    {
        if (hdfsFlush(fs, curHdl)) {
            fprintf(stderr, "Failed to 'flush' %s\n", path);
            exit(-1);
        }
        hdfsCloseFile(fs, curHdl);
    }

    /*//================== not for parallel writing =====================
    //internal use only!
    void readDirCheck()
{
    	if(hdfsExists(fs, path)==0)
    	{
    		fprintf(stderr, "%s already exists!\n", path);
    		exit(-1);
    	}
    	int created=hdfsCreateDirectory(fs, path);
    	if(created==-1)
    	{
    		fprintf(stderr, "Failed to create folder %s!\n", path);
    		exit(-1);
    	}
}

    //internal use only!
    void readDirForce()
{
    	if(hdfsExists(fs, path)==0)
    	{
    		if(hdfsDelete(fs, path)==-1)
    		{
    			fprintf(stderr, "Error deleting %s!\n", path);
    			exit(-1);
    		}
    	}
    	int created=hdfsCreateDirectory(fs, path);
    	if(created==-1)
    	{
    		fprintf(stderr, "Failed to create folder %s!\n", path);
    		exit(-1);
    	}
}
    */ //================== not for parallel writing =====================

    //internal use only!
    void nextHdl()
    {
        //set fileName
        char fname[20];
        strcpy(fname, "part_");
        char buffer[10];
        if (me >= 0) {
            sprintf(buffer, "%d", me);
            strcat(fname, buffer);
            strcat(fname, "_");
        }
        sprintf(buffer, "%d", nxtPart);
        strcat(fname, buffer);
        //flush old file
        if (nxtPart > 0) {
            if (hdfsFlush(fs, curHdl)) {
                fprintf(stderr, "Failed to 'flush' %s\n", path);
                exit(-1);
            }
            hdfsCloseFile(fs, curHdl);
        }
        //open new file
        nxtPart++;
        curSize = 0;
        char* filePath = new char[strlen(path) + strlen(fname) + 2];
        strcpy(filePath, path);
        strcat(filePath, "/");
        strcat(filePath, fname);
        curHdl = getWHandle(filePath, fs);
        delete[] filePath;
    }

    void writeLine(char* line, int num)
    {
        if (curSize + num + 1 > HDFS_BLOCK_SIZE) //+1 because of '\n'
        {
            nextHdl();
        }
        tSize numWritten = hdfsWrite(fs, curHdl, line, num);
        if (numWritten == -1) {
            fprintf(stderr, "Failed to write file!\n");
            exit(-1);
        }
        curSize += numWritten;
        numWritten = hdfsWrite(fs, curHdl, newLine, 1);
        if (numWritten == -1) {
            fprintf(stderr, "Failed to create a new line!\n");
            exit(-1);
        }
        curSize += 1;
    }
};

//====== Put: local->HDFS ======

void put(const char* localpath, const char* hdfspath)
{
    if (dirCheck(hdfspath, false) == -1)
        return;
    hdfsFS fs = getHdfsFS();
    hdfsFS lfs = getlocalFS();

    hdfsFile in = getRHandle(localpath, lfs);
    LineReader* reader = new LineReader(lfs, in);
    LineWriter* writer = new LineWriter(hdfspath, fs, -1);
    while (true) {
        reader->readLine();
        if (!reader->eof()) {
            writer->writeLine(reader->line, reader->length);
        } else
            break;
    }
    hdfsCloseFile(lfs, in);
    delete reader;
    delete writer;

    hdfsDisconnect(lfs);
    hdfsDisconnect(fs);
}

/* //deprecated
//Add by Hongzhi Chen, revised by Da Yan
void put(int lines_per_item, const char* localpath, const char* hdfspath)
{
    if (dirCheck(hdfspath, false) == -1)
        return;
    hdfsFS fs = getHdfsFS();
    hdfsFS lfs = getlocalFS();

    hdfsFile in = getRHandle(localpath, lfs);
    LineReader* reader = new LineReader(lfs, in);
    LineWriter* writer = new LineWriter(hdfspath, fs, -1);
    while (true) {
        //reader->readLine();
        reader->readLines(lines_per_item);
        if (!reader->eof()) {
            writer->writeLine(reader->line, reader->length);
        } else
            break;
    }
    hdfsCloseFile(lfs, in);
    delete reader;
    delete writer;

    hdfsDisconnect(lfs);
    hdfsDisconnect(fs);
}
*/

void putFASTQ(const char* localpath, const char* hdfspath)
{
    if (dirCheck(hdfspath, false) == -1)
        return;
    hdfsFS fs = getHdfsFS();
    hdfsFS lfs = getlocalFS();

    hdfsFile in = getRHandle(localpath, lfs);
    LineReader* reader = new LineReader(lfs, in);
    LineWriter* writer = new LineWriter(hdfspath, fs, -1);
    while (true) {
        reader->readLine();
		if (reader->eof()) break;
		reader->readLine();
		writer->writeLine(reader->line, reader->length);
		reader->readLine();
		reader->readLine();
    }
    hdfsCloseFile(lfs, in);
    delete reader;
    delete writer;

    hdfsDisconnect(lfs);
    hdfsDisconnect(fs);
}

void putf(const char* localpath, const char* hdfspath) //force put, overwrites target
{
    dirCheck(hdfspath, true);
    hdfsFS fs = getHdfsFS();
    hdfsFS lfs = getlocalFS();

    hdfsFile in = getRHandle(localpath, lfs);
    LineReader* reader = new LineReader(lfs, in);
    LineWriter* writer = new LineWriter(hdfspath, fs, -1);
    while (true) {
        reader->readLine();
        if (!reader->eof()) {
            writer->writeLine(reader->line, reader->length);
        } else
            break;
    }
    hdfsCloseFile(lfs, in);
    delete reader;
    delete writer;

    hdfsDisconnect(lfs);
    hdfsDisconnect(fs);
}

//====== Put: all local files under dir -> HDFS ======

void putDir(const char* localpath, const char* hdfspath)
{
    if (dirCheck(hdfspath, false) == -1)
        return;
    hdfsFS fs = getHdfsFS();
    hdfsFS lfs = getlocalFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(lfs, localpath, &numFiles);
	if (fileinfo == NULL) {
		fprintf(stderr, "Failed to list directory %s!\n", localpath);
		exit(-1);
	}
	//------
	LineWriter* writer = new LineWriter(hdfspath, fs, -1);
	for (int i = 0; i < numFiles; i++) {
		if (fileinfo[i].mKind == kObjectKindFile) {
			cout<<"Putting file: "<<fileinfo[i].mName<<endl;
			hdfsFile in = getRHandle(fileinfo[i].mName, lfs);
			LineReader* reader = new LineReader(lfs, in);
			while (true) {
				reader->readLine();
				if (!reader->eof()) {
					writer->writeLine(reader->line, reader->length);
				} else
					break;
			}
			hdfsCloseFile(lfs, in);
			delete reader;
		}
	}
    //------
    hdfsFreeFileInfo(fileinfo, numFiles);
    delete writer;
    hdfsDisconnect(lfs);
    hdfsDisconnect(fs);
}

//====== BufferedWriter ======
struct BufferedWriter {
    hdfsFS fs;
    const char* path;
    int me; //-1 if there's no concept of machines (like: hadoop fs -put)
    int nxtPart;
    vector<char> buf;
    hdfsFile curHdl;

    BufferedWriter(const char* path, hdfsFS fs)
    {
        this->path = path;
        this->fs = fs;
        this->me = -1;
        this->curHdl = getWHandle(this->path, fs);
    }
    BufferedWriter(const char* path, hdfsFS fs, int me)
        : nxtPart(0)
    {
        this->path = path;
        this->fs = fs;
        this->me = me;
        curHdl = NULL;
        nextHdl();
    }

    ~BufferedWriter()
    {
        tSize numWritten = hdfsWrite(fs, curHdl, &buf[0], buf.size());
        if (numWritten == -1) {
            fprintf(stderr, "Failed to write file!\n");
            exit(-1);
        }
        buf.clear();

        if (hdfsFlush(fs, curHdl)) {
            fprintf(stderr, "Failed to 'flush' %s\n", path);
            exit(-1);
        }
        hdfsCloseFile(fs, curHdl);
    }

    //internal use only!
    void nextHdl()
    {
        //set fileName
        char fname[20];

        if (me >= 0) {
            sprintf(fname, "part_%d_%d", me, nxtPart);
        } else {
            sprintf(fname, "part_%d", nxtPart);
        }

        //flush old file
        if (nxtPart > 0) {
            if (hdfsFlush(fs, curHdl)) {
                fprintf(stderr, "Failed to 'flush' %s\n", path);
                exit(-1);
            }
            hdfsCloseFile(fs, curHdl);
        }
        //open new file
        nxtPart++;
        char* filePath = new char[strlen(path) + strlen(fname) + 2];
        sprintf(filePath, "%s/%s", path, fname);
        curHdl = getWHandle(filePath, fs);
        delete[] filePath;
    }

    void check()
    {
        if (buf.size() >= HDFS_BLOCK_SIZE) {
            tSize numWritten = hdfsWrite(fs, curHdl, &buf[0], buf.size());
            if (numWritten == -1) {
                fprintf(stderr, "Failed to write file!\n");
                exit(-1);
            }
            buf.clear();
            if (me != -1) // -1 means "output in the specified file only"
            {
                nextHdl();
            }
        }
    }

    void write(const char* content)
    {
        int len = strlen(content);
        buf.insert(buf.end(), content, content + len);
    }
};

//====== Dispatcher ======

struct sizedFName {
    char* fname;
    tOffset size;

    bool operator<(const sizedFName& o) const
    {
        return size > o.size; //large file goes first
    }
};

struct sizedFString {
    string fname;
    tOffset size;

    bool operator<(const sizedFString& o) const
    {
        return size > o.size; //large file goes first
    }
};

const char* rfind(const char* str, char delim)
{
    int len = strlen(str);
    int pos = 0;
    for (int i = len - 1; i >= 0; i--) {
        if (str[i] == delim) {
            pos = i;
            break;
        }
    }
    return str + pos;
}

vector<string>* dispatchRan(const char* inDir, int numSlaves) //remember to "delete[] assignment" after used
{ //locality is not considered for simplicity
    vector<string>* assignment = new vector<string>[numSlaves];
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[numSlaves];
    for (int i = 0; i < numSlaves; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
        }
    }
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < numSlaves; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    hdfsDisconnect(fs);
    return assignment;
}

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<string>* dispatchLocality(const char* inDir, int numSlaves) //remember to "delete[] assignment" after used
{ //considers locality
    vector<string>* assignment = new vector<string>[numSlaves];
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[numSlaves];
    for (int i = 0; i < numSlaves; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    int avg = 0;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
            avg += fileinfo[i].mSize;
        }
    }
    avg /= numSlaves;
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    vector<sizedFName> recycler;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        istringstream ss(rfind(it->fname, '/'));
        string cur;
        getline(ss, cur, '_');
        getline(ss, cur, '_');
        int slaveOfFile = atoi(cur.c_str());
        if (assigned[slaveOfFile] + it->size <= avg) {
            assignment[slaveOfFile].push_back(it->fname);
            assigned[slaveOfFile] += it->size;
        } else
            recycler.push_back(*it);
    }
    for (it = recycler.begin(); it != recycler.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < numSlaves; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    hdfsDisconnect(fs);
    return assignment;
}

vector<vector<string> >* dispatchRan(const char* inDir) //remember to delete assignment after used
{ //locality is not considered for simplicity
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(_num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
        }
    }
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    hdfsDisconnect(fs);
    return assignmentPointer;
}

vector<vector<string> >* dispatchRan(vector<string> inDirs) //remember to delete assignment after used
{ //locality is not considered for simplicity
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(_num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    vector<sizedFString> sizedfile;
    for (int pos = 0; pos < inDirs.size(); pos++) {
        const char* inDir = inDirs[pos].c_str();
        int numFiles;
        hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
        if (fileinfo == NULL) {
            fprintf(stderr, "Failed to list directory %s!\n", inDir);
            exit(-1);
        }
        for (int i = 0; i < numFiles; i++) {
            if (fileinfo[i].mKind == kObjectKindFile) {
                sizedFString cur = { fileinfo[i].mName, fileinfo[i].mSize };
                sizedfile.push_back(cur);
            }
        }
        hdfsFreeFileInfo(fileinfo, numFiles);
    }
    hdfsDisconnect(fs);
    //sort files by size
    sort(sizedfile.begin(), sizedfile.end());
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //allocate files to slaves
    vector<sizedFString>::iterator it;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    return assignmentPointer;
}

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<vector<string> >* dispatchLocality(const char* inDir) //remember to delete assignment after used
{ //considers locality
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(_num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    int avg = 0;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
            avg += fileinfo[i].mSize;
        }
    }
    avg /= _num_workers;
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    vector<sizedFName> recycler;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        istringstream ss(rfind(it->fname, '/'));
        string cur;
        getline(ss, cur, '_');
        getline(ss, cur, '_');
        int slaveOfFile = atoi(cur.c_str());
        if (assigned[slaveOfFile] + it->size <= avg) {
            assignment[slaveOfFile].push_back(it->fname);
            assigned[slaveOfFile] += it->size;
        } else
            recycler.push_back(*it);
    }
    for (it = recycler.begin(); it != recycler.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    hdfsDisconnect(fs);
    return assignmentPointer;
}

vector<vector<string> >* dispatchLocality(vector<string> inDirs) //remember to delete assignment after used
{ //considers locality
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(_num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    vector<sizedFString> sizedfile;
    int avg = 0;
    for (int pos = 0; pos < inDirs.size(); pos++) {
        const char* inDir = inDirs[pos].c_str();
        int numFiles;
        hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
        if (fileinfo == NULL) {
            fprintf(stderr, "Failed to list directory %s!\n", inDir);
            exit(-1);
        }
        for (int i = 0; i < numFiles; i++) {
            if (fileinfo[i].mKind == kObjectKindFile) {
                sizedFString cur = { fileinfo[i].mName, fileinfo[i].mSize };
                sizedfile.push_back(cur);
                avg += fileinfo[i].mSize;
            }
        }
        hdfsFreeFileInfo(fileinfo, numFiles);
    }
    hdfsDisconnect(fs);
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //sort files by size
    avg /= _num_workers;
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFString>::iterator it;
    vector<sizedFString> recycler;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        istringstream ss(rfind(it->fname.c_str(), '/'));
        string cur;
        getline(ss, cur, '_');
        getline(ss, cur, '_');
        int slaveOfFile = atoi(cur.c_str());
        if (assigned[slaveOfFile] + it->size <= avg) {
            assignment[slaveOfFile].push_back(it->fname);
            assigned[slaveOfFile] += it->size;
        } else
            recycler.push_back(*it);
    }
    for (it = recycler.begin(); it != recycler.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    return assignmentPointer;
}

void reportAssignment(vector<string>* assignment, int numSlaves)
{
    for (int i = 0; i < numSlaves; i++) {
        cout << "====== Rank " << i << " ======" << endl;
        vector<string>::iterator it;
        for (it = assignment[i].begin(); it != assignment[i].end(); ++it) {
            cout << *it << endl;
        }
    }
}

void reportAssignment(vector<vector<string> >* assignment)
{
    for (int i = 0; i < _num_workers; i++) {
        cout << "====== Rank " << i << " ======" << endl;
        vector<string>::iterator it;
        for (it = (*assignment)[i].begin(); it != (*assignment)[i].end(); ++it) {
            cout << *it << endl;
        }
    }
}

void hdfsFullyRead(hdfsFS & fs, hdfsFile & rhdl, char* buffer, tSize length)
{
	tSize lenRead = 0;
	while(lenRead < length)
	{
		int numRead = hdfsRead(fs, rhdl, buffer + lenRead, length - lenRead);
		if (numRead == -1) {
			fprintf(stderr, "%d: Failed to read CP-file!\n", _my_rank);
			exit(-1);
		}
		lenRead += numRead;
	}
}

#endif
