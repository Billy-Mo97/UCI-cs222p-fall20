#include "src/include/pfm.h"
#include <string>
#include <sys/stat.h>
#include <stdio.h>
#include <iostream>
namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        struct stat buffer;
        if(stat (fileName.c_str(), &buffer) == 0){
            return -1;
        }else{
            FILE* file;
            file=fopen(fileName.c_str(),"w+b");
            if(file==NULL){
                return -1;
            }else{
                fclose(file);
                return 0;
            }
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(remove(fileName.c_str())==0){
            return 0;
        }else{
            return -1;
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        struct stat buffer;
        if(stat (fileName.c_str(), &buffer) == 0){
            if(fileHandle.initPointer(fileName)==0){
                fileHandle.readPageCounter = 0;
                fileHandle.writePageCounter = 0;
                fileHandle.appendPageCounter = 0;
                return 0;
            }else{return -1;}
        }else{return -1;}
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closePointer();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        numOfPages=0;
        pointer=NULL;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if(pageNum<0|| pageNum>numOfPages){return -1;}
        fseek(pointer,pageNum*PAGE_SIZE,SEEK_SET);
        int result=fread(data,1,PAGE_SIZE,pointer);
        if(result!=PAGE_SIZE){return -1;}
        else{
            readPageCounter++;
            return 0;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if(pageNum<0|| pageNum>numOfPages){return -1;}
        fseek(pointer,pageNum*PAGE_SIZE,SEEK_SET);
        int result=fwrite(data,1,PAGE_SIZE,pointer);
        if(result!=PAGE_SIZE){return -1;}
        else{
            writePageCounter++;
            return 0;
        }
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(pointer,0,SEEK_END);
        int result=fwrite(data,1,PAGE_SIZE,pointer);
        if(result!=PAGE_SIZE){return -1;}
        else{
            appendPageCounter++;
            numOfPages++;
            return 0;
        }
    }

    unsigned FileHandle::getNumberOfPages() {
        return numOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount=readPageCount;
        writePageCount=writePageCount;
        appendPageCount=appendPageCount;
        return 0;
    }

    RC FileHandle::initPointer(const std::string &fileName) {
        if(pointer!=NULL){return -1;}
        else{
            pointer=fopen(fileName.c_str(),"w+b");
            if(pointer==NULL){return -1;}
            else{
                numOfPages=ftell(pointer)/PAGE_SIZE;
                return 0;
            }
        }
    }

    RC FileHandle::closePointer() {
        if(fclose(pointer)==0){return 0;}
        else{return -1;}
    }

} // namespace PeterDB