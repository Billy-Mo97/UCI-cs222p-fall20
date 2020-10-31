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
        //If the file has existed.
        FILE *file;
        struct stat buffer;
        if ((stat(fileName.c_str(), &buffer) == 0)) {
            return -1;
        } else {
            file = fopen(fileName.c_str(), "w+b");//original: w+b
            if (file == NULL) {
                return -1;
            } else {
                //If the file has been successfully created,
                //initiate readPageCount, writePageCount, appendPageCount, numOfPages to 0,
                //write them to the hidden page of the file (first page).
                unsigned readNum = 0, writeNum = 0, appendNum = 0, pageNum = 0;
                fseek(file, 0, SEEK_SET);
                fwrite(&readNum, sizeof(unsigned), 1, file);
                fwrite(&writeNum, sizeof(unsigned), 1, file);
                fwrite(&appendNum, sizeof(unsigned), 1, file);
                fwrite(&pageNum, sizeof(unsigned), 1, file);
                //Write end mark into the hidden page.
                unsigned end = 0;
                fseek(file, PAGE_SIZE - sizeof(unsigned), SEEK_SET);
                fwrite(&end, sizeof(unsigned), 1, file);
                fclose(file);
                return 0;
            }
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        //Remove the file if it does exist.
        if (remove(fileName.c_str()) == 0) {
            return 0;
        } else {
            return -1;
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        struct stat buffer;
        //Check whether the file exists.
        if (stat(fileName.c_str(), &buffer) == 0) {
            //Initiate the file pointer of FileHandle instance.
            if (fileHandle.initPointer(fileName) == 0) {
                fileHandle.readPageCounter++;
                return 0;
            } else { return -1; }
        } else { return -1; }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closePointer();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        numOfPages = 0;
        pointer = NULL;
    }

    FileHandle::~FileHandle() = default;

    //Since the first page is used as hidden page to store counter values and page number,
    //when reading, writing or appending pages, we need to skip the hidden page.
    //Then pageNum value doesn't count the hidden page.
    //This principle applies to all FileHandle methods.

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum < 0 || pageNum > numOfPages) {
            return -1;
        }
        fseek(pointer, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
        int result = fread(data, 1, PAGE_SIZE, pointer);
        if (result != PAGE_SIZE) {
            return -1;
        } else {
            readPageCounter++;
            return 0;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (pageNum < 0 || pageNum > numOfPages) { return -1; }
        fseek(pointer, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
        int result = fwrite(data, 1, PAGE_SIZE, pointer);
        if (result != PAGE_SIZE) { return -1; }
        else {
            fflush(pointer);
            writePageCounter++;
            return 0;
        }
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(pointer, (numOfPages + 1) * PAGE_SIZE, SEEK_SET);
        int result = fwrite(data, 1, PAGE_SIZE, pointer);
        if (result != PAGE_SIZE) { return -1; }
        else {
            appendPageCounter++;
            numOfPages++;
            return 0;
        }
    }

    unsigned FileHandle::getNumberOfPages() {
        return numOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    RC FileHandle::initPointer(const std::string &fileName) {
        //Initiate the file pointer of FileHandle instance.
        if (pointer != NULL) { return -1; }
        else {
            pointer = fopen(fileName.c_str(), "r+");//original: r+b
            if (pointer == NULL) { return -1; }
            else {
                //Read the readPageCounter, writePageCounter, appendPageCounter and page number
                //from the hidden page (first page).
                unsigned readNum = 0, writeNum = 0, appendNum = 0, pageNum = 0;
                fseek(pointer, 0, SEEK_SET);
                fread(&readNum, sizeof(unsigned), 1, pointer);
                fread(&writeNum, sizeof(unsigned), 1, pointer);
                fread(&appendNum, sizeof(unsigned), 1, pointer);
                fread(&pageNum, sizeof(unsigned), 1, pointer);
                readPageCounter = readNum;
                writePageCounter = writeNum;
                appendPageCounter = appendNum;
                numOfPages = pageNum;
                return 0;
            }
        }
    }

    RC FileHandle::closePointer() {
        //Write the readPageCounter, writePageCounter, appendPageCounter and page number
        //into the hidden page.
        fseek(pointer, 0, SEEK_SET);
        fwrite(&readPageCounter, sizeof(unsigned), 1, pointer);
        fwrite(&writePageCounter, sizeof(unsigned), 1, pointer);
        fwrite(&appendPageCounter, sizeof(unsigned), 1, pointer);
        fwrite(&numOfPages, sizeof(unsigned), 1, pointer);
        fflush(pointer);
        if (fclose(pointer) == 0) {
            pointer = NULL;
            return 0;
        }
        else { return -1; }
    }

} // namespace PeterDB