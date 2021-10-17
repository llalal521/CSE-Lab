// chfs client.  implements FS operations using extent server
#include "chfs_client.h"
#include "extent_client.h"
#include "extent_protocol.h"
#include "gettime.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_Sym) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

bool
chfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    std::string buf;
    r = ec->get(ino, buf);
    buf.resize(size);
    r = ec->put(ino, buf);
    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    return r;
}



int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found = false;
    std::string buf = "";
    lookup(parent, name, found, ino_out);
    if(found){
        return EXIST;
    }
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    ec->create(extent_protocol::T_FILE, ino_out);
    ec->get(parent, buf);
    std::string filename_string = "";
    std::string inum_string = "";
    while(*name != '\0'){
        filename_string.push_back(*name);
        name++;
    }
    inum_string = filename(ino_out);
    filename_string.push_back('/');
    inum_string.push_back('/');
    filename_string.append(inum_string);
    buf.append(filename_string);
    //std::cout << filename_string;
    ec->put(parent, buf);
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found = false;
    std::string parent_buf = "";
    std::string inum_string = "";
    std::string filename_string = "";
    r = lookup(parent, name, found, ino_out);
    if(found){
        return EXIST;
    }
    ec->create(extent_protocol::T_DIR, ino_out);
    while(*name != '\0'){
        filename_string.push_back(*name);
        name++;
    }
    inum_string = filename(ino_out);
    ec->get(parent, parent_buf);
    filename_string.push_back('/');
    inum_string.push_back('/');
    parent_buf.append(filename_string);
    parent_buf.append(inum_string);
    ec->put(parent, parent_buf);
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::cout << name;
    std::string dir = "";
    ec->get(parent, dir);
    std::string filename = "";
    std::string judge_string = "";
    std::string inum_string = "";
    //std::cout << dir << std::endl;
    bool isname = true;
    for(size_t i = 0; i < dir.size(); ++i){
        if(isname){
            if(dir[i] == '/'){
                isname = false;
                if(!filename.compare(name)){
                    found = true;
                }
                //std::cout << filename << std::endl;
                filename.clear();
                continue;
            }
            filename.push_back(dir[i]);
        }
        else{
            if(dir[i] == '/'){
                if(found){
                    ino_out = n2i(inum_string);
                    std :: cout << "inum: " << inum_string << std::endl;
                    return EXIST;
                }
                isname = true;
                inum_string.clear();
                continue;
            }
            inum_string.push_back(dir[i]);
        }
    }
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf = "";
    ec->get(dir, buf);
    std::string filename = "";
    std::string judge_string = "";
    dirent *content_dirent = new dirent();
    std::string inum_string = "";
    bool isname = true;
    for(size_t i = 0; i < buf.size(); ++i){
        if(isname){
            if(buf[i] == '/'){
                isname = false;
                content_dirent->name = filename;
                // std::cout << filename << std::endl;
                filename.clear();
                continue;
            }
            filename.push_back(buf[i]);
        }
        else{
            if(buf[i] == '/'){
                content_dirent->inum = n2i(inum_string);
                list.push_back(*content_dirent);
                isname = true;
                inum_string.clear();
                continue;
            }
            inum_string.push_back(buf[i]);
        }
    }
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    std::string buf = "";
    r = ec->get(ino, buf);
    size_t read_count = 0;
    for(uint32_t i = 0; i < buf.size() - off; ++i){
        data.push_back(buf[i + off]);
        read_count ++;
        if(read_count == size)
            break;
    }
    /*
     * your code goes here.
     * note: read using ec->get().
     */

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{  
    int r = OK;
    std::string read_buf = "";
    std::string write_buf = "";
    extent_protocol::attr a;
    r = ec->getattr(ino, a);
    r = ec->get(ino, read_buf);
    size_t read_string_size = read_buf.size();
    if(off >= read_string_size){
        write_buf.append(read_buf);
        for(uint32_t i = read_string_size; i < off; ++i){
            write_buf.push_back('\0');
        }
        for(uint32_t i = 0; i < size; ++i){
            write_buf.push_back(*data);
            data++;
        }
    }
    else{
        for(uint32_t i = 0; i < off; ++i){
            write_buf.push_back(read_buf[i]);
        } //保存前段

        for(uint32_t i = 0; i < size; ++i){
            write_buf.push_back(*data);
            data++;
        }//写入新段

        if(off + size < read_string_size)
        for(uint32_t i = off + size; i < read_string_size; ++i){
            write_buf.push_back(read_buf[i]);
        }
    }
    bytes_written = size;
    std::cout << "size1: " << size << ' '<< off << std::endl;
    r = ec->put(ino, write_buf);  
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    std::string filename_inum = "";
    inum ino_out;
    bool found = false;
    ec->get(parent, filename_inum);
    lookup(parent, name, found, ino_out);
    if(!found)
        return OK;
    ec->remove(ino_out);
    std::string entry = std::string(name) + '/' +  filename(ino_out) + '/';
    std::string::size_type pos = filename_inum.find(entry);
    std::cout << filename_inum << pos << std::endl;
    filename_inum.replace(pos, entry.size(), "");
    ec->put(parent, filename_inum);
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    return r;
}

int chfs_client::symlink(inum parent, const char *linked_name, const char *name, inum &ino_out)
{
    int r = OK;

    std::string filename_inum = "";
    ec->get(parent, filename_inum);
    bool found = false;
    lookup(parent, name, found, ino_out);
    if(found){
        printf("symlink error\n");
        r = EXIST;
        return r;
    }

    r = ec->create(extent_protocol::T_Sym, ino_out);
    std::string new_pair = "";
    new_pair = std::string(name) + '/' + filename(ino_out) + '/';
    std::cout << "pair " << new_pair;
    filename_inum.append(new_pair);

    r = ec->put(ino_out, std::string(linked_name));
    r = ec->put(parent, filename_inum);

    return r;
}

int chfs_client::readlink(inum ino, std::string &content) //相当于读出来文件内容
{
    int r = OK;

    r = ec->get(ino, content);

    return r;
}

