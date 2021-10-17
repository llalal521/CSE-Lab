#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf, int size)
{
  unsigned char *block = blocks[id];
  uint32_t block_cursor = 0;
  while(block_cursor < size){
  *buf = *block;
  block++;
  buf++;
  block_cursor++;
  }
}


void
disk::write_block(blockid_t id, const char *buf, int size)
{
  unsigned char *block = blocks[id];
  uint32_t block_cursor = 0;
  while(block_cursor < size){
    *block = *buf;
    block ++;
    buf ++;
    block_cursor ++;
  }
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  char buf[BLOCK_SIZE];
  char mask;
  
  /* Find the first empty bit in bitmap and alloc this block */
  blockid_t id;
  uint32_t offset;
  uint32_t num;
  /* Check every bitmap */
  for (id = 0; id < BLOCK_NUM; id += BPB) {
    read_block(BBLOCK(id), buf);
    /* Check every bit in bitmap */
    for (offset = 0; offset < BPB; offset++) {
      mask = 1 << (offset % 8);
      /* Find the empty bit (0) */
      num = buf[offset/8];
      if ((num & mask) == 0) {
        buf[offset/8] = num | mask;
        write_block(BBLOCK(id), buf, BLOCK_SIZE);
        return id + offset;
      }
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  char buf[BLOCK_SIZE];
  read_block(BBLOCK(id), buf);

  uint32_t offset = id % BPB;
  uint32_t mask = ~(1 << (offset % 8));
  buf[offset/8] = buf[offset/8] & mask;
  write_block(BBLOCK(id), buf), BLOCK_SIZE;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  alloc_block();

  /* Alloc super block */ 
  alloc_block();

  /* Alloc bitmap */ 
  uint32_t i;
  for (i = 0; i < BLOCK_NUM / BPB; i++) {
    alloc_block();
  }

  /* Alloc inode table */ 
  for (i = 0; i < INODE_NUM / IPB; i++) {
    alloc_block();
  }
}

void
block_manager::read_block(uint32_t id, char *buf, int size)
{
  d->read_block(id, buf, size);
}

void
block_manager::write_block(uint32_t id, const char *buf, int size)
{
  d->write_block(id, buf, size);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
  // bm->write_block(10, (char *) busy_list, BLOCK_SIZE);
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  char buf[BLOCK_SIZE];

  uint32_t inodeNum;
  /* Check every inodeBlock */
  for (inodeNum = 1; inodeNum < bm->sb.ninodes; inodeNum++) {
    if ((inodeNum - 1) % IPB == 0) {
      bm->read_block(IBLOCK(inodeNum, bm->sb.nblocks), buf, BLOCK_SIZE);
    } 
    /* Check every inode in block */
    inode_t *inode = (inode_t *)buf + (inodeNum - 1) % IPB;
    /* Find an empty inode */
    if (inode->type == 0) {
      inode->type = type;
      bm->write_block(IBLOCK(inodeNum, bm->sb.nblocks), buf, BLOCK_SIZE);
      return inodeNum;
    }
  }
  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *inode = get_inode(inum);

  /* Check if the inode is already a freed one */
  if (inode->type == 0) {
    return;
  } else {
    memset(inode, 0, sizeof(inode_t));
    put_inode(inum, inode);
    free(inode);
  }

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE] = {'\0'};

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE] = {'\0'};
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf, BLOCK_SIZE);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return allocted data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t *inode_i = get_inode(inum);
  blockid_t *block_num = inode_i->blocks;
  int tmp_size = 0;
  int record = 0;
  int *Array = NULL;
  std::string truncate_tail = "";
  bool truncate_symbol = false;
  std::string truncate_result = "";
  std::cout << "here" << std::endl;
  for(uint32_t i = 0; i <= NDIRECT; ++i){
    if(i < NDIRECT && block_num[i] != 0){
      if(inum < 18)
        printf("%d ", block_num[i]);
      record++;
      }
    if(i == NDIRECT && block_num[i] != 0){ //deal with indirect inode
      char buf[BLOCK_SIZE] = {'\0'};
      bm->read_block(block_num[i], buf); //get the int array
      Array = (int *)buf;
      printf("\n block%d \n", block_num[i]);
      for(uint32_t j = 0; j < NINDIRECT; ++j){
        if(Array[j] != 0){
          printf("%d ", Array[j]);
          record++;
        }
      }
    }
  }
  std::cout << "record " << record;
  char *result = new char[record * BLOCK_SIZE];
  *buf_out = result;
  for(uint32_t i = 0; i <= NDIRECT; ++i){
    if(i < NDIRECT && block_num[i] != 0){
      char *tmp = result + i * BLOCK_SIZE;
      bm->read_block(block_num[i], tmp);
      if(block_num[i+1] != 0){
        tmp_size = tmp_size + BLOCK_SIZE;
      }
      else 
      for(uint32_t i = 0; i < BLOCK_SIZE; ++i){
        if(i == BLOCK_SIZE - 1){
          if(*tmp != '\0')
            tmp_size ++;
          else
            continue;
        }
        else{
          if(*tmp != '\0'){
            tmp ++;
            tmp_size++;
            truncate_symbol = false;
            if(*tmp == '\0')
              truncate_symbol = true;
          }
          else{
            if(truncate_symbol){
              truncate_tail.push_back(*tmp);
              if(*(tmp+1) != '\0'){
                truncate_symbol = false;
                truncate_result.append(truncate_tail);
                truncate_tail.clear();
              }
              tmp ++;
            }
          }
        }
      }
    }
    if(i == NDIRECT && block_num[i] != 0){ //deal with indirect inode
      for(uint32_t j = 0; j < NINDIRECT; ++j){
        if(Array[j] != 0){
          char *tmp = result + (100+j) * BLOCK_SIZE;
          bm->read_block(Array[j], tmp);
          if(j != NINDIRECT && Array[j+1] != 0)
            tmp_size = tmp_size + BLOCK_SIZE;
          else  
          for(uint32_t i = 0; i < BLOCK_SIZE; ++i){
            if(i == BLOCK_SIZE - 1){
              if(*tmp != '\0')
                tmp_size ++;
              else
                continue;
            }
            else{
              if(*tmp != '\0'){
                tmp ++;
                tmp_size++;
                truncate_symbol = false;
                if(*tmp == '\0')
                  truncate_symbol = true;
              }
              else{
                if(truncate_symbol){
                  truncate_tail.push_back(*tmp);
                  if(*(tmp+1) != '\0'){
                    truncate_symbol = false;
                    truncate_result.append(truncate_tail);
                    truncate_tail.clear();
                  }
                  tmp ++;
                }
              }
            }
          }
        }
      }
    }
  }
  printf("\n");
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  inode_i->ctime = ts.tv_sec;
  inode_i->atime = ts.tv_sec;
  put_inode(inum, inode_i);
  delete inode_i;
  *size = tmp_size + truncate_result.size();
  std::cout << "size " << *size;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */

  if(size > MAXFILE * BLOCK_SIZE){
    printf("file is too large!");
    return;
  }
  inode_t *inode_i = get_inode(inum);
  if(inode_i == NULL)
    return;
  blockid_t *block_num = inode_i->blocks;
  uint32_t num = size / BLOCK_SIZE + 1;
  uint32_t current_num = inode_i->size / BLOCK_SIZE + 1;
  std::cout << size << ' ' << inode_i->size << std::endl;
  int *Array = NULL;
  char char_array[BLOCK_SIZE] = {'\0'};
  bool type = false;
  // if(block_num[100] != 0){
  //   bm->read_block(block_num[100], char_array);
  //   Array = (int *) char_array;
  // }
  blockid_t last_id = 0;
  char indirect_block[BLOCK_SIZE] = {'\0'};
  if(size < inode_i->size){
    for(uint32_t i = num - 1; i <= current_num - 1; ++i){
      if(i < 100){
        blockid_t tmp = block_num[i];
        bm->free_block(tmp);
        if(i != num - 1)
          block_num[i] = 0;
        else  block_num[i] = bm->alloc_block();
      } else{
        if(i == 100 || i == num - 1){
          bm->read_block(block_num[100], char_array);
          Array = (int *) char_array;
        }
        if(i == 100 && i != num - 1){ 
          type = true;
        }
        bm->free_block(Array[i - 100]);
        if(i != num - 1){
          ((blockid_t *)char_array)[i - 100] = 0;
          Array[i - 100] = 0;
        }
        else{
          Array[i - 100] = bm->alloc_block();
          ((blockid_t *)char_array)[i - 100] = Array[i - 100];
        }
        if(type){
          if(block_num[100] != 0)
            bm->free_block(block_num[100]);
          block_num[100] = 0;
        }
      }
    }
    if(num >= 100)
      bm->write_block(block_num[100], char_array, BLOCK_SIZE);
  } else{
    for(uint32_t i = current_num - 1; i <= num - 1; ++i){
      if(i < 100){
        if(block_num[i] == 0){
          block_num[i] = bm->alloc_block();
          std::cout << block_num[i] ;
        }
      }
      else{
        if(i == 100){
          if(block_num[100] == 0){
            block_num[100] = bm->alloc_block();
          }
          bm->read_block(block_num[100], char_array);
          Array = (int *) char_array;
        }
        if(i == 100 || i == current_num - 1){
          bm->read_block(block_num[100], char_array);
          Array = (int *) char_array;
        }
        if(Array[i - 100] == 0){
          Array[i - 100] = bm->alloc_block();
          ((blockid_t *) char_array)[i - 100] = Array[i - 100];
        }
      }
    }
    if(num > 100)
      bm->write_block(block_num[100], char_array, BLOCK_SIZE);
  } 
  for(uint32_t i = 0; i < num; ++i){
    if(i < 100){
      if(i == num - 1){
        bm->write_block(block_num[i], buf, size - i * BLOCK_SIZE);
      }
      else{
        bm->write_block(block_num[i], buf, BLOCK_SIZE);
        buf = buf + BLOCK_SIZE;
      }
    }
    else{
      if(i == num - 1){
        bm->write_block(Array[i - 100], buf, size - i * BLOCK_SIZE);
      }
      else{
        bm->write_block(Array[i - 100], buf, BLOCK_SIZE);
        buf = buf + BLOCK_SIZE;
      }
    }
  }
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  inode_i->ctime = ts.tv_sec;
  inode_i->mtime = ts.tv_sec;
  inode_i->size = size;
  put_inode(inum, inode_i);
  delete inode_i;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *inode = get_inode(inum);
  if(inode == NULL){
    a.type = 0;
    a.size = 0;
    a.mtime = 0;
    a.ctime = 0;
    a.atime = 0;
  }
  else{
    a.type = uint32_t(inode->type);
    a.size = inode->size;
    a.mtime = inode->mtime;
    a.ctime = inode->ctime;
    a.atime = inode->atime;
  }
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *free_node = get_inode(inum);
  blockid_t *free_block = free_node->blocks;
  for(uint32_t i = 0; i <= NDIRECT; ++i){
    if(i < NDIRECT && free_block[i] != 0){
      bm->free_block(free_block[i]);
    }
    if(i == NDIRECT && free_block[i] != 0){ //deal with indirect inode
      char buf[BLOCK_SIZE] = {'\0'};
      bm->read_block(free_block[i], buf); //get the int array
      int *Array = (int *)buf;
      for(uint32_t j = 0; j < NINDIRECT; ++j){
        if(Array[i] != 0){
          bm->free_block(Array[i]);
        }
      }
    }
  }
  free_inode(inum);
  delete free_node;
  return;
}
