#pragma once

#define MDB_ERROR      -1
#define MDB_OK         0
#define MDB_BUSY       5

#define FREE_SPACE_OFFSET 1
#define CELL_NUM_OFFSET 3

#define NO_LOCK         0
#define SHARED_LOCK     1
#define RESERVED_LOCK   2
#define PENDING_LOCK    3
#define EXCLUSIVE_LOCK  4

#define PENDING_BYTE      0x40000000
#define RESERVED_BYTE     (PENDING_BYTE+1)
#define SHARED_FIRST      (PENDING_BYTE+2)
#define SHARED_SIZE       510