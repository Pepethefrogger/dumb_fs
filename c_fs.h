#define _GNU_SOURCE
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#define BLOCK_SIZE 4096
#define MAX_NAME_LENGTH 64
#define MAX_FD 1024

#define MAP_OFFSET(off, ptr) (size_t)((char*)(ptr) - (size_t)(off))
#define OUT_OFFSET(off, ptr) (void*)((char*)(off) + (ptr))

#define NULL_OFF 0

typedef size_t NodeOffset;
typedef size_t BlockOffset;

typedef enum { ROOT, FIL, DIR, SIM } NodeType;

typedef struct {
    size_t size;
    BlockOffset first_block;
} FileNode;

typedef struct {
    NodeOffset first_child;
} DirNode;

typedef struct Node {
    NodeType type;
    NodeOffset parent;
    NodeOffset next_sibling;
    char name[MAX_NAME_LENGTH];
    union { FileNode file; DirNode dir; } node;
} Node;

typedef struct {
    NodeOffset next_node;
} EmptyNode;

#define MAX_NODE_COUNT (BLOCK_SIZE - sizeof(void*) - sizeof(size_t)) / sizeof(Node)

// There are blocks that hold data and others that only hold nodes
typedef struct {
    BlockOffset next_block;
    size_t node_count;
    Node nodes[];
} NodeBlock;

#define MAX_DATA_CAPACITY (BLOCK_SIZE - sizeof(void*)) / sizeof(char)

typedef struct {
    BlockOffset next_block;
    char data[];
} DataBlock;

typedef struct {
    BlockOffset next_block;
} EmptyBlock;

typedef union { NodeBlock node; DataBlock data; } Block;

typedef struct {
    NodeType type;
    void* _padd1;
    void* _padd2;
    char _pad3[MAX_NAME_LENGTH];
    NodeOffset first_free_node;
    NodeOffset first_free_block;
    NodeOffset root_dir;
    NodeOffset first_block;
} RootNode;

typedef struct {
    int in_use;
    NodeOffset file;
    size_t offset;
} FD;

typedef struct {
    int file;
    long file_size;
    size_t num_blocks;
    RootNode* root;
    FD fd_table[MAX_FD];
} Mapper;

typedef struct DirIterator {
    Mapper* mapper;
    NodeOffset node;
} DirIterator;

void insert_node(Mapper* mapper, Node* node, Node* insert) {
    NodeOffset next_sibling = insert->next_sibling;
    insert->next_sibling = MAP_OFFSET(mapper->root, node);
    node->next_sibling = next_sibling;
}

NodeOffset get_node(Mapper* mapper);

Mapper* new_mapper(char* filename) {
    Mapper* mapper = (Mapper*)calloc(1, sizeof(Mapper));
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        perror("open");
        exit(1);
    }
    mapper->file = fd;
    mapper->file_size = lseek(fd, 0, SEEK_END);
    mapper->num_blocks = mapper->file_size / BLOCK_SIZE;
    int file_empty = mapper->num_blocks == 0;
    if (file_empty) {
        if (ftruncate(fd, BLOCK_SIZE) == -1) {
            perror("ftruncate");
            close(fd);
            exit(1);
        }
        mapper->file_size = BLOCK_SIZE;
        mapper->num_blocks = 1;
    }
    RootNode* root = (RootNode*)mmap(0, mapper->file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (root == MAP_FAILED) {
        perror("mmap");
        close(fd);
        exit(1);
    }
    // Needed for get_node
    mapper->root = root;
    if (file_empty) {
        root->type = ROOT;
        root->first_free_block = NULL_OFF;
        root->first_free_node = NULL_OFF;

        // Can't access root directly from here, since this may move it
        NodeOffset rd = get_node(mapper);
        Node* root_dir = (Node*)OUT_OFFSET(mapper->root, rd);
        root_dir->type = DIR;
        root_dir->name[0] = 0;
        root_dir->node.dir.first_child = NULL_OFF;
        root_dir->next_sibling = NULL_OFF;
        root_dir->parent = NULL_OFF;

        mapper->root->root_dir = rd;
    }
    return mapper;
}

BlockOffset get_first_empty_block(RootNode* root) {
    BlockOffset b = root->first_free_block;
    if (b == NULL_OFF) return NULL_OFF;
    EmptyBlock* block = (EmptyBlock*)OUT_OFFSET(root, root->first_free_block);

    BlockOffset n = block->next_block;
    if (n == NULL_OFF) {
        root->first_free_block = NULL_OFF;
        return b;
    }
    EmptyBlock* next = (EmptyBlock*)OUT_OFFSET(root, n);
    root->first_free_block = MAP_OFFSET(root, next);
    return b;
}

BlockOffset get_block(Mapper* mapper) {
    BlockOffset block = get_first_empty_block(mapper->root);
    if (block != NULL_OFF) return block;

    long old_size = mapper->file_size;
    mapper->file_size += BLOCK_SIZE;
    size_t block_idx = mapper->num_blocks;
    mapper->num_blocks += 1;
    if (ftruncate(mapper->file, mapper->file_size) == -1) {
        perror("ftruncate");
        close(mapper->file);
        exit(1);
    }
    void* new_map = mremap(mapper->root, old_size, mapper->file_size, MREMAP_MAYMOVE);
    if (new_map == MAP_FAILED) {
        perror("mremap");
        close(mapper->file);
        exit(1);
    }
    mapper->root = (RootNode*)new_map;
    block = block_idx * BLOCK_SIZE;
    return block;
}

BlockOffset new_data_block(Mapper* mapper) {
    BlockOffset block = get_block(mapper);
    memset(OUT_OFFSET(mapper->root, block), 0, BLOCK_SIZE);
    return block;
}

BlockOffset new_node_block(Mapper* mapper) {
    BlockOffset b = get_block(mapper);
    Block* block = (Block*)OUT_OFFSET(mapper->root, b);
    block->node.next_block = NULL_OFF;
    block->node.node_count = 0;

    BlockOffset next = mapper->root->first_block;
    if (next == NULL_OFF) {
        mapper->root->first_block = b;
        return b;
    }
    mapper->root->first_block = b;
    block->node.next_block = next;
    return b;
}

NodeOffset get_first_empty_node(RootNode* root) {
    NodeOffset first_free = root->first_free_node;
    if (first_free == NULL_OFF) return NULL_OFF;
    EmptyNode* node = (EmptyNode*)OUT_OFFSET(root, first_free);

    if (node->next_node == NULL_OFF) {
        root->first_free_node = NULL_OFF;
        return first_free;
    }
    EmptyNode* next = (EmptyNode*)OUT_OFFSET(root, node->next_node);
    root->first_free_node = MAP_OFFSET(root, next);
    return first_free;
}

NodeOffset get_node(Mapper* mapper) {
    NodeOffset node = get_first_empty_node(mapper->root);
    if (node != NULL_OFF) return node;

    BlockOffset f = mapper->root->first_block;
    Block* first = (Block*)OUT_OFFSET(mapper->root, f);
    if (f == NULL_OFF || first->node.node_count >= MAX_NODE_COUNT) {
        BlockOffset b = new_node_block(mapper);
        NodeBlock* block = (NodeBlock*)OUT_OFFSET(mapper->root, b);
        block->node_count = 1;
        block->next_block = f;
        mapper->root->first_block = MAP_OFFSET(mapper->root, block);
        return MAP_OFFSET(mapper->root, &block->nodes[0]);
    }
    first->node.node_count += 1;
    return MAP_OFFSET(mapper->root, &first->node.nodes[first->node.node_count - 1]);
}

void close_mapper(Mapper* mapper) {
    if (msync(mapper->root, mapper->file_size, MS_SYNC) == -1) {
        perror("msync");
        close(mapper->file);
        exit(1);
    }
    close(mapper->file);
}

void initialize_dir(Mapper* mapper, Node* dir) {
    dir->type = DIR;
    dir->node.dir.first_child = NULL_OFF;
}

void initialize_file(Mapper* mapper, Node* file) {
    file->type = FIL;
    file->node.file.first_block = NULL_OFF;
    file->node.file.size = 0;
}

DirIterator create_iterator(Mapper *mapper, NodeOffset n);
NodeOffset iter_next(DirIterator *iter);

NodeOffset create_children(Mapper* mapper, Node* dir, char* name) {
    assert(dir->type == DIR);

    DirIterator iter = create_iterator(mapper, MAP_OFFSET(mapper->root, dir));
    NodeOffset search;
    while (search = iter_next(&iter), search != NULL_OFF) {
        Node* s = (Node*)OUT_OFFSET(mapper->root, search);
        if (strcmp(s->name, name) == 0) {
            puts("there already is a node with that name");
            exit(1);
        }
    }
    NodeOffset nc = get_node(mapper);
    Node* new_child = (Node*)OUT_OFFSET(mapper->root, nc);

    new_child->parent = MAP_OFFSET(mapper->root, dir);
    strncpy(new_child->name, name, MAX_NAME_LENGTH);

    NodeOffset first_child = dir->node.dir.first_child;
    dir->node.dir.first_child = MAP_OFFSET(mapper->root, new_child);
    new_child->next_sibling = first_child;
    return nc;
}

void create_dir(Mapper* mapper, NodeOffset d, char* name) {
    Node* dir = (Node*)OUT_OFFSET(mapper->root, d);
    Node* child = (Node*)OUT_OFFSET(mapper->root, create_children(mapper, dir, name));
    initialize_dir(mapper, child);
}

void create_file(Mapper* mapper, NodeOffset d, char* name) {
    Node* dir = (Node*)OUT_OFFSET(mapper->root, d);
    Node* child = (Node*)OUT_OFFSET(mapper->root, create_children(mapper, dir, name));
    initialize_file(mapper, child);
}

size_t get_empty_fd(Mapper* mapper) {
    for (size_t i = 0; i < MAX_FD; i++) {
        if (!mapper->fd_table[i].in_use) {
            return i;
        }
    }
    return MAX_FD;
}

size_t open_file(Mapper* mapper, Node* file) {
    size_t i = get_empty_fd(mapper);
    FD* fd = &mapper->fd_table[i];
    fd->in_use = 1;
    fd->file = MAP_OFFSET(mapper->root, file);
    fd->offset = 0;
    return i;
}

void close_file(Mapper* mapper, size_t fd) {
    mapper->fd_table[fd].in_use = 0;
}

size_t min(size_t a, size_t b) {
    if (a < b) return a;
    return b;
}

// In these functions, we only store offsets since we are constantly using functions that may reallocate

int write_file(Mapper* mapper, size_t fd, void* data, size_t len) {
    FD* entry = &mapper->fd_table[fd];
    assert(entry->in_use);
    size_t offset = entry->offset;
    NodeOffset file = entry->file;

    size_t file_length = ((Node*)OUT_OFFSET(mapper->root, file))->node.file.size;
    if (offset + len >= file_length) {
        ((Node*)OUT_OFFSET(mapper->root, file))->node.file.size = offset + len + 1;
    }

    size_t num_block = offset / BLOCK_SIZE;

    if (((Node*)OUT_OFFSET(mapper->root, file))->node.file.first_block == NULL_OFF) {
        ((Node*)OUT_OFFSET(mapper->root, file))->node.file.first_block = new_data_block(mapper);
    }

    BlockOffset block = ((Node*)OUT_OFFSET(mapper->root, file))->node.file.first_block;
    for (size_t i = 0; i < num_block; i++) {
        if (((Block*)OUT_OFFSET(mapper->root, block))->data.next_block == NULL_OFF) {
            BlockOffset new_block = new_data_block(mapper);
            ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block = new_block;
        }
        block = ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block;
    }

    size_t block_offset = offset % BLOCK_SIZE;
    size_t n_written = 0;
    while (n_written != len) {
        char* dst = ((Block*)OUT_OFFSET(mapper->root, block))->data.data + block_offset;
        size_t n = min(BLOCK_SIZE - block_offset, len - n_written);
        memcpy(dst, ((char*)data) + n_written, n);
        n_written += n;
        block_offset = 0;
        if (n_written != len && ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block == NULL_OFF) {
            BlockOffset new_block = new_data_block(mapper);
            ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block = new_block;
        }
        block = ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block;
    }

    entry->offset += n_written;
    return n_written;
}

int read_file(Mapper* mapper, size_t fd, void* data, size_t len) {
    FD* entry = &mapper->fd_table[fd];
    assert(entry->in_use);
    size_t offset = entry->offset;
    NodeOffset file = entry->file;

    size_t file_length = ((Node*)OUT_OFFSET(mapper->root, file))->node.file.size;
    len = min(len, file_length - offset);

    size_t num_block = offset / BLOCK_SIZE;

    if (((Node*)OUT_OFFSET(mapper->root, file))->node.file.first_block == NULL_OFF) {
        ((Node*)OUT_OFFSET(mapper->root, file))->node.file.first_block = new_data_block(mapper);
    }

    BlockOffset block = ((Node*)OUT_OFFSET(mapper->root, file))->node.file.first_block;
    for (size_t i = 0; i < num_block; i++) {
        if (((Block*)OUT_OFFSET(mapper->root, block))->data.next_block == NULL_OFF) {
            BlockOffset new_block = new_data_block(mapper);
            ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block = new_block;
        }
        block = ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block;
    }

    size_t block_offset = offset % BLOCK_SIZE;
    size_t n_read = 0;
    while (n_read != len) {
        char* src = ((Block*)OUT_OFFSET(mapper->root, block))->data.data + block_offset;
        size_t n = min(BLOCK_SIZE - block_offset, len - n_read);
        memcpy(((char*)data) + n_read, src, n);
        n_read += n;
        block_offset = 0;
        if (n_read != len && ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block == NULL_OFF) {
            BlockOffset new_block = new_data_block(mapper);
            ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block = new_block;
        }
        block = ((Block*)OUT_OFFSET(mapper->root, block))->data.next_block;
    }

    entry->offset += n_read;
    return len;
}

void seek_file(Mapper* mapper, size_t fd, size_t offset, int flag) {
    FD* entry = &mapper->fd_table[fd];
    assert(entry->in_use);
    Node* file = (Node*)OUT_OFFSET(mapper->root, entry->file);
    switch (flag) {
        case SEEK_SET:
            entry->offset = offset;
            break;
        case SEEK_END:
            entry->offset = file->node.file.size - offset - 1;
            break;
        case SEEK_CUR:
            entry->offset += offset;
    }
}

DirIterator create_iterator(Mapper* mapper, NodeOffset n) {
    Node* node = (Node*)OUT_OFFSET(mapper->root, n);
    assert(node->type == DIR);
    DirIterator iter = {
        .mapper = mapper,
        .node = node->node.dir.first_child
    };
    return iter;
}

NodeOffset iter_next(DirIterator* iter) {
    if (iter->node == NULL_OFF) {
        return NULL_OFF;
    }
    NodeOffset n = iter->node;
    Node* node = (Node*)OUT_OFFSET(iter->mapper->root, n);
    if (node->next_sibling == NULL_OFF) {
        iter->node = NULL_OFF;
    } else {
        iter->node = node->next_sibling;
    }
    return n;
}

NodeOffset traverse_single_step(Mapper* mapper, NodeOffset dir, char* name, size_t len) {
    DirIterator iter = create_iterator(mapper, dir);

    NodeOffset n;
    while (n = iter_next(&iter), n != NULL_OFF) {
        Node* node = (Node*)OUT_OFFSET(mapper->root, n);
        if (strncmp(node->name, name, len) == 0) {
            return n;
        }
    }
    return NULL_OFF;
}

int find_char(char* str, char c) {
    for (size_t i = 0; str[i] != 0; i++) {
        if (str[i] == c) {
            return i;
        }
    }
    return -1;
}

NodeOffset traverse_path(Mapper* mapper, NodeOffset dir, char* path) {
    int finished = 0;
    NodeOffset found_node = dir;
    while (1) {
        int end = find_char(path, '/');
        if (end == -1) {
            end = find_char(path, 0);
            finished = 1;
        }
        Node* f = (Node*)OUT_OFFSET(mapper->root, found_node);
        if (strncmp(path, "..", end) == 0) {
            if (f->parent != NULL_OFF) {
                found_node = f->parent;
            }
        } else {
            found_node = traverse_single_step(mapper, found_node, path, end);
        }
        if (finished) {
            return found_node;
        }
        path = path + end + 1;
    }
}
