#include "c_fs.h"
#include <stdio.h>
#include <string.h>

void print_node(Mapper* mapper, NodeOffset n) {
    Node* node = OUT_OFFSET(mapper->root, n);
    switch (node->type) {
        case DIR:
            if (node->parent == NULL_OFF) {
                puts("/ dir\n");
            } else {
                printf("%s dir\n", node->name);
            }
            break;
        case FIL:
            printf("%s file, size %zu\n", node->name, node->node.file.size);
            break;
        default:
            break;
    }
}

void ls(Mapper* mapper, NodeOffset node) {
    Node* dir = OUT_OFFSET(mapper->root, node);
    char* name = dir->name;
    if (dir->parent == NULL_OFF) {
        name = "/";
    }
    printf("Listing directory %s\n", name);
    DirIterator iter = create_iterator(mapper, node);
    NodeOffset n;
    while (n = iter_next(&iter), n != NULL_OFF) {
        print_node(mapper, n);
    }
}

int validate_name(char* name) {
    return find_char(name, '/') == -1;
}

void trim_newline(char* str) {
    int index = find_char(str, '\n');
    if (index == -1) return;
    str[index] = 0;
}

int main() {
    Mapper* mapper = new_mapper("fs.img");
    NodeOffset cwd = mapper->root->root_dir;

    char line[1024];
    while (1) {
        if (fgets(line, sizeof(line), stdin)) {
            trim_newline(line);
            if (strncmp(line, "lsof", 4) == 0) {
                for (size_t i = 0; i <= MAX_FD; i++) {
                    FD entry = mapper->fd_table[i];
                    if (entry.in_use) {
                        Node* file = (Node*)OUT_OFFSET(mapper->root, entry.file);
                        printf("%zu -> %s\n", i, file->name);
                    }
                }
            } else if (strncmp(line, "lsfree", 6) == 0) {
                puts("Free node indices\n");
                NodeOffset empty_node = mapper->root->first_free_node;
                while (empty_node != NULL_OFF) {
                    EmptyNode* node = (EmptyNode*)OUT_OFFSET(mapper->root, empty_node);
                    printf("\tnode %zu\n", empty_node);
                    empty_node = node->next_node;
                }

                puts("Free block indices\n");
                BlockOffset empty_block = mapper->root->first_free_block;
                while (empty_block != NULL_OFF) {
                    EmptyBlock* block = (EmptyBlock*)OUT_OFFSET(mapper->root, empty_block);
                    printf("\tblock %zu\n", empty_block);
                    empty_block = block->next_block;
                }
            } else if (strncmp(line, "ls", 2) == 0) {
                ls(mapper, cwd);
            } else if (strncmp(line, "cd", 2) == 0) {
                char path[256];
                if (sscanf(line, "cd %s", path) != 1) {
                    puts("invalid use of cd");
                    continue;
                }
                NodeOffset found;
                if (found = traverse_path(mapper, cwd, path), found != NULL_OFF) {
                    cwd = found;
                } else {
                    printf("path %s not found\n", path);
                }
            } else if (strncmp(line, "mkdir", 5) == 0) {
                char name[MAX_NAME_LENGTH];
                if (sscanf(line, "mkdir %s", name) != 1) {
                    puts("invalid use of mkdir");
                    continue;
                }
                if (!validate_name(name)) {
                    printf("invalid name %s\n", name);
                    continue;
                }
                create_dir(mapper, cwd, name);
            } else if (strncmp(line, "touch", 5) == 0) {
                char name[MAX_NAME_LENGTH];
                if (sscanf(line, "touch %s", name) != 1) {
                    puts("invalid use of touch");
                    continue;
                }
                if (!validate_name(name)) {
                    printf("invalid name %s\n", name);
                    continue;
                }
                create_file(mapper, cwd, name);
            } else if (strncmp(line, "open", 4) == 0) {
                char path[256];
                if (sscanf(line, "open %s", path) != 1) {
                    puts("invalid use of open");
                    continue;
                }
                NodeOffset offset = traverse_path(mapper, cwd, path);
                if (offset == NULL_OFF) {
                    printf("file %s doesn't exist\n", path);
                    continue;
                }
                Node* n = (Node*)OUT_OFFSET(mapper->root, traverse_path(mapper, cwd, path));
                if (n->type != FIL) {
                    printf("path %s is not a file\n", path);
                    continue;
                }
                size_t fd = open_file(mapper, n);
                printf("opened with fd %zu\n", fd);
            } else if (strncmp(line, "close", 5) == 0) {
                int fd;
                if (sscanf(line, "close %d", &fd) != 1) {
                    puts("invalid use of close");
                    continue;
                }
                FD* entry = &mapper->fd_table[fd];
                if (!entry->in_use) {
                    printf("file descriptor %d is not being used", fd);
                    continue;
                }
                close_file(mapper, fd);
            } else if (strncmp(line, "read", 4) == 0) {
                char buffer[1024];
                int fd;
                int size;
                if (sscanf(line, "read %d %d", &fd, &size) != 2) {
                    puts("invalid use of read");
                    continue;
                }
                if (size >= 1024) {
                    puts("too large");
                }
                int read = read_file(mapper, fd, buffer, size);
                buffer[read] = 0;
                printf("Read %d bytes:\n%s\n", read, buffer);
            } else if (strncmp(line, "write", 5) == 0) {
                char buffer[1024] = {0};
                int fd;
                if (sscanf(line, "write %d %1023s", &fd, buffer) != 2) {
                    puts("invalid use of write");
                    continue;
                }
                int len = strlen(buffer);
                int written = write_file(mapper, fd, buffer, len);
                printf("wrote %d bytes\n", written);
            } else if (strncmp(line, "seek", 4) == 0) {
                char buffer[4];
                int fd;
                int offset;
                int flag;
                if (sscanf(line, "seek %d %d %3s", &fd, &offset, buffer) != 3) {
                    puts("invalid use of seek");
                    continue;
                }
                if (strncmp(buffer, "cur", 3) == 0) {
                    flag = SEEK_CUR;
                } else if (strncmp(buffer, "end", 3) == 0) {
                    flag = SEEK_END;
                } else if (strncmp(buffer, "set", 3) == 0) {
                    flag = SEEK_SET;
                } else {
                    puts("invalid seek flag");
                    continue;
                }
                seek_file(mapper, fd, offset, flag);
            } else if (strncmp(line, "rm", 2) == 0) {
                char path[256];
                if (sscanf(line, "rm %s", path) != 1) {
                    puts("invalid rm usage");
                    continue;
                }
                NodeOffset node = traverse_path(mapper, cwd, path);
                if (node == NULL_OFF) {
                    printf("node %s doesn't exist", path);
                    continue;
                }
                if (delete_child(mapper, node) == 1) {
                    printf("deleted file %s\n", path);
                } else {
                    printf("couldn't delete file %s\n", path);
                }
            } else if (strncmp(line, "exit", 4) == 0) {
                return 0;
            } else {
                printf("Unknown command: %s\n", line);
            }
        }
    }
}
