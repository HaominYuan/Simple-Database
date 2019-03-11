#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

// 输入存放的位置
struct InputBuffer_t {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

// 元命令执行的结果
enum MetaCommandResult_t {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareResult_t {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

// 声明类型
enum StatementType_t {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

// 注意这里的常量并不等于下面的数组长度
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

// 数据库中的一条记录
struct Row_t {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
};
typedef struct Row_t Row;

// 声明，包括声明的类型和记录
struct Statement_t {
    StatementType type;
    Row row_to_insert;
};
typedef struct Statement_t Statement;

// 声明执行后的状态
enum ExecuteResult_t {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL
};
typedef enum ExecuteResult_t ExecuteResult;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define ID_OFFSET 0
uint32_t ID_SIZE = size_of_attribute(Row, id);
uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
uint32_t USERNAME_OFFSET = 0;
uint32_t EMAIL_OFFSET = 0;
uint32_t ROW_SIZE = 0;

#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100

/* B 树内部节点头部布局
 * NODE_TYPE_SIZE          节点类型       1
 * IS_ROOT_SIZE            是否为根节点   1
 * PARENT_POINTER_SIZE     指向父亲的指针 4
 * */
uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
uint32_t NODE_TYPE_OFFSET = 0;
uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
uint32_t IS_ROOT_OFFSET = 0;
uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
uint32_t PARENT_POINTER_OFFSET = 0;
uint8_t  COMMON_NODE_HEADER_SIZE = 0;

/* B 树叶子结点头部布局
 * LEAF_NODE_NUM_CELLS_SIZE 键值对 4
 */
uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
uint32_t LEAF_NODE_NUM_CELLS_OFFSET = 0;
uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = 0;
uint32_t LEAF_NODE_HEADER_SIZE = 0;

/* B 树叶子结点体布局
 * KEY_SIZE     键         4
 * VALUE_SIZE   值的大小   ROW_SIZE
 */
uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
uint32_t LEAF_NODE_KEY_OFFSET = 0;
uint32_t LEAF_NODE_VALUE_SIZE = 0;
uint32_t LEAF_NODE_VALUE_OFFSET = 0;
uint32_t LEAF_NODE_CELL_SIZE = 0;
uint32_t LEAF_NODE_SPACE_FOR_CELLS = 0;
uint32_t LEAF_NODE_MAX_CELLS = 0;
uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = 0;
uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = 0;

/* B 树内部节点头部布局
 */

uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = 0;
uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = 0;
uint32_t INTERNAL_NODE_HEADER_SIZE = 0;

/* B 树内部节点体布局 */
uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
uint32_t INTERNAL_NODE_CELL_SIZE = 0;

// 数据库文件，包括所有的页面
struct Pager_t {
    int file_descriptor;
    uint32_t file_length;
    // 记录目前页面的总数。
    uint32_t num_pages;
    void *pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

// 表格
struct Table_t {
    Pager *pager;
    uint32_t root_page_num;
};
typedef struct Table_t Table;

// 游标
struct Cursor_t {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
};
typedef struct Cursor_t Cursor;

// 节点类型
enum NodeType_t {
    NODE_INTERNAL,
    NODE_LEAF
};
typedef enum NodeType_t NodeType;

void initialize();
InputBuffer *new_input_buffer(void);
void print_prompt(void);
void read_input(InputBuffer *input_buffer);
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);
ExecuteResult execute_statement(Statement *statement, Table *table);
void print_row(Row *row);
void serialize_row(Row *source, void *destination);
void deserialize_row(void *source, Row *destination);
void *cursor_value(Cursor *cursor);
ExecuteResult execute_insert(Statement *statement, Table *table);
ExecuteResult execute_select(Statement *statement, Table *table);
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
void *get_page(Pager* pager, uint32_t page_num);
Pager* pager_open(const char *filename);
Table *db_open(const char *filename);
void pager_flush(Pager *pager, uint32_t page_num);
void db_close(Table *table);
Cursor *table_start(Table *table);
void cursor_advance(Cursor *cursor);

/* 访问页节点中的属性*/
uint32_t *leaf_node_num_cells(void *node);
void *leaf_node_cell(void *node, uint32_t cell_num);
uint32_t *leaf_node_key(void *node, uint32_t cell_num);
void *leaf_node_value(void *node, uint32_t cell_num);
void initialize_leaf_node(void *node);

/* 插入节点 */
void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value);

/* 打印数据库信息 */
void print_constants();

/* 在table 中朝对应的游标 */
Cursor *table_find(Table *table, uint32_t key);

/* 在叶子中朝相对应的游标 */
Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key);

/* 获得节点的种类 */
NodeType get_node_type(void *node);

/* 设置节点的种类 */
void set_node_type(void *node, NodeType type);

/* 分隔节点并插入 */
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value);

/* 获取未使用的页面编号 */
uint32_t get_unused_page_num(Pager *pager);

/* 创建新的根节点 */
void create_new_root(Table *table, uint32_t right_child_page_num);

uint32_t *internal_node_num_keys(void *node);

uint32_t *internal_node_right_child(void *node);

uint32_t *internal_node_cell(void *node, uint32_t cell_num);

uint32_t *internal_node_child(void *node, uint32_t child_num);

uint32_t *internal_node_key(void *node, uint32_t key_num);

uint32_t get_node_max_key(void *node);


/* 判断是否为根节点 */
bool is_node_root(void *node);

void set_node_root(void *node, bool is_root);

/* 初始化B 树内部节点 */
void initialize_internal_node(void *node);

/* 打印整棵树 */
void indent(uint32_t level);
void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level);


Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key);

uint32_t *leaf_node_next_leaf(void *node);

int main(int argc, char *argv[]) {
    initialize();
    if (argc < 2) {
        printf("Must supply a database filename.\n");
	exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    // 打开文件，并没有赋予空间
    Table *table = db_open(filename);

    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

	if (input_buffer->buffer[0] == '.') {
	    switch (do_meta_command(input_buffer, table)) {
	        case (META_COMMAND_SUCCESS):
		    continue;
		case (META_COMMAND_UNRECOGNIZED_COMMAND):
		    printf("Unrecognized command '%s'\n", input_buffer->buffer);
		    continue;
	    }    
	}

	Statement statement;
	switch (prepare_statement(input_buffer, &statement)) {
	    case (PREPARE_SUCCESS):
	        break;
	    case (PREPARE_NEGATIVE_ID):
	        printf("ID must be positive.\n");
		continue;
	    case (PREPARE_STRING_TOO_LONG):
	        printf("String is too long.\n");
		continue;
	    case (PREPARE_SYNTAX_ERROR):
	        printf("Syntax error. Could not parse statement.\n");
		continue;
	    case (PREPARE_UNRECOGNIZED_STATEMENT):
	        printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer);
		continue;
	}

	switch (execute_statement(&statement, table)) {
	    case (EXECUTE_SUCCESS):
	        printf("Executed.\n");
		break;
	    case (EXECUTE_DUPLICATE_KEY):
	        printf("Error: Duplicate key.\n");
		break;
	    case (EXECUTE_TABLE_FULL):
	        printf("Error: Table full.\n");
		break;
        }
    }
    return 0;
}

/* 结构初始化 */
InputBuffer *new_input_buffer(void) {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    /* malloc 函数不对分配的内存区域进行初始化 */
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

/* 输出提示语 */
void print_prompt(void) {
    printf("db > ");
}


void read_input(InputBuffer* input_buffer) {
    /* ssize_t 表示有符号整数，在 32 位机器上等同于 int，在 64 位机器上等同于 long int。当 input_buffer->buffer 被设置为 NULL 并且 input_buffer->buffer_length 等于零时，getline 函数会自动分配一块内存空间来储存输入的行。当分配的空间不足时，会自动进行扩容。*/
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        /* 定义在 stdlib.h 头文件中，#define EXIT_FAULURE 1 */
        exit(EXIT_FAILURE);
    }

    /* 省略尾随的换行符 */
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}


MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
	print_tree(table->pager, 0, 0);
	return META_COMMAND_SUCCESS;
    } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
	print_constants();
	return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
	return prepare_insert(input_buffer, statement);
    }

    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
	return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
	    return execute_insert(statement, table);
	case (STATEMENT_SELECT):
	    return execute_select(statement, table);
    }
}

void initialize() {
    USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
    EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
    ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

    /* B 树公共节点头部布局 */
    IS_ROOT_OFFSET = NODE_TYPE_SIZE;
    PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
    COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
    
    /* B 树叶节点头部布局 */
    LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
    LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
    LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;
    
    /* B 树叶节点体布局 */

    LEAF_NODE_VALUE_SIZE = ROW_SIZE;
    LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
    LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
    LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
    LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

    /* 左右叶子的最大节点数 */
    LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
    LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;


    /* B 树内部节点头部布局 */
    INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
    INTERNAL_NODE_RIGHT_CHILD_OFFSET = 
        INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
    INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

}


ExecuteResult execute_insert(Statement *statement, Table *table) {
    /* 判断表是否已满 */
    void *node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Row *row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor *cursor = table_find(table, key_to_insert);

    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
	if(key_at_index == key_to_insert) {
	    return EXECUTE_DUPLICATE_KEY;
	}
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    free(cursor);

    return EXECUTE_SUCCESS;
}


ExecuteResult execute_select(Statement *statement, Table *table) {
    Cursor *cursor = table_start(table);
    
    Row row;
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
	print_row(&row);
	cursor_advance(cursor);
    }

    free(cursor);
    return EXECUTE_SUCCESS;
}

void print_row(Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row *source, void *destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}


void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}


void *cursor_value(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    /* 获得那一页的指针 */
    void *page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
    // 将声明置为插入语句
    statement->type = STATEMENT_INSERT;

    // strtok() 函数为不可重入函数，不安全。
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);

    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->num_pages == 0) {
        void *root_node = get_page(pager, 0);
	initialize_leaf_node(root_node);
	set_node_root(root_node, true);
    }

    return table;
}

void *get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
	exit(EXIT_FAILURE);
    }

    // 这里意味着只有当用的时候才将数据从磁盘当中读取出来
    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
	void *page = malloc(PAGE_SIZE);
	uint32_t num_pages = pager->file_length / PAGE_SIZE;

	// We might save a partial page at the end of the file
	if (pager->file_length % PAGE_SIZE) {
	    num_pages += 1;
	}

	// 如果小于意味可以从文件中读取出来
	if (page_num <= num_pages) {
	    lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
	    ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
	    if (bytes_read == -1) {
	        printf("Error reading file: %d\n", errno);
		exit(EXIT_FAILURE);
	    }
	}

	pager->pages[page_num] = page;

	if (page_num >= pager->num_pages) {
	    pager->num_pages = page_num + 1;
	}
    }

    return pager->pages[page_num];
}

Pager* pager_open(const char *filename) {
    /* O_RDWR  -> Read/Write mode
     * O_CREAT -> Create file if it does not exist
     * S_IWUSR -> User write permission
     * S_IRUSR -> User read permission */
    int fd = open(filename, O_RDWR | O_APPEND | S_IWUSR | S_IRUSR);

    if (fd == -1) {
        fd = open(filename, O_RDWR | O_CREAT | S_IWUSR | S_IRUSR);
    }

    if (fd == -1) {
        printf("Unable to open file\n");
	exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupte file.\n");
	exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void db_close(Table *table) {
    Pager *pager = table->pager;
    
    // 存放整数页
    for (uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
	    continue;
	}
	pager_flush(pager, i);
	free(pager->pages[i]);
	pager->pages[i] = NULL;
    }

    // 关闭文件
    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
	exit(EXIT_FAILURE);
    }

    // 释放空间
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void *page = pager->pages[i];
	if (page) {
	    free(page);
	    pager->pages[i] = NULL;
	}
    }
    // 释放空间
    free(pager);
}

void pager_flush(Pager *pager, uint32_t page_num) {
    // 存空页则报错
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
	exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
	exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
	exit(EXIT_FAILURE);
    }
}

Cursor *table_start(Table *table) {
    Cursor *cursor = table_find(table, 0);

    void *node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

void cursor_advance(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    void *node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
        uint32_t next_page_num = *leaf_node_next_leaf(node);
	if (next_page_num == 0) {
	    cursor->end_of_table = true;
	} else {
	    cursor->page_num = next_page_num;
	    cursor->cell_num = 0;
	}
    }
}

/* 获得一个 node 中键值对的个数的指针 */
uint32_t *leaf_node_num_cells(void *node) {
    return (uint32_t *)((char *)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

/* 根据 node 和 cell_num 返回一个键值对的指针 */
void *leaf_node_cell(void *node, uint32_t cell_num) {
    return (char *)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

/* 根据 node 和 cell_num 返回一个键值对的键的指针 */
uint32_t *leaf_node_key(void *node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

/* 根据 node 和 cell_num 返回一个键值对的值的指针 */
void *leaf_node_value(void *node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

/* 将 node 中键值对的个数设置为 0 */
void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
}

void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value) {
    void *node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        leaf_node_split_and_insert(cursor, key, value);
	return;
    }

    if (cursor->cell_num < num_cells) {
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
	    memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
	}
     }
    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

/* 打印数据库信息 */
void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

/* 返回对应键所在的游标 */
Cursor *table_find(Table *table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void *root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    }
    return internal_node_find(table, root_page_num, key);
 }

 Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key) {
    void *node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;
    
    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;

    while (min_index < one_past_max_index) {
        uint32_t index = min_index + (one_past_max_index - min_index) / 2;
	uint32_t key_at_index = *leaf_node_key(node, index);
	if (key_at_index < key) {
	    min_index = index + 1;
	} else {
	    one_past_max_index = index;
	}
    }

    cursor->cell_num = min_index;
    return cursor;
}

/* 获得节点的种类 */
NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t *)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

/* 设置节点的种类 */
void set_node_type(void *node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

/* 分割叶节点，并插入 */
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value) {
    void *old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void *new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;

    /* 分割复制 */
    for (int32_t i = LEAF_NODE_MAX_CELLS; i>=0; i--) {
        void * destination_node;
	if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
	    destination_node = new_node;
	} else {
            destination_node = old_node;
	}

	uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
	void* destination = leaf_node_cell(destination_node, index_within_node);
	if (i == cursor->cell_num) {
	    serialize_row(value, leaf_node_value(destination_node, index_within_node));
	    *leaf_node_key(destination_node, index_within_node) = key;
	} else if (i > cursor->cell_num) {
	    memcpy(destination, leaf_node_cell(old_node,i - 1), LEAF_NODE_CELL_SIZE);
	} else {
	    memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
	}
    }

    /* 更新两个节点的节点个数 */
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        printf("Need to implement updating parent after split\n");
	exit(EXIT_FAILURE);
    }
}

uint32_t get_unused_page_num(Pager *pager) {
    return pager->num_pages;
}


void create_new_root(Table *table, uint32_t right_child_page_num) {
    /* root 节点相当于以前的左节点 */
    void *root = get_page(table->pager, table->root_page_num);
    void *right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void *left_child = get_page(table->pager, left_child_page_num);

    /* 将 root 节点的数据复制给做节点 */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    /* root 节点是一个只有一个键和两个孩子的节点 */
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
}

uint32_t *internal_node_num_keys(void *node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t *internal_node_right_child(void *node) {
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t *internal_node_cell(void *node, uint32_t cell_num) {
    return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t *internal_node_child(void *node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
	exit(EXIT_FAILURE);
    }
    if (child_num == num_keys) {
        return internal_node_right_child(node);
    }

    return internal_node_cell(node, child_num);
}


uint32_t *internal_node_key(void *node, uint32_t key_num) {
    return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t get_node_max_key(void *node) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
	    return *internal_node_key(node, *internal_node_num_keys(node) - 1);
	case NODE_LEAF:
	    return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
}

bool is_node_root(void *node) {
    uint8_t value = *((uint8_t*) (node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(void *node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

void initialize_internal_node(void *node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}

void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level) {
    void *node = get_page(pager, page_num);
    uint32_t num_keys, child;

    switch (get_node_type(node)) {
        case (NODE_LEAF):
	    num_keys = *leaf_node_num_cells(node);
	    indent(indentation_level);
	    printf("- leaf (size %d)\n", num_keys);
	    for (uint32_t i = 0; i < num_keys; i++) {
	        indent(indentation_level + 1);
		printf("- %d\n", *leaf_node_key(node, i));
	    }
	    break;
	case (NODE_INTERNAL):
	    num_keys = *internal_node_num_keys(node);
	    indent(indentation_level);
	    printf("- internal (size %d)\n", num_keys);
	    for (uint32_t i = 0; i < num_keys; i++) {
	        child = *internal_node_child(node, i);
		print_tree(pager, child, indentation_level + 1);

		indent(indentation_level);
		printf("- key %d\n", *internal_node_key(node, i));
	    }

	    child = *internal_node_right_child(node);
	    print_tree(pager, child, indentation_level + 1);
	    break;
    }
}


Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key) {
    void *node = get_page(table->pager, page_num);
    uint32_t num_keys = *internal_node_num_keys(node);

    /* Binary search to find index of child to search */
    uint32_t min_index = 0;
    uint32_t max_index = num_keys;

    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    uint32_t child_num = *internal_node_child(node, min_index);
    void *child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
	    return leaf_node_find(table, child_num, key);
    }

}

uint32_t* leaf_node_next_leaf(void *node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}
