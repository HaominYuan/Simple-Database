#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>


struct InputBuffer_t {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

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

enum StatementType_t {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

struct Row_t {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
};
typedef struct Row_t Row;

struct Statement_t {
    StatementType type;
    Row row_to_insert;
};
typedef struct Statement_t Statement;

enum ExecuteResult_t {
    EXECUTE_SUCCESS,
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
uint32_t ROWS_PER_PAGE = 0;
uint32_t TABLE_MAX_ROWS = 0;


struct Pager_t {
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
    Pager *pager;
    uint32_t num_rows;
};
typedef struct Table_t Table;

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
void *row_slot(Table *table, uint32_t row_num);
ExecuteResult execute_insert(Statement *statement, Table *table);
ExecuteResult execute_select(Statement *statement, Table *table);
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
void *get_page(Pager* pager, uint32_t page_num);
Pager* pager_open(const char *filename);
Table *db_open(const char *filename);
void pager_flush(Pager *pager, uint32_t page_num, uint32_t size);
void db_close(Table *table);


int main(int argc, char *argv[]) {
    initialize();
    if (argc < 2) {
        printf("Must supply a database filename.\n");
	exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
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
    ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
    TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
}


ExecuteResult execute_insert(Statement *statement, Table *table) {
    /* 判断表是否已满 */
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement->row_to_insert);

    /* 序列化行，并将行的数目加一 */
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}


ExecuteResult execute_select(Statement *statement, Table *table) {
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
	print_row(&row);
    }
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


void *row_slot(Table *table, uint32_t row_num) {
    /* 计算出这一行储存在哪一个页中 */
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    /* 获得那一页的指针 */
    void *page = get_page(table->pager, page_num);
    /* 计算出在那一页的第几行 */
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    /* 计算出字节偏移量 */
    uint32_t byte_offset = row_offset * ROW_SIZE;
    /* 返回指针 */
    return page + byte_offset;
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
    statement->type = STATEMENT_INSERT;

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

    if (strlen(email) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;

    return table;
}

void *get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
	exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
	void *page = malloc(PAGE_SIZE);
	uint32_t num_pages = pager->file_length / PAGE_SIZE;

	// We might save a partial page at the end of the file
	if (pager->file_length % PAGE_SIZE) {
	    num_pages += 1;
	}

	if (page_num <= num_pages) {
	    lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
	    ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
	    if (bytes_read == -1) {
	        printf("Error reading file: %d\n", errno);
		exit(EXIT_FAILURE);
	    }
	}

	pager->pages[page_num] = page;
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

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void db_close(Table *table) {
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
	    continue;
	}
	pager_flush(pager, i, PAGE_SIZE);
	free(pager->pages[i]);
	pager->pages[i] = NULL;
    }

    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
	if (pager->pages[page_num] != NULL) {
	    pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
	    free(pager->pages[page_num]);
	    pager->pages[page_num] = NULL;
	}
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
	exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void *page = pager->pages[i];
	if (page) {
	    free(page);
	    pager->pages[i] = NULL;
	}
    }
    free(pager);
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
	exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
	exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
	exit(EXIT_FAILURE);
    }
}
