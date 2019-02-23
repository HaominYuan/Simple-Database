#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

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
    PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

struct Statement_t {
    StatementType type;
};
typedef struct Statement_t Statement;


InputBuffer *new_input_buffer(void);
void print_prompt(void);
void read_input(InputBuffer *input_buffer);
MetaCommandResult do_meta_command(InputBuffer *input_buffer);
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);
void execute_statement(Statement *statement);




int main(int argc, char *argv[]) {
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

	if (input_buffer->buffer[0] == '.') {
	    switch (do_meta_command(input_buffer)) {
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
	    case (PREPARE_UNRECOGNIZED_STATEMENT):
	        printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer);
		continue;
	}

	execute_statement(&statement);
	printf("Executed.\n");
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


MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
	return PREPARE_SUCCESS;
    }

    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
	return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
	    printf("This is where we would do an insert.\n");
	    break;
	case (STATEMENT_SELECT):
	    printf("This is where we would do a select.\n");
	    break;
    }
}
