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

InputBuffer *new_input_buffer(void);
void print_prompt(void);
void read_input(InputBuffer *input_buffer);

int main(int argc, char *argv[]) {
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (strcmp(input_buffer->buffer, ".exit") == 0) {
            /* 定义在stdlib.h 头文件中 #define EXIT_SUCCESS 0 */
            exit(EXIT_SUCCESS);
        } else {
            printf("Unrecognized command '%s'. \n", input_buffer->buffer);        }
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


