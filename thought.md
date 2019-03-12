# 打开数据库文件

- 检查是否传入了数据库文件的名称，如果未传入名称，则输出提示信息，并退出程序。
- 调用 `Table *db_open(const char *filename)` 函数打开数据库文件。
- Table *db_open(const char *filiname)
  - 调用 `Pager* pager_open(const char *filename)` 函数打开数据库文件。
  - 初始化变量 table，并将根页面设置为第一个页面即`able->root_page_num = 0`。
  - 从页面的个数是否为零可以知道打开的是否为新文件，若页面的个数为零。如果
    - 调用 `void *get_page(Pager *pager, uint32_t page_num)` 函数获取第一个页面。
    - 获取文件长度。
    - 调用 `void initialize_leaf_node(void *node)` 函数将这个页面进行初始化。
    - 调用 `void set_node_root(void *node, bool is_root)` 函数将这个页面设置为根页面。
  - 返回表即返回变量 table。
- Pager* pager_open(const char *filename)
  - 假设文件已经被打开，调用 `int open(const char *pathname, int flags)` 函数（O_APPEND）将文件打开。
  - 判断返回的文件描述符是不是等于 -1，如果等于 -1 那么文件打开失败，调用 `int open(const char *pathname, int flags)` 函数（O_CREATE）将文件打开。
  - 判断返回的文件描述符是不是等于 -1，如果等于 -1 那么文件打开失败，则输出提示信息，并退出程序。
  - 调用 ` off_t lseek(int filedes, off_t offset, int whence)` 函数获取文件的长度。
  - 初始化变量 pager，包括文件描述符，文件长度，以及页面的数量。
  - 判断文件长度是否为页面大小的整数倍，如果不是整数倍，输出提示信息，并退出程序。
  - 将所有页面的指针置为空。
- Void *get_page(Pager *pager, uint32_t page_num)
  - 判断页面的序号是不是已经超出了页的个数的范围。如果超出，则输出提示信息，并退出程序。
  - 判断此页面是否已经在内存中，如果不在内存中，则需要从磁盘中读取到内存中。
    - 为对应的页面分配内存。
    - 判断此页面的序号是否小于等于文件中最大的序号。如果是，那么可以从文件中读取出来。
      - 调用 ` off_t lseek(int filedes, off_t offset, int whence)` 函数将当前文件偏移量移动到对应页面所在的位置。
      - 调用 `ssize_t read(int fd, void *buf, size_t count)` 函数读取对应的页面。
      - 如果读取失败，则输出提示信息，并退出程序。
    - 判断读取的页面是否为大于等于当前最大的序号，如果是，将当前序号加一。
  - 返回对应序号的页面。
- void initialize_leaf_node(void *node)
  - 调用 `void set_node_type(void *node, NodeType type)` 函数设置节点的类型为叶子结点。
  - 调用 `void set_node_root(void *node, bool is_root)` 函数将节点设置为非根节点。
  - 调用 `uint32_t *leaf_node_num_cells(void *node)` 函数将节点的键值对个数设置为 0。
  - 调用 `uint32_t* leaf_node_next_leaf(void *node)` 函数设置节点的下一个叶子结点为 0 号节点。



# 元命令

- 当用户输入以 `.` 开头的命令时，表示这是命令是元命令。随后主程序调用  `MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)` 函数。
- MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
  - 将输入的命令与已知的元命令进行比较，如果未找到匹配的命令时，返回`META_COMMAND_UNRECOGNIZED_COMMAND` 。
  - 当用户输入 `.exit` 命令时，调用 `void db_close(Table *table)` 函数，将内存中的数据存储在磁盘中。
  - 当用户输入 `.btree` 时，调用 `void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level)` 函数，输出内存中树的结构。
  - 当用户输入 `.constants` 时，调用 `void print_constants()` 函数，输出数据库的一些基本参数。
- void db_close(Table *table)
  - 循环整个页面，当这个页面不为空时，则表示被读取过因此需要调用 `void pager_flush(Pager *pager, uint32_t page_num)` 将这个页面重新写入磁盘中。
  - 调用 `Int close(int fd)` 函数关闭文件。
- void Pager_flush(Pager *pager, uint32_t page_num)
  - 首先检查传入的页面是否为空页面，如果为空页面则输出提示信息，并退出程序。
  - 调用 ` off_t lseek(int filedes, off_t offset, int whence)` 函数，将当前文件偏移量移动到指定的页面。
  - 判断 offset 是否等于 -1，如果等于 -1 输出提示信息，并退出程序。
  - 随后调用 `ssize_t write(int fd, const void *buf, size_t nbyte)` 函数将内存中的页面写入磁盘中。
  - 判断 bytes_written 是否等于 -1，如果等于 -1 输出提示信息，并退出程序。



# select 命令实现

- 调用 `Cursor *table_start(Table *table)` 函数获取游标。此游标指向的是整个数据库的第一条数据。
- 在一个循环中调用 `void deserialize_row(void *source, Row *destination)` 函数将游标指向的记录提取出来。
- 调用 `void print_row(Row *row)` 函数将记录输出到控制台中。
- 调用 `void cursor_advance(Cursor *cursor)` 函数将游标向后移动一下。
- 判断游标是否已经到达了数据库的末端，如果是则退出循环。
- table_start(Table *table)
  - 调用 `Cursor *table_find(Table *table, uint32_t key)` 函数，来寻找第一条记录。
  - 获取游标对应的页面，获得该页面的元素个数，通过判断该页面的元素个数来判断此游标是否为数据库的末尾。（如果改数据库一个元素都没有的话，意味着获得的游标就是数据库的末尾。）
- Cursor *table_find(Table *table, uint32_t key)
  - 根据 table 变量获得表的根页面。
  - 判断根页面是否为叶子结点，如果是叶子结点，调用 `Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key)` 函数。
  - 否则调用 `Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key)` 函数。
- Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key)
  - 根据 page_num 号码获得页面。根据获得的页面获得当前页面中键值对的个数。
  - 采用二分法搜索获得对应 key 的在页面中的位置。
- Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key)
  - 根据 page_num 号码获得页面。在该页面中找到对应 key 的子结点的编号。
  - 判断该子结点是叶子结点还是内部节点，如果是叶子结点，调用 `Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key)` 函数。
  - 否则调用 `Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key)` 函数。
- void cursor_advance(Cursor *cursor)
  - 将游标中的页面中的 cell_num 加 1。
  - 判断 cell_num 是否已经到达了叶子的末端。如果到了叶子的末端则判断该叶子是否有下一个叶子。
  - 如果有下一个叶子则将游标的编号替换为该叶子的编号，并将 cell_num 置为零。
  - 否则，将游标中的 end_of_table 置为 true 表示已经是数据库的末端了。



# insert 命令实现

- 调用 `void *get_page(Pager* pager, uint32_t page_num)` 函数获得对应 key 所在的页面的编号和 cell_num。
- 如果 cell_num 小于该叶子结点的 num_cells，则意味着有可能出现相同的键，所以调用 `uint32_t *leaf_node_key(void *node, uint32_t cell_num)` 获得该下标的对应的键，如果和插入的键相同，则返回 `EXECUTE_DUPLICATE_KEY。
- 调用 `void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value)` 函数插入新的节点。
- void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value)
  - 根据 cursor 变量获得对应的页面。获得该页面中叶子的数量。
  - 如果该叶子的数量大于等于 `LEAF_NODE_MAX_CELLS`，那么调用函数 `void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value) ` 函数对页面进行拆分，并插入数据。
  - 判断新的数据是否在页面的中间，如果在页面的中间，则需要将其他数据向后挪。
  - 将新的数据插入页面中，并更新页面中 cell 的数量。
- leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value)
  - 调用 `uint32_t get_unused_page_num(Pager *pager)` 函数初始化一个新的节点。
  - 将新节点的父亲置为与旧节点的父亲。将新节点的下一个叶子结点置为旧节点的下一个叶子结点。将旧节点的下一个节点置为新的节点。
  - 随后旧节点的元素分为左右两个部分，左边是旧的节点 key 小的那一部分，右边是旧的节点中 key 大的那一部分。节点的分割是通过是否大于等于 `LEAF_NODE_LEFT_SPLIT_COUNT` 来分割的。
  - 判断被分割的节点是否为根节点。如果是根节点则调用 `void create_new_root(Table *table, uint32_t right_child_page_num)` 函数，返回。
  - 否则调用 `void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key)` 函数将父节点中的旧的 key 改为新的 key。
  - 调用 `void internal_node_insert(Table *table, uint32_t parent_page_num, uint32_t child_page_num)` 将新的节点插入到父节点中。
  - void create_new_root(Table *table, uint32_t right_child_page_num)
    - 分配一个新的做节点，将 root 节点中的值赋给新的节点。
    - 调用 `void initialize_internal_node(void *node)` 函数将 root 节点初始化为内部节点。
    - 并将第一个节点设置为新的左边的叶子结点，key 同理。
    - 将内部节点的最有的节点设置为右边的叶子结点。
    - 将两个叶子结点的父亲设置为 root。