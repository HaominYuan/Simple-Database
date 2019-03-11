- B 树分为两种节点

  | 节点类型 | 名字          |
  | -------- | ------------- |
  | 内部节点 | NODE_INTERNAL |
  | 叶子结点 | NODE_LEAF     |

- 节点的头部

  | 属性         | 名字                | 字段类型 |
  | ------------ | ------------------- | -------- |
  | 节点类型     | NODE_TYPE_SIZE      | uint8_t  |
  | 是否为根节点 | IS_ROOT_SIZE        | uint8_t  |
  | 父节点指针   | PARENT_POINTER_SIZE | uint32_t |

- 叶子结点的头部

  - 将键值对称为 cell。

  | 属性                 | 名字                     | 字段类型 |
  | -------------------- | ------------------------ | -------- |
  | cell 的个数          | LEAF_NODE_NUM_CELLS_SIZE | uint32_t |
  | 下一个叶子结点的编号 | LEAF_NODE_NEXT_LEAF      | uint32_t |

- 叶子结点的内容

  - 叶子的内部是由很多的 cell 构成的。

  | 属性 | 名字                 | 字段类型 |
  | ---- | -------------------- | -------- |
  | 键   | LEAF_NODE_KEY_SIZE   | uint32_t |
  | 值   | LEAF_NODE_VALUE_SIZE | ROW_SIZE |

- 内部节点的头部

  | 属性           | 名字                           | 字段类型 |
  | -------------- | ------------------------------ | -------- |
  | 键的个数       | INTERNAL_NODE_NUM_KEYS_SIZE    | uint32_t |
  | 最右的节点编号 | INTERNAL_NODE_RIGHT_CHILD_SIZE | uint32_t |

- 内部节点的内容

  | 属性         | 名字                     | 字段类型 |
  | ------------ | ------------------------ | -------- |
  | 键           | INTERNAL_NODE_KEYS_SIZE  | uint32_t |
  | 子节点的编号 | INTERNAL_NODE_CHILD_SIZE | uint32_t |