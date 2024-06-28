import pickle  # 导入pickle模块，用于序列化和反序列化数据

def get_vocab(corpus1, corpus2):
    """
    获取两个语料库中的词汇表。

    Args:
    - corpus1 (list): 第一个语料库，包含多个文档。
    - corpus2 (list): 第二个语料库，包含多个文档。

    Returns:
    - word_vocab (set): 合并后的词汇表集合。
    """
    word_vocab = set()  # 初始化一个空集合，用于存储词汇表
    for corpus in [corpus1, corpus2]:  # 遍历两个语料库
        for i in range(len(corpus)):  # 遍历每个语料库中的文档
            word_vocab.update(corpus[i][1][0])  # 更新词汇表，将第一个部分的词汇添加到集合中
            word_vocab.update(corpus[i][1][1])  # 更新词汇表，将第二个部分的词汇添加到集合中
            word_vocab.update(corpus[i][2][0])  # 更新词汇表，将第三个部分的词汇添加到集合中
            word_vocab.update(corpus[i][3])      # 更新词汇表，将第四个部分的词汇添加到集合中
    print(len(word_vocab))  # 打印词汇表的大小
    return word_vocab  # 返回合并后的词汇表集合


def load_pickle(filename):
    """
    加载pickle文件并返回其中的数据。

    Args:
    - filename (str): pickle文件的路径。

    Returns:
    - data: 从pickle文件中加载的数据。
    """
    with open(filename, 'rb') as f:
        data = pickle.load(f)  # 使用pickle模块加载数据
    return data  # 返回加载的数据


def vocab_processing(filepath1, filepath2, save_path):
    """
    处理词汇表的相关操作，包括加载文件、获取词汇表并保存结果。

    Args:
    - filepath1 (str): 第一个文件路径，包含词汇数据。
    - filepath2 (str): 第二个文件路径，包含词汇数据。
    - save_path (str): 结果保存的文件路径。
    """
    with open(filepath1, 'r') as f:
        total_data1 = set(eval(f.read()))  # 从文件1中读取数据并转换为集合形式
    with open(filepath2, 'r') as f:
        total_data2 = eval(f.read())  # 从文件2中读取数据

    word_set = get_vocab(total_data2, total_data2)  # 获取两个语料库的词汇表

    excluded_words = total_data1.intersection(word_set)  # 找出在总数据1中并且在词汇表中的词汇
    word_set = word_set - excluded_words  # 从词汇表中去除在总数据1中出现的词汇

    print(len(total_data1))  # 打印总数据1的大小
    print(len(word_set))  # 打印处理后的词汇表大小

    with open(save_path, 'w') as f:
        f.write(str(word_set))  # 将处理后的词汇表写入保存路径中


if __name__ == "__main__":
    python_hnn = './data/python_hnn_data_teacher.txt'
    python_staqc = './data/staqc/python_staqc_data.txt'
    python_word_dict = './data/word_dict/python_word_vocab_dict.txt'

    sql_hnn = './data/sql_hnn_data_teacher.txt'
    sql_staqc = './data/staqc/sql_staqc_data.txt'
    sql_word_dict = './data/word_dict/sql_word_vocab_dict.txt'

    new_sql_staqc = './ulabel_data/staqc/sql_staqc_unlabled_data.txt'
    new_sql_large = './ulabel_data/large_corpus/multiple/sql_large_multiple_unlable.txt'
    large_word_dict_sql = './ulabel_data/sql_word_dict.txt'

    vocab_processing(sql_word_dict, new_sql_large, large_word_dict_sql)  # 调用词汇表处理函数处理SQL相关数据