import pickle
import multiprocessing
from python_structured import *  # 导入python结构化处理函数
from sqlang_structured import *  # 导入SQL结构化处理函数

# 并行处理Python语言相关数据集的查询
def multipro_python_query(data_list):
    return [python_query_parse(line) for line in data_list]

# 并行处理Python语言相关数据集的代码解析
def multipro_python_code(data_list):
    return [python_code_parse(line) for line in data_list]

# 并行处理Python语言相关数据集的上下文解析
def multipro_python_context(data_list):
    result = []
    for line in data_list:
        if line == '-10000':
            result.append(['-10000'])
        else:
            result.append(python_context_parse(line))
    return result

# 并行处理SQL语言相关数据集的查询
def multipro_sqlang_query(data_list):
    return [sqlang_query_parse(line) for line in data_list]

# 并行处理SQL语言相关数据集的代码解析
def multipro_sqlang_code(data_list):
    return [sqlang_code_parse(line) for line in data_list]

# 并行处理SQL语言相关数据集的上下文解析
def multipro_sqlang_context(data_list):
    result = []
    for line in data_list:
        if line == '-10000':
            result.append(['-10000'])
        else:
            result.append(sqlang_context_parse(line))
    return result

# 解析数据列表并返回解析结果
def parse(data_list, split_num, context_func, query_func, code_func):
    pool = multiprocessing.Pool()  # 创建多进程池
    split_list = [data_list[i:i + split_num] for i in range(0, len(data_list), split_num)]  # 将数据列表分割成多个子列表
    results = pool.map(context_func, split_list)  # 使用进程池并行处理上下文数据
    context_data = [item for sublist in results for item in sublist]  # 展开结果列表

    results = pool.map(query_func, split_list)  # 使用进程池并行处理查询数据
    query_data = [item for sublist in results for item in sublist]  # 展开结果列表

    results = pool.map(code_func, split_list)  # 使用进程池并行处理代码数据
    code_data = [item for sublist in results for item in sublist]  # 展开结果列表

    pool.close()  # 关闭进程池
    pool.join()  # 等待所有进程结束

    return context_data, query_data, code_data

# 主函数，负责整个数据处理流程
def main(lang_type, split_num, source_path, save_path, context_func, query_func, code_func):
    with open(source_path, 'rb') as f:
        corpus_lis = pickle.load(f)  # 加载数据列表

    # 解析数据并获取上下文、查询和代码解析结果
    context_data, query_data, code_data = parse(corpus_lis, split_num, context_func, query_func, code_func)
    qids = [item[0] for item in corpus_lis]  # 获取每个数据项的qid

    # 将数据按照qid进行整合
    total_data = [[qids[i], context_data[i], code_data[i], query_data[i]] for i in range(len(qids))]

    with open(save_path, 'wb') as f:
        pickle.dump(total_data, f)  # 将整理好的数据保存为pickle文件

if __name__ == '__main__':
    # 设置Python语言相关数据集的路径和保存路径
    staqc_python_path = '.ulabel_data/python_staqc_qid2index_blocks_unlabeled.txt'
    staqc_python_save = '../hnn_process/ulabel_data/staqc/python_staqc_unlabled_data.pkl'

    # 设置SQL语言相关数据集的路径和保存路径
    staqc_sql_path = './ulabel_data/sql_staqc_qid2index_blocks_unlabeled.txt'
    staqc_sql_save = './ulabel_data/staqc/sql_staqc_unlabled_data.pkl'

    # 执行主函数处理Python语言相关数据集
    main(python_type, split_num, staqc_python_path, staqc_python_save, multipro_python_context, multipro_python_query, multipro_python_code)

    # 执行主函数处理SQL语言相关数据集
    main(sqlang_type, split_num, staqc_sql_path, staqc_sql_save, multipro_sqlang_context, multipro_sqlang_query, multipro_sqlang_code)

    # 设置大型Python语言相关数据集的路径和保存路径
    large_python_path = './ulabel_data/large_corpus/multiple/python_large_multiple.pickle'
    large_python_save = '../hnn_process/ulabel_data/large_corpus/multiple/python_large_multiple_unlable.pkl'

    # 设置大型SQL语言相关数据集的路径和保存路径
    large_sql_path = './ulabel_data/large_corpus/multiple/sql_large_multiple.pickle'
    large_sql_save = './ulabel_data/large_corpus/multiple/sql_large_multiple_unlable.pkl'

    # 执行主函数处理大型Python语言相关数据集
    main(python_type, split_num, large_python_path, large_python_save, multipro_python_context, multipro_python_query, multipro_python_code)

    # 执行主函数处理大型SQL语言相关数据集
    main(sqlang_type, split_num, large_sql_path, large_sql_save, multipro_sqlang_context, multipro_sqlang_query, multipro_sqlang_code)