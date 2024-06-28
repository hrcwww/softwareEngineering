import pickle
from collections import Counter

def load_pickle(filename):
    # 加载 pickle 格式的数据
    with open(filename, 'rb') as f:
        data = pickle.load(f, encoding='iso-8859-1')
    return data

def split_data(total_data, qids):
    # 根据问题ID将数据分为单问题和多问题列表
    result = Counter(qids)  # 统计每个问题ID出现的次数
    total_data_single = []
    total_data_multiple = []
    for data in total_data:
        if result[data[0][0]] == 1:
            total_data_single.append(data)  # 只出现一次的问题ID的数据加入单问题列表
        else:
            total_data_multiple.append(data)  # 出现多次的问题ID的数据加入多问题列表
    return total_data_single, total_data_multiple

def data_staqc_processing(filepath, save_single_path, save_multiple_path):
    with open(filepath, 'r') as f:
        total_data = eval(f.read())  # 从文件中读取数据并转换为 Python 对象
    qids = [data[0][0] for data in total_data]  # 获取所有问题的ID
    total_data_single, total_data_multiple = split_data(total_data, qids)  # 根据问题ID分割数据

    # 将单问题数据和多问题数据分别保存到文件中
    with open(save_single_path, "w") as f:
        f.write(str(total_data_single))
    with open(save_multiple_path, "w") as f:
        f.write(str(total_data_multiple))

def data_large_processing(filepath, save_single_path, save_multiple_path):
    total_data = load_pickle(filepath)  # 加载 pickle 格式的数据文件
    qids = [data[0][0] for data in total_data]  # 获取所有问题的ID
    total_data_single, total_data_multiple = split_data(total_data, qids)  # 根据问题ID分割数据

    # 将单问题数据和多问题数据分别保存到 pickle 格式的文件中
    with open(save_single_path, 'wb') as f:
        pickle.dump(total_data_single, f)
    with open(save_multiple_path, 'wb') as f:
        pickle.dump(total_data_multiple, f)

def single_unlabeled_to_labeled(input_path, output_path):
    total_data = load_pickle(input_path)  # 加载 pickle 格式的数据文件
    labels = [[data[0], 1] for data in total_data]  # 给每条数据添加标签，假定标签都为1
    total_data_sort = sorted(labels, key=lambda x: (x[0], x[1]))  # 按问题ID和标签进行排序
    with open(output_path, "w") as f:
        f.write(str(total_data_sort))  # 将排序后的数据保存到文件中

if __name__ == "__main__":
    # 定义各类数据文件的路径和保存路径，并调用相应的处理函数
    staqc_python_path = './ulabel_data/python_staqc_qid2index_blocks_unlabeled.txt'
    staqc_python_single_save = './ulabel_data/staqc/single/python_staqc_single.txt'
    staqc_python_multiple_save = './ulabel_data/staqc/multiple/python_staqc_multiple.txt'
    data_staqc_processing(staqc_python_path, staqc_python_single_save, staqc_python_multiple_save)

    staqc_sql_path = './ulabel_data/sql_staqc_qid2index_blocks_unlabeled.txt'
    staqc_sql_single_save = './ulabel_data/staqc/single/sql_staqc_single.txt'
    staqc_sql_multiple_save = './ulabel_data/staqc/multiple/sql_staqc_multiple.txt'
    data_staqc_processing(staqc_sql_path, staqc_sql_single_save, staqc_sql_multiple_save)

    large_python_path = './ulabel_data/python_codedb_qid2index_blocks_unlabeled.pickle'
    large_python_single_save = './ulabel_data/large_corpus/single/python_large_single.pickle'
    large_python_multiple_save = './ulabel_data/large_corpus/multiple/python_large_multiple.pickle'
    data_large_processing(large_python_path, large_python_single_save, large_python_multiple_save)

    large_sql_path = './ulabel_data/sql_codedb_qid2index_blocks_unlabeled.pickle'
    large_sql_single_save = './ulabel_data/large_corpus/single/sql_large_single.pickle'
    large_sql_multiple_save = './ulabel_data/large_corpus/multiple/sql_large_multiple.pickle'
    data_large_processing(large_sql_path, large_sql_single_save, large_sql_multiple_save)

    large_sql_single_label_save = './ulabel_data/large_corpus/single/sql_large_single_label.txt'
    large_python_single_label_save = './ulabel_data/large_corpus/single/python_large_single_label.txt'
    single_unlabeled_to_labeled(large_sql_single_save, large_sql_single_label_save)
    single_unlabeled_to_labeled(large_python_single_save, large_python_single_label_save)