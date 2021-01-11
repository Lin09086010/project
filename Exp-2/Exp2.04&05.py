import pandas as pd
import numpy as np
from pandas import DataFrame as df
import matplotlib.pyplot as plt
import seaborn as sns
import math

np.set_printoptions(suppress=True)#取消科学计数法
plt.rcParams['font.sans-serif']=['SimHei'] #显示中文标签
plt.rcParams['axes.unicode_minus']=False   #这两行需要手动设置

np.seterr(divide='ignore',invalid='ignore')#忽略可能出现0/0从而报错的情况
df=pd.read_table('sorted.txt',index_col=None,header=None,sep=",") #读取txt文件
#量化Constitution数据
def quantization(y):
    for i in range(len(y)):
        if y[i]=='excellent':
            y[i]=100
        elif y[i]=='good':
            y[i]=90
        elif y[i]=='general':
            y[i]=80
        elif y[i]=='bad':
            y[i]=70
        else:
            y[i]=60

#均值
def Mean(y):
    num = 0
    for i in range(len(y)):
        num += y[i]
    return num/len(y)

#标准差
def std(y,u):
    nu=0
    for j in range(len(y)):
        nu+=(y[j]-u)**2
    return  math.sqrt(nu/len(y))

#协方差
def cov(x,y):
    num1=0
    for i in range(len(x)):
        num1+=(x-Mean(x))*(y-Mean(y))
    return num1/len(x)

#相关矩阵
def cMatrix():
    quantization(df[15].values)
    # dic=np.zeros(shape=(10,10))#定义10*10空矩阵
    num=df[0].size
    dic= [[0]*num for _ in range(num)]#初始化矩阵每个值为0,为106x106的矩阵
    for i in range(len(dic)):
        A = [0 for _ in range(11)]#初始化一个长度为11的数组，里面每个元素为0，用来存储i号学生的成绩
        B = [0 for _ in range(11)]#初始化一个长度为11的数组，里面每个元素为0，用来存储j号学生的成绩
        for j in range(len(dic[0])):
            nm = 0
            if(i==j):
                dic[i][j] = 1#如果i==j，说明是自己与自己，相关性为1
            else:
                for k in range(5,16):
                    # 循环存储成绩到A，B数组
                    A[k-5]=df[k].values[i]
                    B[k-5]=df[k].values[j]
                for g in range(5,16):
                    ak=(A[g-5]-Mean(A))/std(A,Mean(A))#相关性公式中第k项
                    bk=(B[g-5]-Mean(B))/std(B,Mean(B))#相关性公式中第k项
                    nm+=ak*bk#累加求相关性
                dic[i][j]=nm#将所求的相关性赋值
    return dic

#绘制混淆矩阵
def visualization(dic):
    # 定义图的参数
    fig,ax= plt.subplots(figsize=(50, 50))
    # 设置热力图参数
    sns.heatmap(np.around(dic,1),
                annot=True, vmax=106, vmin=0, fmt='.1f',xticklabels=True, yticklabels=True, square=True,
                linewidths=2,cmap="YlGnBu",annot_kws={'size':8, 'weight':'light', 'color':'red'})
    ax.set_title('二维数组热力图', fontsize=18)#设置标题
    ax.set_ylabel('数字', fontsize=18)  # x轴
    ax.set_xlabel('字母', fontsize=18)  # 横变成y轴，跟矩阵原始的布局情况是一样的
    plt.savefig(r"混淆矩阵.png") # 保存图
    plt.show()  # 显示图片

#找距离最近三个样本
def output(dic):
    with open('Exp2_05.txt', 'w') as f1:
        for i in range(len(dic)):
            key_dic = {}    # 定义空字典
            for j in range(len(dic[0])):
                key_dic[str(df[0].values[j])] = dic[i][j]# 将矩阵中的值对应ID号存入字典
            key_dic.items()  # 得到类似: dict_items([('a', 1), ('c', 3), ('b', 2)])
            L = list(key_dic.items())  # 得到类似列表: L=[('a', 1), ('c', 3), ('b', 2)]
            L.sort(key=lambda x: x[1], reverse=True)# 进行排序，按照从大到小的顺序，得到的是列表
            for k in range(3):
                # 获取列表中前三个，列表每一个元素有两个子元素，第一个子元素即ID号
                if(k<2):
                    f1.write(str(L[k][0]))# 写入txt文件
                    f1.write("\t")
                else:
                    f1.write(str(L[k][0]))# 写入txt文件
                    f1.write("\n")
    f1.close()

if __name__ == '__main__':
    confusion_matrix=cMatrix()
    visualization(confusion_matrix)
    output(confusion_matrix)