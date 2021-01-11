import pandas as pd
import numpy as np
from pandas import DataFrame as df
import matplotlib.pyplot as plt

#matplotlib画图中中文显示会有问题，需要这两行设置默认字体
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

df=pd.read_table('sorted.txt',header=None,sep=",") #读取txt文件
x=df[5].values  #读取第6列即C1课程成绩

plt.title('C1课程成绩分布直方图')    #设置直方图标题
plt.xlabel('分数')    #设置横坐标内容
plt.ylabel('人数')    #设置纵坐标内容
plt.xlim(xmax=100,xmin=0)   #设置x轴长度范围
x_ticks = np.arange(0, 105, 5)  #设置横坐标刻度范围为0-100
plt.xticks(x_ticks) #设置横坐标刻度

# 画图
plt.hist( x, # 指定绘图数据
          bins = 20, # 指定直方图中条块的个数
          color = 'steelblue', # 指定直方图的填充色
          edgecolor = 'black') # 指定直方图的边框色
plt.savefig(r'Exp2_02.png', bbox_inches='tight',dpi=300)    #保存图
plt.show()  #显示图