import pandas as pd
import numpy as np
from pandas import DataFrame as df
import matplotlib.pyplot as plt

#matplotlib画图中中文显示会有问题，需要这两行设置默认字体
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

plt.xlabel('C1')    #x坐标名
plt.ylabel('Constitution')  #y坐标名
plt.title('C1课程成绩与体育成绩散点图')    #设置散点图标题
area = np.pi * 1**2  #点的面积大小

df=pd.read_table('sorted.txt',header=None,sep=",") #读取txt文件
x=df[5].values  #x坐标(C1)数组
y=df[15].values #x坐标(C1)数组

#量化Constitution数据
for i in range(y.size):
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
# 画图
plt.scatter(x, y,s=area)
plt.savefig(r'Exp2_01.png', bbox_inches='tight',dpi=300)
plt.show()