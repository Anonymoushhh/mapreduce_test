import xlrd
import sys
import os
def read_excel():
    # 打开文件 
    workbook1 = xlrd.open_workbook('student.xlsx') 
    workbook2 = xlrd.open_workbook('student_course.xlsx') 

    # 根据sheet索引或者名称获取sheet内容 
    sheet1 = workbook1.sheet_by_index(0) # sheet索引从0开始 
    sheet2 = workbook2.sheet_by_index(0) 
    #写入txt
    f1=open(r"student.csv","ab")
    f2=open(r"student_course.csv","ab")
    for rowNum in range(sheet1.nrows):       
        tmp=""
        for colNum in range(sheet1.ncols):
            if sheet1.cell(rowNum,colNum).value!=None and colNum!=sheet1.ncols-1:
                tmp+=str(sheet1.cell(rowNum ,colNum ).value)+u","
            if sheet1.cell(rowNum,colNum).value!=None and colNum==sheet1.ncols-1:
            	tmp+=str(sheet1.cell(rowNum ,colNum ).value)
        tmp+="\r\n"
        f1.write(tmp.encode('utf8'))

    for rowNum in range(sheet2.nrows):
    	tmp=""
    	for colNum in range(sheet2.ncols):
    		if sheet2.cell(rowNum,colNum).value!=None and colNum!=sheet2.ncols-1:
    			tmp+=str(sheet2.cell(rowNum,colNum).value)+u","
    		if sheet2.cell(rowNum,colNum).value!=None and colNum==sheet2.ncols-1:
    			tmp+=str(sheet2.cell(rowNum,colNum).value)
    	tmp+="\r\n"
    	f2.write(tmp.encode('utf8'))
if __name__ == '__main__': 
    read_excel()
