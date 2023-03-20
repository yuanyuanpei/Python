# RWS intro please refer to: https://rwslib.readthedocs.io/en/latest/index.html
# SASPy intro please refer to: https://sassoftware.github.io/saspy/

#Impt1: 导入数据接口RWSConnection，用于连接Rave系统
## rwslib(a python library that provides an interface to Rave Web Services)
from rwslib import RWSConnection
##导入数据接口中的抓取FormData模块，用于读取数据集(.csv in default or .xml, begin with a header, end with "\nEOF")
from rwslib.rws_requests.biostats_gateway import FormDataRequest
##输入Authorization User and Password:
Host = 'demo'
Author = 'DataTech'
AuthorPSWD = 'DBD666666'
##创建链接至URL，https://_host_.mdsol.com
rws=RWSConnection(Host,Author,AuthorPSWD)

#Impt2:
import os

#Impt3: 导入pandas模块，用于处理python读进来的.csv格式的数据集
import pandas

#Impt4: 导入SASPy模块，用于集成python和SAS，以运行SAS code
import saspy
##创建连接：
sas=saspy.SASsession()

#Redefine FormDataRequest to return UTF-8 format(.csv in default):
class UTF8FormDataRequest(FormDataRequest):
    def result(self, response):
        #By default return text
        response.encoding="utf-8-sig"
        return response.text

#Step1: 读取数据by form，返回结果为.csv格式的string
RAW_AE=rws.send_request(UTF8FormDataRequest('2021-760-00CH1','PROD','regular','AE',dataset_format='csv'))
RAW_DM=rws.send_request(UTF8FormDataRequest('2021-760-00CH1','PROD','regular','DM',dataset_format='csv'))
RAW_EX=rws.send_request(UTF8FormDataRequest('2021-760-00CH1','PROD','regular','EX',dataset_format='csv'))

#Step2: 去除RAW_XX.csv文件末尾的换行+EOF:"\nEOF"
New_AE=RAW_AE.replace("\nEOF","",-1)
New_DM=RAW_DM.replace("\nEOF","",-1)
New_EX=RAW_EX.replace("\nEOF","",-1)

#Step3: 去除New_XX.csv文件每行末尾的换行符，然后将文件写入新建的.csv(可进文件夹查看）
with open(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\AE.csv','a+',encoding='utf-8') as f1:
    #如果用sascode直接将.csv转为.sas7bdat，则用：f1.write(New_AE)。最终的sas7bdat中变量format全为Char，且freeText的length为实际最长。
    #如果用Step4-6: pandas读取后再转为.sas7bdat则用：f1.write(New_AE.replace("\n","“）。最终的sas7bdat变量为pandas中的格式(实际Num/Char)，且freeText的length为实际最长。
    f1.write(New_AE.replace("\n", ""))
    f1.close()
with open(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\DM.csv','a+',encoding='utf-8') as f2:
    f2.write(New_DM.replace("\n",""))
    f2.close()
with open(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\EX.csv','a+',encoding='utf-8') as f3:
    f3.write(New_EX.replace("\n",""))
    f3.close()

#Step4: 使用pandas模块导人XX.csv文件为pandas DataFrame
pds_AE=pandas.DataFrame(pandas.read_csv(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\AE.csv',header=0))
pds_DM=pandas.DataFrame(pandas.read_csv(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\DM.csv',header=0))
pds_EX=pandas.DataFrame(pandas.read_csv(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\EX.csv',header=0))

#Step5: 创建SAS library， 并将pandas DataFrame转化为SAS数据集
sas.saslib("RAW 'C:\\ShareCache\\Pei Yuanyuan\\_Learning\\python\\Rave_RWS_demo'")
sas_AE=sas.df2sd(pds_AE,table='ae',libref='RAW')
sas_DM=sas.df2sd(pds_DM,table='dm',libref='RAW')
sas_EX=sas.df2sd(pds_EX,table='ex',libref='RAW')

#Step6: 删除XX.csv临时文件
os.remove(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\AE.csv')
os.remove(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\DM.csv')
os.remove(r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\EX.csv')

#Step7: 执行sas code
with open(file=r'C:\ShareCache\Pei Yuanyuan\_Learning\python\Rave_RWS_demo\Run.sas',encoding='utf-8') as sascd:
    code=sascd.read()
    #print(code)
    sas.submit(code)

# from rwslib.rws_requests import VersionRequest
# ver=rws.send_request(VersionRequest())
# print(ver)
#
# tm=rws.request_time
# print(tm)
#
# from rwslib.rws_requests import CodeNameRequest
# cd=rws.send_request(CodeNameRequest())
# print(cd)

#--用来抓取Study的信息：
# from rwslib.rws_requests import ClinicalStudiesRequest
# studies=rws.send_request(ClinicalStudiesRequest())
# print(studies.ODMVersion)
# print(studies.fileoid)
# print(studies.creationdatetime)
# print(len(studies))

# for study in studies:
#     print("OID:",study.oid)
#     print("Name:",study.studyname)
    # print("Protocolname",study.protocolname)
    # print("IsProd?",study.isProd())

#--用来抓取Study中Subject的信息：
# from rwslib.rws_requests import StudySubjectsRequest
# subject_list = rws.send_request(StudySubjectsRequest("2021-760-00CH1","PROD"))
# print(subject_list.ODMVersion)

# for subject in subject_list:
#     print("Name: %s" % subject.subjectkey)
# print(rws.base_url)
# print(str(subject_list))

#--用来抓取Study层面的数据集信息(ODM Data）
# from rwslib.rws_requests import StudyDatasetRequest
# demo=rws.send_request(StudyDatasetRequest("2021-760-00CH1","PROD","regular",formoid="DM"))
# print(demo)

#--用来抓取Subject层面的数据集信息(ODM Data)
# from rwslib.rws_requests import SubjectDatasetRequest
# demo2=rws.send_request(SubjectDatasetRequest("2021-760-00CH1","PROD","01001",formoid="AE"))
# print(demo2)

#--用来抓取CRFVersion层面的数据集信息(ODM Data)
# from rwslib.rws_requests import VersionDatasetRequest
# demo3=rws.send_request(VersionDatasetRequest("2021-760-00CH1","PROD",181,formoid="DM"))
# print(demo3)

#--用来check上一次刷CV的日期
# from rwslib.rws_requests import StudyDatasetRequest
# xml=rws.send_request(StudyDatasetRequest("2021-760-00CH1","PROD"))
# lstdt=rws.last_result.headers['X-MWS-CV-Last-Updated']
# print(lstdt)

