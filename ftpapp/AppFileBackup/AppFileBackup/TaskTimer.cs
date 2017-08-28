using System;
using System.Collections.Generic;
using System.Text;
using System.Timers;
using System.IO;
using System.Data;
using FileTransfer;
using System.Diagnostics;
using System.Collections;
using System.Xml.Serialization;
using System.Globalization;
using Common;

//test ErrRpt edit by zhouyu 2014-1-14
using SendErrMsgDLL;
using FileScheduleEDW;
//end

namespace FileTransfer
{
    public class TaskTimer
    {
        #region "var ����"
        private const string CRLF = "\r\n";
        string TaskId = null;
        string TaskName = null;
        string TaskMode = null;  //S���ϴ���D������
        bool TaskDeleteFolder = false; //ɾ�����ϼ�
        string TaskSourcePath = null;
        string TaskBackupPath = null;
        //string TaskLogPath = null;
        public string TaskLogPath = null;//alter by zhouyu 2014-1-20
        string TaskWay = null;
        string TaskUser = null;
        string TaskPassword = null;
        string TaskServerIP = null;
        string TaskServerPort = null;
        bool TaskFtpPassive = true;
        string TaskDestinationPath = null;

        bool TaskWS = false;
        bool TaskFile = false;
        string TaskOrder = null;
        //string TaskWSIP = null;
        //string TaskWSPort = null;
        string TaskWSClass = null;
        string TaskWSMethod = null;
        string TaskWSParameter = null;
        string TaskWSReturnType = null;

        string TaskFileType = null;

        string TaskTimeType = null;
        DateTime TaskOneDate;
        DateTime TaskOneTime;
        string TaskFrequencyType = null;
        string TaskDayCyType = null;
        DateTime TaskDayCyOneTime;
        int TaskDayCyRepeat = 0;
        string TaskDayCyUnit = null;
        DateTime TaskDayCyBeginTime;
        DateTime TaskDayCyEndTime;
        bool TaskWK1 = false;
        bool TaskWK2 = false;
        bool TaskWK3 = false;
        bool TaskWK4 = false;
        bool TaskWK5 = false;
        bool TaskWK6 = false;
        bool TaskWK7 = false;


        DateTime NowDate = System.DateTime.Today;
        DateTime NowTime = System.DateTime.Now;
        DateTime nextDate = System.DateTime.Today;
        DateTime nextTime = System.DateTime.Now;
        DateTime preDate = System.DateTime.Today;

        public string taskid = null;
        public string ParameterXmlPath = null;
        public string Parameter = string.Empty;

        bool blnfinished = false;
        bool blnExec = true;   //�ж������ȫ�ֳɹ���ʧ��
        bool blnWS = true;    //�ж��Ƿ����WS
        bool TaskChkChk = true; //�ж��Ƿ���CHK�ļ�

        private string strlog = null;
        private string strWs = null;
        private string LogFileName = null;
        string TaskDate = null;
        string strStartLog = null;

        //add by jdb 20081027
        private string loginUser = "";
        //end by jdb 20081027

        //bool blnDs = false;

        #endregion "var"

        public void Run()
        {
            GetFileParameter();
            GetTimerStart();
        }

        /// <summary>
        /// ��ȡFTP�ϵ�Ŀ¼�ļ�
        /// </summary>
        /// <returns></returns>
        public string[] GetFtpFileList()
        {
            GetFileParameter();

            FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
            string errrorMsg = null;
            if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺ͨѶʧ�ܡ�" + '\t' + "0";
                FileControl fw = new FileControl();
                LogFileName = System.DateTime.Now.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + "-" + TaskId + "-List.txt";
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                return null;
            }
            List<string> dLFiles = new List<string>();
            string message = null;
            string[] fi = ftpUpDown.GetFileList(TaskSourcePath, true, TaskFileType, out message);

            return fi;
        }

        /// <summary>
        /// ��ȡ�����ϵ�Ŀ¼�ļ�
        /// </summary>
        /// <returns></returns>
        public string[] GetLocalFileList()
        {
            GetFileParameter();
            FileControl FileOperator = new FileControl();
            string[] files = null;
            if (TaskMode == "S")  // �ϴ�
            {
                files = FileOperator.GetFileList(TaskSourcePath, TaskFileType, true);
            }
            else if (TaskMode == "D")  //����
            {
                files = FileOperator.GetFileList(TaskDestinationPath, TaskFileType, true);
            }

            return files;
        }

        //�ֶ�����ִ��
        public bool ExecManualTask()
        {
            return this.ExecManualTask("");
        }

        //�ֶ�����ִ��
        public bool ExecManualTask(string loginUser)
        {
            GetFileParameter();

            //add by jdb 20081027
            this.loginUser = loginUser;
            //end by jdb 20081027

            TaskDate = System.DateTime.Now.ToShortDateString();
            LogFileName = System.DateTime.Now.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + "-" + TaskId + ".txt";
            blnExec = true;
            strlog = null;
            strStartLog = null;
            //    int intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "��ʼ�����ֶ�����������--" + taskid + "("+TaskName+")");
            strStartLog = TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "��ʼ�����ֶ�����������" + '\t' + CRLF;
            strWs = null;
            SelectExecute();

            //����ִ�н���
            strlog = null;

            if (blnExec == false)
            {
                //        intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "06", "������������������ʧ��--" + taskid + "(" + TaskName + ")");
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ʧ��" + '\t' + "0";
            }
            else
            {
                //        intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "������������������ɹ�--" + taskid + "(" + TaskName + ")");
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ɹ�" + '\t' + "1";

            }
            FileControl fw = new FileControl();
            fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

            return blnExec;
        }

        /// <summary>
        /// ��DataSet �ļ�¼����
        /// </summary>
        /// <returns></returns>
        public bool ExecManualTaskDs(DataSet ds)
        {
            return this.ExecManualTaskDs(ds, "");
        }

        /// <summary>
        /// ��DataSet �ļ�¼����
        /// </summary>
        /// <returns></returns>
        public bool ExecManualTaskDs(DataSet ds, string loginUser)
        {

            GetFileParameter();

            this.loginUser = loginUser;

            TaskDate = System.DateTime.Now.ToShortDateString();
            LogFileName = System.DateTime.Now.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + "-" + TaskId + ".txt";
            blnExec = true;
            strlog = null;
            strStartLog = null;
            strStartLog = TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "��ʼ�����ֶ�����������" + '\t' + CRLF;
            strWs = null;
            SelectExecute(ds);

            //����ִ�н���
            strlog = null;

            if (blnExec == false)
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ʧ��" + '\t' + "0";
            }
            else
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ɹ�" + '\t' + "1";

            }
            FileControl fw = new FileControl();
            fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

            return blnExec;

        }

        /// <summary>
        /// ��DataTable �ļ�¼����
        /// </summary>
        /// <returns></returns>
        public bool ExecManualTaskDt(DataTable dt, string loginUser, ArrayList arlList)
        {

            GetFileParameter();

            this.loginUser = loginUser;

            TaskDate = System.DateTime.Now.ToShortDateString();
            LogFileName = System.DateTime.Now.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + "-" + TaskId + ".txt";
            blnExec = true;
            strlog = null;
            strStartLog = null;
            strStartLog = TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "��ʼ�����ֶ�����������" + '\t' + CRLF;
            strWs = null;
            SelectExecuteDt(dt, arlList);

            //����ִ�н���
            strlog = null;

            if (blnExec == false)
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ʧ��" + '\t' + "0";
            }
            else
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ɹ�" + '\t' + "1";

            }
            FileControl fw = new FileControl();
            fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

            return blnExec;

        }


      

        /// <summary>
        /// �����Ⱥ�ѡ��
        /// </summary>
        public void SelectExecute()
        {
            // Update 2008/6/3 begin ���ftpͨѶ�Ƿ�����ͨѶ
            if (TaskFile == true)
            {
                if (taskid != "M8008" && taskid != "M8009")
                {
                    FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                    string errrorMsg = null;
                    if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺ͨѶʧ�ܡ�" + '\t' + "0";
                        FileControl fw = new FileControl();
                        fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                        blnExec = false;
                        blnfinished = false;
                        return;
                    }

                }
                else
                {
                    SFTPOperation SFTPHelper = new SFTPOperation(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword);

                    string errrorMsg = null;
                    if (SFTPHelper.chkFTPOpen(out errrorMsg) == false)
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺ͨѶʧ�ܡ�" + '\t' + "0";
                        FileControl fw = new FileControl();
                        fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                        blnExec = false;
                        blnfinished = false;
                        return;
                    }
                   
                
                }
            }
            // Update 2008/6/3 end

            //����ws
            if (TaskWS == true)
            {
                if (TaskFile == true)
                {
                    //�ȵ�ws
                    if (TaskOrder == "1")
                    {
                        strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ȵ���WebService,���ļ����䡣" + '\t' + CRLF;
                        if (ExecuteWS() == true)
                        {
                            OnFileTransfer(TaskId);
                        }
                    }
                    //�ȴ��ļ�
                    else if (TaskOrder == "2")
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺���ļ�����,�����WebService��" + '\t' + CRLF;
                        OnFileTransfer(TaskId);
                        if (blnExec == true)
                        {
                            if (blnWS == true)
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ļ����䴦��ɹ�����ʼ����WebService������" + '\t' + CRLF;
                                ExecuteWS();
                            }
                            else
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺������WebService������" + '\t' + CRLF;
                            }
                        }
                        else
                        {
                            strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ļ����䴦��ʧ�ܣ�������WebService������" + '\t' + CRLF;
                            blnExec = false;
                        }
                    }
                }
                else
                {
                    strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺��ʼ����WebService������" + '\t' + CRLF;
                    ExecuteWS();
                }
            }
            else if (TaskFile == true)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺��ʼ�ļ����䡣" + '\t' + CRLF;
                OnFileTransfer(TaskId);

            }
        }


        /// <summary>
        /// �����Ⱥ�ѡ��
        /// </summary>
        public void SelectExecute(DataSet ds)
        {
            // Update 2008/6/3 begin ���ftpͨѶ�Ƿ�����ͨѶ
            if (TaskFile == true)
            {
                FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                string errrorMsg = null;
                if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
                {
                    strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺ͨѶʧ�ܡ�" + '\t' + "0";
                    FileControl fw = new FileControl();
                    fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                    blnExec = false;
                    blnfinished = false;
                    return;
                }
            }
            // Update 2008/6/3 end

            //����ws
            if (TaskWS == true)
            {
                if (TaskFile == true)
                {
                    //�ȵ�ws
                    if (TaskOrder == "1")
                    {
                        strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ȵ���WebService,���ļ����䡣" + '\t' + CRLF;

                        if (ExecuteWS(ds) == true)
                        {
                            OnFileTransfer(TaskId);
                        }

                    }
                    //�ȴ��ļ�
                    else if (TaskOrder == "2")
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺���ļ�����,�����WebService��" + '\t' + CRLF;
                        OnFileTransfer(TaskId);
                        if (blnExec == true)
                        {
                            if (blnWS == true)
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ļ����䴦��ɹ�����ʼ����WebService������" + '\t' + CRLF;
                                ExecuteWS(ds);
                            }
                            else
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺������WebService������" + '\t' + CRLF;
                            }
                        }
                        else
                        {
                            strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ļ����䴦��ʧ�ܣ�������WebService������" + '\t' + CRLF;
                            blnExec = false;
                        }
                    }
                }
                else
                {
                    strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺��ʼ����WebService������" + '\t' + CRLF;
                    ExecuteWS(ds);
                }
            }
            else if (TaskFile == true)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺��ʼ�ļ����䡣" + '\t' + CRLF;
                OnFileTransfer(TaskId);

            }
        }

        /// <summary>
        /// �����Ⱥ�ѡ��
        /// </summary>
        public void SelectExecuteDt(DataTable dt, ArrayList arlList)
        {
            // Update 2008/6/3 begin ���ftpͨѶ�Ƿ�����ͨѶ
            if (TaskFile == true)
            {
                FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                string errrorMsg = null;
                if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
                {
                    strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺ͨѶʧ�ܡ�" + '\t' + "0";
                    FileControl fw = new FileControl();
                    fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                    blnExec = false;
                    blnfinished = false;
                    return;
                }
            }
            // Update 2008/6/3 end

            //����ws
            if (TaskWS == true)
            {
                if (TaskFile == true)
                {
                    //�ȵ�ws
                    if (TaskOrder == "1")
                    {
                        strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ȵ���WebService,���ļ����䡣" + '\t' + CRLF;

                        if (ExecuteWSDt(dt, arlList) == true)
                        {
                            OnFileTransfer(TaskId);
                        }

                    }
                    //�ȴ��ļ�
                    else if (TaskOrder == "2")
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺���ļ�����,�����WebService��" + '\t' + CRLF;
                        OnFileTransfer(TaskId);
                        if (blnExec == true)
                        {
                            if (blnWS == true)
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ļ����䴦��ɹ�����ʼ����WebService������" + '\t' + CRLF;
                                ExecuteWSDt(dt, arlList);
                            }
                            else
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺������WebService������" + '\t' + CRLF;
                            }
                        }
                        else
                        {
                            strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ļ����䴦��ʧ�ܣ�������WebService������" + '\t' + CRLF;
                            blnExec = false;
                        }
                    }
                }
                else
                {
                    strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺��ʼ����WebService������" + '\t' + CRLF;
                    ExecuteWSDt(dt, arlList);
                }
            }
            else if (TaskFile == true)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺��ʼ�ļ����䡣" + '\t' + CRLF;
                OnFileTransfer(TaskId);

            }
        }

        /// <summary>
        /// �趨���ݼ��Timer����
        /// </summary>
        internal void GetTimerStart()
        {
            //����ʱ���ʱ��ִ���¼���
            Timer t = new Timer(6 * 10000);
            t.Elapsed += new ElapsedEventHandler(TaskTimer_Elapsed);
            t.AutoReset = true;
            t.Enabled = true;
            t.Start();
        }

        /// <summary>
        /// timer�¼�
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>

        private void TaskTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            //add by xsq 20110929 ����ִ�м��
            FileControl fw = new FileControl();

            //end by xsq 20110929
            try
            {

                ////ʱ��ıȶ�          
                DateTime dtNow = System.DateTime.Now;
                if (checkNextTime(dtNow) == true)
                {

                    //дִ��ʱ�����־
                    string slog = "�����--" + TaskId + " ִ�е�ʱ�䣺" + dtNow.Date.ToString() + '\t' + dtNow.ToShortTimeString();
                    //FileControl fw = new FileControl();
                    fw.WriteFile(slog, TaskLogPath, "�Զ�ִ��-" + TaskId + "-" + dtNow.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + ".txt");

                    slog = string.Empty;

                    if (blnfinished == false)
                    {
                        blnfinished = true;
                        TaskDate = dtNow.ToShortDateString();
                        LogFileName = dtNow.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + "-" + TaskId + ".txt";

                        //bool blnReturn = UI.PROXY<bool>.CallService("BLCommon", "LockTask", taskid, "1", loginUser);
                        //if (blnReturn == false)
                        //{
                        //    fw.WriteFileLog("��������", TaskLogPath, LogFileName);
                        //    return;
                        //}

                        blnExec = true;
                        strlog = null;
                        strWs = null;
                        strStartLog = null;
                        //     int intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "��ʼ��������ʱ��������--" + taskid + "(" + TaskName + ")");
                        strStartLog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + dtNow.ToString() + '\t' + "��ʼ��������ʱ��������, ִ����������:" + '\t' + CRLF;
                        SelectExecute();

                        //����ִ�н���
                        strlog = null;
                        if (blnExec == false)
                        {
                            //          intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "06", "������������������ʧ��--" + taskid + "(" + TaskName + ")");
                            strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ʧ��" + '\t' + "0";
                        }
                        else
                        {
                            //         intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "������������������ɹ�--" + taskid + "(" + TaskName + ")");
                            strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ɹ�" + '\t' + "1";

                        }

                        ////add by xsq 20110929 �ͷ�����
                        //bool blnLock = UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                        ////end by xsq 20110929 �ͷ�����

                        //slog = "�����--" + TaskId + " �ͷ�����" + blnLock.ToString();
                        fw = new FileControl();
                        fw.WriteFile(slog, TaskLogPath, "�Զ�ִ��-" + TaskId + "-" + dtNow.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + ".txt");


                        //strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������" + (blnExec == false ? "ʧ��" : "�ɹ�") + "\t";
                        // FileControl fw = new FileControl();
                        fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

                        #region �ɹ��󷢳�������Ϣ
                        //add by zhouyu 2014-1-14;begin
                        try
                        {
                            string strSysType = "";
                            string strResult = "0";
                            //add by zhouyu 2014-16;begin
                            //�ؼ�����
                            DataTable dtTaskListA = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9101", "01");
                            //�賿����
                            DataTable dtTaskListB = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9101", "02");
                            //add by zhouyu 2014-16;end
                            if (blnExec == false)
                            {
                                strResult = "0";
                            }
                            else
                            {
                                strResult = "1";
                            }

                            if (string.IsNullOrEmpty(strSysType))
                            {
                                strSysType = "MA";
                            }
                            try
                            {
                                //ȡ��Ҫ��ر���������ż�Ҫ��ر����Ĵ���״̬��0 ʧ�� 1�ɹ��� 
                                DataTable dtTaskList = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9100");

                                foreach (DataRow drTaskList in dtTaskList.Rows)
                                {
                                    if (drTaskList["TSM_SUB_VALUE"].ToString().Contains(taskid))
                                    {
                                        if (drTaskList["TSM_SUB_VALUE2"].ToString().Contains(strResult))
                                        {
                                            //add by zhouyu 2014-1-16;begin
                                            this.SetSendMessage(dtTaskListA, dtTaskListB, strSysType, taskid, strResult);

                                        }
                                        else if (dtTaskListA.Rows[0]["TSM_SUB_VALUE"].ToString().Contains(taskid) && strResult.Equals("1"))
                                        {
                                            DataTable dtMessageErr = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetMessageErr");
                                            foreach (DataRow drMessageErr in dtMessageErr.Rows)
                                            {
                                                //����־��Ϊ1
                                                int flag = UI.PROXY<int>.CallService("M07F0904_FileAccept", "UpdateMessageErr", drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_LOG_DATE"].ToString());
                                                SendErrMsg objSendErrMsg = new SendErrMsg();
                                                //"���", "ϵͳ���", "�����", "�Ƿ�ɹ�(0 ʧ�� 1 �ɹ� 2 ����ִ����)", "��Ϣ���"
                                                string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_RESULT"].ToString(), drMessageErr["MER_RESULT"].ToString());
                                                fw.WriteFileLog("���ͱ�����Ϣ��" + strSysType + "ϵͳ" + taskid + "���� " + strResult + strTest + "", TaskLogPath, LogFileName);//������LOG 20120921                                           
                                            }
                                        }
                                        //add by zhouyu 2014-1-16;end
                                    }
                                }
                                //end
                            }
                            catch (Exception exCeption)
                            {
                                //fw.WriteFileLog("���������쳣��" + exCeption + "", TaskLogPath, LogFileName);//������LOG 20120921

                                this.SetSendMessage(dtTaskListA, dtTaskListB, strSysType, taskid, strResult);//add by zhouyu 2014-1-16

                                //add by zhouyu 2014-1-16;end
                                //SendErrMsg objSendErrMsg = new SendErrMsg();
                                ////"���", "ϵͳ���", "�����", "�Ƿ�ɹ�(0 ʧ�� 1 �ɹ� 2 ����ִ����)", "��Ϣ���" 
                                //string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult); 
                                //fw.WriteFileLog("���ͱ�����Ϣ��" + strSysType + "ϵͳ" + taskid + "���� " + strResult + strTest + "", TaskLogPath, LogFileName);//������LOG 20120921
                            }
                        }
                        catch
                        {

                        }
                        //add by zhouyu 2014-1-14;end
                        #endregion

                        blnfinished = false;

                    }
                }
                else
                {
                    string slog = "�����--" + TaskId + " ���õ�ʱ�䣺" + dtNow.Date.ToString() + '\t' + dtNow.ToShortTimeString();
                    // FileOperate.WriteFile(slog, @"C:\SLOG", "����-"+TaskId + ".txt");     
                }
            }
            catch (Exception ex)
            {
                strlog = null;
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "������������������ʧ��(�Զ���ʱ�쳣:" + ex.Message + ")" + '\t' + "0";
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

                #region ʧ�ܺ󷢳�������Ϣ
                //add by zhouyu 2014-1-14;begin
                try
                {
                    string strSysType = "";
                    string strResult = "0";
                    //add by zhouyu 2014-1-16;begin
                    //�ؼ�����
                    DataTable dtTaskListA = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9101", "01");
                    //�賿����
                    DataTable dtTaskListB = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9101", "02");
                    //add by zhouyu 2014-1-16;end
                    if (string.IsNullOrEmpty(strSysType))
                    {
                        strSysType = "MA";
                    }
                    try
                    {
                        //ȡ��Ҫ��ر���������ż�Ҫ��ر����Ĵ���״̬��0 ʧ�� 1�ɹ��� 
                        DataTable dtTaskList = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaser", "9100");

                        foreach (DataRow drTaskList in dtTaskList.Rows)
                        {
                            if (drTaskList["TSM_SUB_VALUE"].ToString().Contains(taskid))
                            {
                                if (drTaskList["TSM_SUB_VALUE2"].ToString().Contains(strResult))
                                {
                                    this.SetSendMessage(dtTaskListA, dtTaskListB, strSysType, taskid, strResult);
                                    //SendErrMsg objSendErrMsg = new SendErrMsg();
                                    // //"���", "ϵͳ���", "�����", "�Ƿ�ɹ�(0 ʧ�� 1 �ɹ� 2 ����ִ����)", "��Ϣ���"
                                    // string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult);
                                }
                            }
                        }
                        //end
                    }
                    catch (Exception exCeption)
                    {
                        //fw.WriteFileLog("���������쳣��" + exCeption + "", TaskLogPath, LogFileName);//������LOG 20120921

                        this.SetSendMessage(dtTaskListA, dtTaskListB, strSysType, taskid, strResult); //add by zhouyu 2014-1-16;

                        //SendErrMsg objSendErrMsg = new SendErrMsg();
                        ////"���", "ϵͳ���", "�����", "�Ƿ�ɹ�(0 ʧ�� 1 �ɹ� 2 ����ִ����)", "��Ϣ���" 
                        //string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult);  
                        //fw.WriteFileLog("���ͱ�����Ϣ��" + strSysType + "ϵͳ" + taskid + "���� " + strResult + strTest + "", TaskLogPath, LogFileName);//������LOG 20120921
                    }
                }
                catch
                {

                }
                //add by zhouyu 2014-1-14;end
                #endregion
                //add by lwj 2012-6-6
                blnfinished = false;
            }


        }

        /// <summary>
        /// ��ȡ����Ĳ��� �Ӳ��������ļ�FilesTransfer.xml�ж�ȡ����Ĳ���
        /// </summary>
        public void GetFileParameter()
        {
            DataSet ds = new DataSet();
            ds = XmlDatasetConvert.ConvertXMLFileToDataSet(ParameterXmlPath);
            DataRow[] trow = ds.Tables[0].Select("taskid ='" + taskid + "'");
            foreach (DataRow drow in trow)
            {
                TaskTimer tm = new TaskTimer();
                TaskId = drow["TaskID"].ToString();
                TaskName = drow["TaskName"].ToString();
                TaskWS = (drow["TaskWS"] == DBNull.Value ? false : (bool)drow["TaskWS"]);
                TaskFile = (drow["TaskFile"] == DBNull.Value ? false : (bool)drow["TaskFile"]);
                TaskOrder = drow["TaskOrder"].ToString();
                TaskWSClass = drow["TaskWSClass"].ToString();
                TaskWSMethod = drow["TaskWSMethod"].ToString();

                TaskWSReturnType = drow["TaskWSReturnType"].ToString();
                TaskLogPath = @"" + drow["TaskLogPath"].ToString();
                TaskChkChk = (drow["TaskChkChk"] == DBNull.Value ? false : (bool)drow["TaskChkChk"]);

                if (drow["TaskWSParameter"].ToString().Length > 0)
                {
                    TaskWSParameter = (TaskChkChk ? "yes," : "no,") + drow["TaskWSParameter"].ToString();
                }
                else
                {
                    TaskWSParameter = (TaskChkChk ? "yes" : "no");
                }


                //add by jdb 20081027
                this.loginUser = (drow["LoginUser"] == DBNull.Value ? "" : drow["LoginUser"].ToString());
                //end by jdb 20081027

                if (TaskFile == true)  //�ļ�����Ĳ�����ȡ
                {
                    TaskFileType = drow["TaskFileType"].ToString();
                    TaskDeleteFolder = (drow["TaskDeleteFolder"] == DBNull.Value ? false : (bool)drow["TaskDeleteFolder"]);
                    TaskSourcePath = @"" + drow["TaskSourcePath"].ToString();
                    TaskBackupPath = @"" + drow["TaskBackupPath"].ToString();

                    if (TaskLogPath.Substring(TaskLogPath.Length - 1) != "\\")
                    { TaskLogPath = TaskLogPath + "\\"; }
                    TaskMode = drow["TaskMode"].ToString();
                    TaskWay = drow["TaskWay"].ToString();
                    if (TaskWay == "F")
                    {
                        TaskUser = drow["TaskUser"].ToString();
                        TaskPassword = Common.Encrypt.DecrypTo(drow["TaskPassword"].ToString());
                        TaskFtpPassive = (drow["TaskFtpPassive"] == DBNull.Value ? false : (bool)drow["TaskFtpPassive"]);
                    }
                    TaskServerIP = drow["TaskServerIP"].ToString();
                    TaskServerPort = drow["TaskServerPort"].ToString();

                    TaskDestinationPath = @"" + drow["TaskDestinationPath"].ToString();

                    if (TaskMode == "D")
                    {
                        if (TaskWay == "F")
                        {
                            if (TaskSourcePath.Substring(TaskSourcePath.Length - 1) != "/")
                            {
                                TaskSourcePath = TaskSourcePath + "/";
                            }
                            //Ftp����·��
                            if (TaskBackupPath.Substring(TaskBackupPath.Length - 1) != "/")
                            { TaskBackupPath = TaskBackupPath + "/"; }
                        }
                        else
                        {
                            if (TaskSourcePath.Substring(TaskSourcePath.Length - 1) != @"\")
                            {
                                TaskSourcePath = TaskSourcePath + @"\";
                            }
                            //���ر���·��
                            if (TaskBackupPath.Substring(TaskBackupPath.Length - 1) != "\\")
                            { TaskBackupPath = TaskBackupPath + "\\"; }
                        }
                        if (TaskDestinationPath.Substring(TaskDestinationPath.Length - 1) != @"\")
                        {
                            TaskDestinationPath = TaskDestinationPath + @"\";
                        }
                    }
                    else if (TaskMode == "S")
                    {
                        if (TaskWay == "F")
                        {
                            if (TaskDestinationPath.Substring(TaskDestinationPath.Length - 1) != "/")
                            {
                                TaskDestinationPath = TaskDestinationPath + "/";
                            }
                        }
                        else
                        {
                            if (TaskDestinationPath.Substring(TaskDestinationPath.Length - 1) != @"\")
                            {
                                TaskDestinationPath = TaskDestinationPath + @"\";
                            }
                        }
                        if (TaskSourcePath.Substring(TaskSourcePath.Length - 1) != @"\")
                        {
                            TaskSourcePath = TaskSourcePath + @"\";
                        }
                    }
                }

                TaskTimeType = drow["TaskTimeType"].ToString();

                if (TaskTimeType == "1")
                {
                    TaskOneDate = (DateTime)drow["TaskOneDate"];
                    TaskOneTime = (DateTime)drow["TaskOneTime"];
                }
                else if (TaskTimeType == "2")
                {
                    TaskFrequencyType = drow["TaskFrequencyType"].ToString();
                    TaskDayCyType = drow["TaskDayCyType"].ToString();

                    if (TaskFrequencyType == "D")   //ÿ��ִ�в���ֵ
                    {

                        if (TaskDayCyType == "1") //ִ��һ�ε�ʱ��
                        {
                            TaskDayCyOneTime = (DateTime)drow["TaskDayCyOneTime"];
                        }
                        else if (TaskDayCyType == "2")  //����ִ�е����ڼ��ʱ��
                        {
                            TaskDayCyRepeat = (int)drow["TaskDayCyRepeat"];
                            TaskDayCyUnit = drow["TaskDayCyUnit"].ToString();
                            TaskDayCyBeginTime = (DateTime)drow["TaskDayCyBeginTime"];
                            TaskDayCyEndTime = (DateTime)drow["TaskDayCyEndTime"];
                        }

                    }
                    else if (TaskFrequencyType == "W")  //ÿ��ִ�в���ֵ
                    {
                        TaskWK1 = (drow["TaskWK1"] == DBNull.Value ? false : (bool)drow["TaskWK1"]);
                        TaskWK2 = (drow["TaskWK2"] == DBNull.Value ? false : (bool)drow["TaskWK2"]);
                        TaskWK3 = (drow["TaskWK3"] == DBNull.Value ? false : (bool)drow["TaskWK3"]);
                        TaskWK4 = (drow["TaskWK4"] == DBNull.Value ? false : (bool)drow["TaskWK4"]);
                        TaskWK5 = (drow["TaskWK5"] == DBNull.Value ? false : (bool)drow["TaskWK5"]);
                        TaskWK6 = (drow["TaskWK6"] == DBNull.Value ? false : (bool)drow["TaskWK6"]);
                        TaskWK7 = (drow["TaskWK7"] == DBNull.Value ? false : (bool)drow["TaskWK7"]);

                        if (TaskDayCyType == "1") //ִ��һ�ε�ʱ��
                        {
                            TaskDayCyOneTime = (DateTime)drow["TaskDayCyOneTime"];
                        }
                        else if (TaskDayCyType == "2")  //����ִ�е����ڼ��ʱ��
                        {
                            TaskDayCyRepeat = (int)drow["TaskDayCyRepeat"];
                            TaskDayCyUnit = drow["TaskDayCyUnit"].ToString();
                            TaskDayCyBeginTime = (DateTime)drow["TaskDayCyBeginTime"];
                            TaskDayCyEndTime = (DateTime)drow["TaskDayCyEndTime"];
                        }
                    }
                }
            }

        }

        /// <summary>
        /// ִ�е�ʱ��ıȽ�
        /// </summary>
        /// <param name="dtNow"></param>
        /// <returns></returns>
        private bool checkNextTime(DateTime dtNow)
        {
            bool flag = false;
            if (TaskTimeType == "1")     //ִ��һ�ε�ʱ��
            {
                if (dtNow.Date.CompareTo(TaskOneDate.Date) == 0 & dtNow.ToShortTimeString().CompareTo(TaskOneTime.ToShortTimeString()) == 0)
                {
                    NowDate = TaskOneDate;
                    NowTime = TaskOneTime;
                    flag = true;
                }
            }
            else if (TaskTimeType == "2")����//����ִ��ʱ��
            {

                if (TaskFrequencyType == "D")   //ÿ��ִ�в���ֵ
                {
                    NowDate = dtNow.Date;
                    if (TaskDayCyType == "1") //ִ��һ�ε�ʱ��
                    {
                        nextDate = NowDate.AddDays(1);
                        if (dtNow.ToShortTimeString().CompareTo(TaskDayCyOneTime.ToShortTimeString()) == 0)
                        {
                            NowTime = TaskDayCyOneTime;
                            flag = true;
                        }
                    }
                    else if (TaskDayCyType == "2")  //����ִ�е����ڼ��ʱ��
                    {
                        DateTime startDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyBeginTime.ToShortTimeString());
                        DateTime endDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyEndTime.ToShortTimeString());
                        DateTime Nowtime = Convert.ToDateTime(dtNow.Year + "-" + dtNow.Month + "-" + dtNow.Day + " " + dtNow.ToShortTimeString());

                        if (startDate > endDate) ����//����ʱ��
                        {
                            endDate = endDate.AddHours(24);

                            if (preDate.Day != dtNow.Day & Nowtime <= endDate)
                            {
                                preDate = dtNow.AddDays(-1);
                                startDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyBeginTime.ToShortTimeString());
                            }
                            else
                            { preDate = dtNow.Date; }

                        }
                        else
                        { preDate = dtNow.Date; }

                        if (Nowtime >= startDate & Nowtime <= endDate)
                        {
                            TimeSpan ts = endDate - startDate;
                            if (TaskDayCyUnit == "H")
                            {
                                double diffHours = ts.TotalHours;
                                DateTime nextdatetime = startDate;
                                for (int i = 0; i <= diffHours; i++)
                                {
                                    if (nextdatetime.ToShortTimeString().CompareTo(dtNow.ToShortTimeString()) == 0)
                                    {
                                        flag = true;
                                        break;
                                    }
                                    else
                                    { nextdatetime = nextdatetime.AddHours(TaskDayCyRepeat); }

                                }
                            }
                            else if (TaskDayCyUnit == "M")
                            {
                                double diffMinutes = ts.TotalMinutes;
                                DateTime nextdatetime = startDate;
                                for (int i = 0; i <= diffMinutes; i++)
                                {
                                    if (nextdatetime.ToShortTimeString().CompareTo(dtNow.ToShortTimeString()) == 0)
                                    {
                                        flag = true;
                                        break;
                                    }
                                    else
                                    { nextdatetime = nextdatetime.AddMinutes(TaskDayCyRepeat); }
                                }
                            }
                        }
                    }
                }
                else if (TaskFrequencyType == "W")  //ÿ��ִ�в���ֵ
                {
                    bool blnWK = false;

                    DateTime startDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyBeginTime.ToShortTimeString());
                    DateTime endDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyEndTime.ToShortTimeString());
                    DateTime Nowtime = Convert.ToDateTime(dtNow.Year + "-" + dtNow.Month + "-" + dtNow.Day + " " + dtNow.ToShortTimeString());
                    if (TaskDayCyType == "2")
                    {
                        if (startDate > endDate) ����//����ʱ��
                        {
                            endDate = endDate.AddHours(24);

                            if (preDate.Day != dtNow.Day & Nowtime <= endDate)
                            {
                                preDate = dtNow.AddDays(-1);
                                startDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyBeginTime.ToShortTimeString());
                            }
                            else
                            { preDate = dtNow.Date; }
                        }
                        else
                        { preDate = dtNow.Date; }
                    }
                    else
                    { preDate = dtNow.Date; }

                    string dow = Convert.ToInt16(preDate.DayOfWeek).ToString();
                    switch (dow)
                    {
                        case "1":
                            if (TaskWK1 == true)
                            {
                                blnWK = true;
                            }
                            break;
                        case "2":
                            if (TaskWK2 == true)
                            {
                                blnWK = true;
                            }
                            break;
                        case "3":
                            if (TaskWK3 == true)
                            {
                                blnWK = true;
                            }
                            break;
                        case "4":
                            if (TaskWK4 == true)
                            {
                                blnWK = true;
                            }
                            break;
                        case "5":
                            if (TaskWK5 == true)
                            {
                                blnWK = true;
                            }
                            break;
                        case "6":
                            if (TaskWK6 == true)
                            {
                                blnWK = true;
                            }
                            break;
                        case "0":
                            if (TaskWK7 == true)
                            {
                                blnWK = true;
                            }
                            break;
                    }
                    if (blnWK == true)
                    {
                        NowDate = dtNow.Date;
                        if (TaskDayCyType == "1") //ִ��һ�ε�ʱ��
                        {
                            nextDate = NowDate.AddDays(1);
                            if (dtNow.ToShortTimeString().CompareTo(TaskDayCyOneTime.ToShortTimeString()) == 0)
                            {
                                NowTime = TaskDayCyOneTime;
                                flag = true;
                            }
                        }
                        else if (TaskDayCyType == "2")  //����ִ�е����ڼ��ʱ��
                        {
                            if (Nowtime >= startDate & Nowtime <= endDate)
                            {
                                TimeSpan ts = endDate - startDate;
                                if (TaskDayCyUnit == "H")
                                {
                                    double diffHours = ts.TotalHours;
                                    DateTime nextdatetime = startDate;
                                    for (int i = 0; i <= diffHours; i++)
                                    {
                                        if (nextdatetime.ToShortTimeString().CompareTo(dtNow.ToShortTimeString()) == 0)
                                        {
                                            flag = true;
                                            break;
                                        }
                                        else
                                        { nextdatetime = nextdatetime.AddHours(TaskDayCyRepeat); }

                                    }
                                }
                                else if (TaskDayCyUnit == "M")
                                {
                                    double diffMinutes = ts.TotalMinutes;
                                    DateTime nextdatetime = startDate;
                                    for (int i = 0; i <= diffMinutes; i++)
                                    {
                                        if (nextdatetime.ToShortTimeString().CompareTo(dtNow.ToShortTimeString()) == 0)
                                        {
                                            flag = true;
                                            break;
                                        }
                                        else
                                        { nextdatetime = nextdatetime.AddMinutes(TaskDayCyRepeat); }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return flag;
        }

        /// <summary>
        /// ws����
        /// </summary>
        /// <returns></returns>
        private bool ExecuteWS()
        {
            bool flag = false;
            try
            {
                object obj;

                //by xieming edit
                string chkchkfile;
                //if (TaskChkChk.ToString() == "True")
                //{
                //    chkchkfile = "yes";
                //}
                //else
                //{
                //    chkchkfile = "no";
                //}

                //if (TaskWSParameter != "" && TaskWSParameter != null && TaskWSParameter != "" && TaskWSParameter != string.Empty)
                //{
                //    TaskWSParameter = chkchkfile + "," + TaskWSParameter.ToString();
                //    //if (string.IsNullOrEmpty(Parameter) || Parameter == "")
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + TaskWSParameter.ToString();
                //    //}
                //    //else
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + Parameter + "," + TaskWSParameter.ToString();
                //    //}
                //}
                //else
                //{
                //    TaskWSParameter = chkchkfile;
                //    //if (string.IsNullOrEmpty(Parameter) || Parameter == "")
                //    //{
                //    //    TaskWSParameter = chkchkfile;
                //    //}
                //    //else
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + Parameter;
                //    //}
                //}

                //by xieming edit

                string[] ParaTemp = TaskWSParameter.Split(',');
                if (TaskWSParameter != "" && TaskWSParameter != null && TaskWSParameter != "" && TaskWSParameter != string.Empty)
                {
                    obj = UI.PROXY<object>.CallService(TaskWSClass, TaskWSMethod, this.loginUser, ParaTemp);

                    if (TaskWSReturnType.ToLower() == "bool")
                    {
                        flag = (bool)obj;
                    }
                }
                else
                {
                    obj = UI.PROXY<object>.CallService(TaskWSClass, TaskWSMethod, this.loginUser);

                    if (TaskWSReturnType.ToLower() == "bool")
                    {
                        flag = (bool)obj;
                    }
                }

                if (flag == true)
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ��WebService��������ɹ�." + '\t';// +CRLF;
                }
                else
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ��WebService��������ʧ��." + '\t' + "0"; //+ CRLF;
                    blnExec = false;
                }

                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);

                return flag;
            }
            catch (Exception ex)
            {
                strWs = strWs + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ������WebService���������쳣:" + ex.Message + '\t' + "0" + CRLF;
                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //add by xsq 20110929 �ͷ�����
                //UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                //end by xsq 20110929 �ͷ�����
                return false;
            }

        }


        /// <summary>
        /// ws����
        /// </summary>
        /// <returns></returns>
        private bool ExecuteWS(DataSet ds)
        {
            bool flag = false;
            try
            {
                object obj;

                string chkchkfile;
                //if (TaskChkChk.ToString() == "True")
                //{
                //    chkchkfile = "yes";
                //}
                //else
                //{
                //    chkchkfile = "no";
                //}

                //if (TaskWSParameter != "" && TaskWSParameter != null && TaskWSParameter != "" && TaskWSParameter != string.Empty)
                //{
                //    TaskWSParameter = chkchkfile + "," + TaskWSParameter.ToString();
                //    //if (string.IsNullOrEmpty(Parameter) || Parameter == "")
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + TaskWSParameter.ToString();
                //    //}
                //    //else
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + Parameter + "," + TaskWSParameter.ToString();
                //    //}
                //}
                //else
                //{
                //    TaskWSParameter = chkchkfile;
                //    //if (string.IsNullOrEmpty(Parameter) || Parameter == "")
                //    //{
                //    //    TaskWSParameter = chkchkfile;
                //    //}
                //    //else
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + Parameter;
                //    //}
                //}

                //by xieming edit

                string[] ParaTemp = TaskWSParameter.Split(',');
                if (TaskWSParameter != "" && TaskWSParameter != null && TaskWSParameter != "" && TaskWSParameter != string.Empty)
                {

                    obj = UI.PROXY<object>.CallService(TaskWSClass, TaskWSMethod, this.loginUser, ParaTemp, ds);

                    if (TaskWSReturnType.ToLower() == "bool")
                    {
                        flag = (bool)obj;
                    }
                }
                else
                {

                    obj = UI.PROXY<object>.CallService(TaskWSClass, TaskWSMethod, this.loginUser, ds);

                    if (TaskWSReturnType.ToLower() == "bool")
                    {
                        flag = (bool)obj;
                    }
                }

                if (flag == true)
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ��WebService��������ɹ�." + '\t';// +CRLF;
                }
                else
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ��WebService��������ʧ��." + '\t' + "0"; //+ CRLF;
                    blnExec = false;
                }

                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                return flag;
            }
            catch (Exception ex)
            {
                strWs = strWs + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ������WebService���������쳣:" + ex.Message + '\t' + "0" + CRLF;
                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //�ͷ�LOCK TASK
                blnExec = false;
                //UI.PROXY<object>.CallService("BLCommon", "ReleaseLockTask", taskid, this.loginUser);

                return false;
            }

        }

        /// <summary>
        /// ws����
        /// </summary>
        /// <returns></returns>
        private bool ExecuteWSDt(DataTable dt, ArrayList arlList)
        {
            bool flag = false;
            try
            {
                object obj;

                string chkchkfile;
                //if (TaskChkChk.ToString() == "True")
                //{
                //    chkchkfile = "yes";
                //}
                //else
                //{
                //    chkchkfile = "no";
                //}

                //if (TaskWSParameter != "" && TaskWSParameter != null && TaskWSParameter != "" && TaskWSParameter != string.Empty)
                //{
                //    TaskWSParameter = chkchkfile + "," + TaskWSParameter.ToString();
                //    //if (string.IsNullOrEmpty(Parameter) || Parameter == "")
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + TaskWSParameter.ToString();
                //    //}
                //    //else
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + Parameter + "," + TaskWSParameter.ToString();
                //    //}
                //}
                //else
                //{
                //    TaskWSParameter = chkchkfile;
                //    //if (string.IsNullOrEmpty(Parameter) || Parameter == "")
                //    //{
                //    //    TaskWSParameter = chkchkfile;
                //    //}
                //    //else
                //    //{
                //    //    TaskWSParameter = chkchkfile + "," + Parameter;
                //    //}
                //}

                //by xieming edit

                string[] ParaTemp = TaskWSParameter.Split(',');
                if (TaskWSParameter != "" && TaskWSParameter != null && TaskWSParameter != "" && TaskWSParameter != string.Empty)
                {

                    obj = UI.PROXY<object>.CallService(TaskWSClass, TaskWSMethod, this.loginUser, ParaTemp, dt, arlList);

                    if (TaskWSReturnType.ToLower() == "bool")
                    {
                        flag = (bool)obj;
                    }
                }
                else
                {

                    obj = UI.PROXY<object>.CallService(TaskWSClass, TaskWSMethod, this.loginUser, dt, arlList);

                    if (TaskWSReturnType.ToLower() == "bool")
                    {
                        flag = (bool)obj;
                    }
                }

                if (flag == true)
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ��WebService��������ɹ�." + '\t';// +CRLF;
                }
                else
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ��WebService��������ʧ��." + '\t' + "0"; //+ CRLF;
                    blnExec = false;
                }

                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                return flag;
            }
            catch (Exception ex)
            {
                strWs = strWs + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�ࣺ" + TaskWSClass + ",������" + TaskWSMethod + " ������WebService���������쳣:" + ex.Message + '\t' + "0" + CRLF;
                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //�ͷ�LOCK TASK
                blnExec = false;
                //UI.PROXY<object>.CallService("BLCommon", "ReleaseLockTask", taskid, this.loginUser);

                return false;
            }

        }

        private void OnFileTransfer(string taskid)
        {
            // this.blnWS = true;
            string TransferType = TaskWay;

            switch (TransferType)
            {
                case "R": //Remoting ���ζ��Ʋ����˹���
                    break;
                case "F": //FTP
                    {
                        if (TaskMode == "S")  // �ϴ�
                        {
                            OnFileTransfer_Send();
                        }
                        else if (TaskMode == "D")  //����
                        {
                            OnFileTransfer_Down();
                        }
                        break;
                    }
            }
        }

        //�ϴ�
        private void OnFileTransfer_Send()
        {
            string messglog = null;
            //string message = null;
            bool blnZip = false;
            string taskIdName = TaskDate + '\t' + TaskId + '\t' + TaskName;
            FileControl FileOperator = new FileControl();
            //string TransferType = TaskWay;
            string[] FileBackList = null;
            //
            this.blnWS = true;

            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺��ʼ�ϴ��ļ� " + '\t' + CRLF;
            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

            try
            {
                // �ļ��ϴ���ʽ 
                string FileChk = null;


               FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);

               SFTPOperation SFTPHelper = new SFTPOperation(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword);
              

                #region �ϴ�

                List<string> dLFiles = new List<string>();
                string[] files = FileOperator.GetFileList(TaskSourcePath, TaskFileType, true);
                StringBuilder strfile = new StringBuilder();

                if (files != null)
                {
                    ArrayList Listfi = new ArrayList();
                    Listfi.AddRange(files);
                    Listfi.Sort();
                    Listfi.Reverse();
                    strfile = new StringBuilder();
                    for (int i = 0; i < Listfi.Count; i++)
                    {
                        strfile.Append(Listfi[i]);
                        strfile.Append("\n");

                    }
                    strfile.Remove(strfile.ToString().LastIndexOf('\n'), 1);


                    string[] fi = strfile.ToString().Split('\n');
                    FileBackList = fi;
                    #region  �ļ��ϴ�
                    for (int i = 0; i < fi.Length; i++)
                    {

                        bool blnSend = true;
                        blnZip = false;
                        // 1--�ϴ��ļ���Ŀ�ķ�������
                        FileInfo sourcefile = new FileInfo(fi[i]);
                        if (TaskChkChk == true)
                        {

                            //��������.chk�ļ����򲻴��ļ���
                            FileChk = null;
                            if (sourcefile.Extension.ToLower() == ".zip")
                            {
                                blnZip = true;
                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                {
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ���ϴ��ļ���Ե�chk�ļ������ڡ����β��ϴ����ļ���" + '\t' + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    blnSend = false;
                                    blnExec = false;
                                    goto ExitNext;
                                }
                            }
                            else if (sourcefile.Extension.ToLower() == ".chk")  //���chk�Ƿ���������Ӧ��zip�ļ�
                            {
                                goto ExitChk;
                            }

                        }
                        // 2--����ϴ����ļ���ȷ�� 
                        if (sourcefile.Extension.ToLower() == ".zip")
                        {
                            //edit by ssh 2013-04-17  �ж��ļ��Ƿ���������Ľ���ʹ��
                            if (FileOperator.IsFileInUse(fi[i]) == true)
                            {
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ���ļ�����ռ�á�" + '\t' + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                System.Threading.Thread.Sleep(20000);
                            }
                            //edit by ssh 2013-04-16 ���˴�ѭ�����ļ��ѱ���������һ���̣��Ƴ���������´�ѭ��
                            if (!File.Exists(fi[i]))
                            {
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " ���ļ��ѱ�ɾ������������" + "\t" + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                continue;
                            }
                            //end 
                            FileOperator.CheckDirectory(@"C:\TEMP");
                            if (FilesZip.UnZip(fi[i], @"C:\TEMP") == false)
                            {
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " ��ѹ�ļ��쳣�����β��ϴ����ļ���" + "\t" + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                blnSend = false;
                                blnExec = false;
                                goto ExitNext;
                            }
                        }
                        //3--�ϴ��ļ�
                        messglog = null;



                        //by xieming add 2012-05-21 on
                        string mytaskid = taskid;
                        //if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, out messglog) == false)

                        if (mytaskid == "M8008" || mytaskid == "M8009")
                        {

                            //SFTPHelper.Put(file, Path.GetFileName(file));
                            //SFTPHelper.Put(file, Path.GetFileName(file.Replace(ZIPEXTENSION, CHKEXTENSION)));
                            //by daiyunquan on 2017-08-04 begin
                            //for (i = 0; i < fi.Length; i++)
                            //{
                                SFTPHelper.Put(fi[i], Path.GetFileName(fi[i]));
                                SFTPHelper.Put(fi[i].Split('.')[0] + ".chk", Path.GetFileName(fi[i].Split('.')[0] + ".chk"));
                            
                            //}
                            //i = 0;
                            //by daiyunquan on 2017-08-04 end
                           //SFTPHelper.Put(fi[i], Path.GetFileName(fi[i].Replace("zip", "chk")));
                        }
                        else 
                        {
                            if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, mytaskid, out messglog) == false)
                            {
                                FileOperator.WriteFileLog(messglog, TaskLogPath, LogFileName);

                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " �ļ��ϴ������Ե�һ��" + "\t" + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                System.Threading.Thread.Sleep(2000);

                                if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, mytaskid, out messglog) == false)
                                {
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " �ļ��ϴ������Եڶ���" + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                    System.Threading.Thread.Sleep(2000);

                                    if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, mytaskid, out messglog) == false)
                                    {
                                        strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " �ϴ��ļ�ʧ�ܡ�ʧ��ԭ��" + messglog + "\t" + "0" + CRLF;
                                        FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                        blnSend = false;
                                        blnExec = false;
                                        goto ExitNext;
                                    }
                                }

                            }

                          }

                          if (mytaskid != "M8008" && mytaskid != "M8009")
                        {
                            // 4--�Ƚϱ����ļ���Ŀ�Ķ��ļ���С
                            long fileSize = ftpUpDown.GetFileSize(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"));

                            if (fileSize == sourcefile.Length)
                            {
                                if (TaskChkChk == true)
                                {

                                    //�ϴ���Ե�chk�ļ�
                                    if (blnZip == true)
                                    {
                                        if (ftpUpDown.Upload(FileChk, TaskDestinationPath, TaskSourcePath, out messglog) == false)
                                        {
                                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + FileChk + " �ϴ��ļ�ʧ�ܡ�ʧ��ԭ��" + messglog + "\t" + "0" + CRLF;
                                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;
                                        }
                                        long fileChkSize = ftpUpDown.GetFileSize(TaskDestinationPath, FileChk.Replace(TaskSourcePath, ""));
                                        if (fileChkSize != 0)
                                        {
                                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + FileChk + " �ϴ��ļ�ʧ�ܡ�ʧ��ԭ���ϴ��ļ��뱾���ļ���С��һ��." + "\t" + "0" + CRLF;
                                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;
                                        }

                                    }

                                }

                                //�ļ�������20111220

                                FileInfo fileinfo = new FileInfo(fi[i]);
                                if (ftpUpDown.FtpFileRename(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"), fileinfo.Name) == false)
                                {
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " FTP�ļ������������Ե�һ��" + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                    System.Threading.Thread.Sleep(2000);
                                    if (ftpUpDown.FtpFileRename(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"), fileinfo.Name) == false)
                                    {
                                        strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " FTP�ļ������������Եڶ���" + "\t" + "0" + CRLF;
                                        FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                        System.Threading.Thread.Sleep(2000);

                                        if (ftpUpDown.FtpFileRename(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"), fileinfo.Name) == false)
                                        {
                                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " FTP�ļ�������ʧ�ܡ�" + "\t" + "0" + CRLF;
                                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;
                                        }
                                    }
                                }
                                dLFiles.Add(fi[i]);
                            }
                            else
                            {
                                if (TaskId != "M4001")
                                {
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ϴ��ļ���Դ�ļ���С��һ�¡������ϴ�ʧ�ܡ�" + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    blnSend = false;
                                    blnExec = false;
                                    goto ExitNext;
                                }
                                else
                                {
                                    //strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ϴ��ļ���Դ�ļ���С��һ�¡������ϴ�ʧ�ܡ�" + "\t" + "0" + CRLF;
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + Path.GetDirectoryName(fi[i]) + " ����·���е�Դ�ļ���СΪ��" + sourcefile.Length + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + TaskDestinationPath + " FTP·����Ӧ��Ŀ¼�е��ϴ��ļ���СΪ��" + fileSize + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    continue;
                                }

                            }
                    }
                        dLFiles.ToArray();
                        if (blnSend)
                        {
                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ϴ��ļ��ɹ���" + "�ܹ���¼����" + fi.Length + "��ʣ:" + (fi.Length - (i + 1)) + "\t" + "1" + CRLF;
                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                        }
                        else
                        {
                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ϴ��ļ�ʧ�ܡ�" + "\t" + "1" + CRLF;
                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                            blnExec = false;
                        }
                    ExitChk: messglog = null;
                    }
                    #endregion
                    #region �ļ�������ɾ��
                    //��������ɾ��
                    ////??????????????????????????????????????????????????????????????
                    for (int i = 0; i < fi.Length; i++)
                    {
                        blnZip = false;
                        // 1--�ϴ��ļ���Ŀ�ķ�������
                        FileInfo sourcefile = new FileInfo(fi[i]);

                        //��������.chk�ļ����򲻴��ļ���
                        if (TaskChkChk == true)
                        {
                            FileChk = null;
                            if (sourcefile.Extension.ToLower() == ".zip")
                            {
                                blnZip = true;
                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                            }

                            else if (sourcefile.Extension.ToLower() == ".chk")
                            {
                                goto ExitChk;
                            }
                        }

                        // 4-zip�����ļ�
                        string backFile = TaskBackupPath + fi[i].Replace(TaskSourcePath, @"\");
                        FileInfo thefile = new FileInfo(backFile);
                        FileOperator.CheckDirectory(thefile.DirectoryName);

                        if (File.Exists(backFile))
                        {
                            //������Ŀ¼������ͬ���ļ������±��ݵ��ļ�������������ɾ��ԭ�����ļ�
                            // File.Delete(backFile);
                            string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                            backFile = backFile.Replace(thefile.Extension, strhms) + thefile.Extension;

                        }
                        //�ѱ��ݺ�ɾ���ϲ�����Ϊ�ļ����ƶ�
                        if (File.Exists(fi[i]))   //edit by ssh 2013-04-16 �ж��Ƿ�����Ҫ���ݵ��ļ�
                        {
                            File.Copy(fi[i], backFile, true);

                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ��ɹ���" + "\t" + "1" + CRLF;
                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                        }
                        // 4-chk�����ļ�
                        if (TaskChkChk == true)
                        {
                            if (blnZip == true)
                            {
                                string bkChkFile = TaskBackupPath + FileChk.Replace(TaskSourcePath, @"\");
                                FileInfo Chkfile = new FileInfo(bkChkFile);
                                FileOperator.CheckDirectory(Chkfile.DirectoryName);

                                if (File.Exists(bkChkFile))
                                {
                                    //File.Delete(bkChkFile);
                                    //������Ŀ¼������ͬ���ļ������±��ݵ��ļ�������������ɾ��ԭ�����ļ�
                                    string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                                    bkChkFile = bkChkFile.Replace(Chkfile.Extension, strhms) + Chkfile.Extension;

                                }
                                //����ZIP
                                if (File.Exists(FileChk))   //edit by ssh 2013-04-16 �ж��Ƿ�����Ҫ���ݵ��ļ�
                                {
                                    File.Copy(FileChk, bkChkFile, true);
                                }
                            }
                        }
                        //edit by ssh 2013-04-16 �������汸�ݷ�����
                        //strlog =  taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ��ɹ���" + "\t" + "1" + CRLF;
                        //FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);


                        //------------------------------------------------------
                        if (TaskId != "M4001")
                        {
                            //  5--zipɾ���ļ� 
                            if (File.Exists(fi[i]))
                            {
                                File.Delete(fi[i]);
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ɾ���ļ��ɹ���" + "\t" + "1" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                            }

                            //  5--chkɾ���ļ� 
                            if (blnZip == true)
                            {
                                if (TaskChkChk == true)
                                {

                                    if (File.Exists(FileChk))
                                    {
                                        File.Delete(FileChk);
                                    }
                                }
                            }
                        }
                        else   //edit by ssh 2013-03-20 
                        {
                            //��ִ������M4001���Ե���SCʱ����ɾ��ʧ�ܲ��״���������ִ��
                            try
                            {
                                //  5--zipɾ���ļ� 
                                if (File.Exists(fi[i]))
                                {
                                    File.Delete(fi[i]);
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ɾ���ļ��ɹ���" + "\t" + "1" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                }

                                //  5--chkɾ���ļ� 
                                if (blnZip == true)
                                {
                                    if (TaskChkChk == true)
                                    {

                                        if (File.Exists(FileChk))
                                        {
                                            File.Delete(FileChk);
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ļ�ɾ��ʧ�ܣ���������ɾ����һ���ļ����쳣��Ϣ:" + ex.Message + "\t" + "1" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                continue;
                            }
                        }
                    //end
                    ExitChk: FileChk = null;
                    }
                    #endregion
                /////???????????????????????????????????????????????????

                    ////ʧ��ʱ���ļ���ȫ����
                ExitNext: messglog = null;
                    ////ʧ��ʱ���ļ���ȫ����

                }

                else
                {
                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 ������û���ϴ����ļ���" + "\t" + "1" + CRLF;
                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                }

                #endregion  �ϴ�
                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺���������ļ�����" + (blnExec == false ? "ʧ��." : "�ɹ�.") + "\t";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
            }
            catch (Exception ex)
            {
                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�쳣:" + ex.Message + "\t" + "0";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                FileControl fw = new FileControl();
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //add by xsq 20110929 �ͷ�����
                //UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                //end by xsq 20110929 �ͷ�����
            }
            finally
            {
                //ʧ��ʱ���ļ��ϴ���ȫ����
                if (blnExec == false && TaskMode == "S")
                {
                    for (int i = 0; i < FileBackList.Length; i++)
                    {
                        FileInfo sourcefile = new FileInfo(FileBackList[i]);
                        string strhms = "-ERROR-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                        // 4-zip�����ļ�
                        string fileName = sourcefile.Name.Replace(sourcefile.Extension, "") + strhms + sourcefile.Extension;
                        string backFile = TaskBackupPath + FileBackList[i].Replace(TaskSourcePath, @"\").Replace(sourcefile.Name, fileName);

                        FileInfo thefile = new FileInfo(backFile);
                        FileOperator.CheckDirectory(thefile.DirectoryName);

                        //�ѱ��ݺ�ɾ���ϲ�����Ϊ�ļ����ƶ�
                        File.Copy(FileBackList[i], backFile, true);
                    }
                }
                //ʧ��ʱ���ļ���ȫ����
                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "�ϴ��ļ�����" + "\t";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
            }

        }

        private void OnFileTransfer_Down()
        {
            // string messglog = null;
            string message = null;
            bool blnZip = false;
            string taskIdName = TaskDate + '\t' + TaskId + '\t' + TaskName;
            FileControl FileOperator = new FileControl();
            //string TransferType = TaskWay;
            //string[] FileBackList = null;
            //
            this.blnWS = true;

            try
            {
                // �ļ��ϴ���ʽ 
                string FileChk = null;

                //switch (TransferType)
                //{
                //case "R": //Remoting ���ζ��Ʋ����˹���
                //    break;
                //case "F": //FTP
                //    {
                FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                #region ����
                List<string> dLFiles = new List<string>();
                message = null;
                string[] fi = ftpUpDown.GetFileList(TaskSourcePath, true, TaskFileType, out message);

                if (fi != null)
                {
                    for (int i = 0; i < fi.Length; i++)
                    {
                        bool blndown = true;
                        blnZip = false;
                        FileInfo sourcefile = new FileInfo(fi[i]);
                        FileChk = null;
                        if (TaskChkChk == true)
                        {

                            if (sourcefile.Extension.ToLower() == ".zip")
                            {
                                blnZip = true;
                                //edit by ssh 2014-11-12  M5012�����ж��ļ����������ļ�.ok�ļ�
                                if (TaskId == "M5012")
                                {
                                    FileChk = fi[i] +".ok";
                                }
                                else
                                {
                                    FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                }
                                //���޲�����.chk�ļ����򲻴��ļ���

                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                {
                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ���chk�ļ������ڡ����β����ش��ļ���" + "\t" + "0" + CRLF;
                                    blndown = false;
                                    blnExec = false;
                                    goto ExitNext;
                                }
                            }
                            else if (sourcefile.Extension.ToLower() == ".chk" || sourcefile.Extension.ToLower() == ".ok")  //���chk�Ƿ���������Ӧ��zip�ļ�
                            {
                                goto ExitChk;
                            }
                        }
                        long filesize = 0;

                        // 1--��FTP�������ļ�
                        FileInfo fileInf = new FileInfo(TaskDestinationPath + fi[i]);
                        FileOperator.CheckDirectory(fileInf.DirectoryName);
                        if (fileInf.Exists)
                        {
                            File.Delete(TaskDestinationPath + fi[i]);    //������ �����ش����ļ����δ���
                        }

                        string downlog = null;
                        //A-����zip�ļ�
                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, fi[i], out  filesize, out downlog) == true)
                        {
                            //���»�ȡ�����ļ�
                            FileInfo f = new FileInfo(TaskDestinationPath + fi[i]);

                            // 2--��֤���ص��ļ���С��FTP�ϵ��ļ���С�Ƚ��������ݺ�ɾ��
                          
                                if (TaskId == "M5012")
                                {
                                    filesize = f.Length;
                                }

                            if (filesize == f.Length )
                            {
                                //// 3--������ص��ļ���ȷ��  ������
                                if (fileInf.Extension.ToLower() == ".zip")
                                {
                                    //add by jdb 20081021
                                    FileOperator.CheckDirectory(@"C:\TEMP");
                                    //end by jdb 20081021
                                    if (FilesZip.UnZip(TaskDestinationPath + fi[i], @"C:\TEMP") == false)
                                    {
                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ��ѹ�ļ��쳣�����ص��ļ���������" + "\t" + "0" + CRLF;
                                        blndown = false;
                                        goto ExitNext;
                                    }
                                }

                                ////B-���ض�Ӧ��chk.�ļ�
                                if (blnZip == true)
                                {
                                    if (TaskChkChk == true)
                                    {
                                        filesize = 0;
                                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, FileChk, out  filesize, out downlog) == true)
                                        {
                                            //���»�ȡ�����ļ�
                                            FileInfo fk = new FileInfo(TaskDestinationPath + FileChk);
                                            if (filesize != fk.Length)
                                            {
                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ���Դ�ļ���С��һ�¡������ļ��Ĳ�����1��" + "\t" + "0" + CRLF;
                                                blndown = false;
                                                goto ExitNext;
                                            }

                                        }
                                    }
                                }

                                dLFiles.Add(fi[i]);
                            }
                            else
                            {
                                blndown = false;
                                blnExec = false;
                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ���Դ�ļ���С��һ�¡������ļ��Ĳ�����2��" + "\t" + "0" + CRLF;
                                goto ExitNext;
                            }
                        }
                        else
                        {
                            blndown = false;
                            blnExec = false;
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ�ʧ�ܡ�ʧ��ԭ��" + downlog + "\t" + "0" + CRLF;
                            goto ExitNext;
                        }
                        dLFiles.ToArray();
                        if (blndown)
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ��ɹ���" + "\t" + "1" + CRLF;
                        }
                        else
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ�ʧ�ܡ�" + "\t" + "0" + CRLF;
                            blnExec = false;
                        }
                    ExitChk: message = null;

                    }

                    #region
                    if (TaskId == "I2090" || TaskId == "I2092")
                    {
                        for (int p = 0; p < fi.Length; p++)
                        {

                            // 8.1 ��װZip�����ļ���
                            string strZipFileName = Path.GetFileName(fi[p]);

                            // ȡ����".ZIP"���ļ���
                            string strZipFileNameWithout = Path.GetFileNameWithoutExtension(fi[p]);

                            // ȡZIP�ļ�������
                            string[] strValue = DataConvert.ParseString(strZipFileNameWithout).Split(new char[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
                            string ZipAccountDate = "";
                            if (strValue[0] != "")
                            {
                                if (strValue[0].Length == 8 || strValue[0].Length == 16)
                                {
                                    ZipAccountDate = Convert.ToDateTime(strValue[0].ToString().Trim().Substring(0, 4) + "-" + strValue[0].ToString().Trim().Substring(4, 2) + "-" + strValue[0].ToString().Trim().Substring(6, 2)).ToShortDateString();
                                    //ZipAccountDate = strValue[0].ToString();
                                }
                            }
                            //��ȡ�ļ������޸�����
                            FileInfo f = new FileInfo(TaskDestinationPath + fi[p]);
                            string StoreTransDate = f.LastWriteTime.ToString();

                            // ȡZIP�ļ��ĵ��̴��� �����ڵ�"Ӫҵʱ��"( ����Ӫ�ս���SC����ʱд��Ӫ���ų̱�)
                            string ZipStoreId = "";

                            if (TaskId == "I2092")  //2����Ų�ת
                            {
                                if (strValue[3] != "")
                                {
                                    //ZipStoreId = ChgStoreId6bit(strValue[2]);

                                    ZipStoreId = strValue[3];
                                }
                            }
                            if (TaskId == "I2090")
                            {
                                if (strValue[2] != "")
                                {
                                    //ZipStoreId = ChgStoreId6bit(strValue[2]);

                                    ZipStoreId = UI.PROXY<string>.CallService("S31F0102_BTImport", "ChgStoreId6bit", strValue[2]);
                                }
                            }
                            string TargetId = "02";

                            object obj = UI.PROXY<object>.CallService("S31F0102_BTImport", "WriteBtSchedulLog",

                                //1���̺�
                                //2����
                                //3�ų�����  01 ���� 02 Ӫ�� ��������ʱ��01 ,Ӫ�ս����ļ�ʱ��02
                                //4������/Ӫ��ʱ���  01��ʾ0��00��02��ʾ14��00
                                //5�ų���/�ų���  01 �ų����ս�����  02 �ų��� 
                                //6���̶���/Ӫ���ϴ�ʱ�� (�������ṩ����ȡ��)   
                                //7���̶���/Ӫ�����ر�־ 01 δ����  02 ������ 
                                //8��ʳ������־  01 δ���� 02 �Ѷ���
                                //9����״̬ 00δ����  01 �ɹ� 02 ʧ��
                                //10ʧ��ԭ��
                                //11������
                                ZipStoreId,
                                ZipAccountDate,
                                "02",
                                TargetId,
                                "02",
                                StoreTransDate,
                                "02",
                                "",
                                "00",
                                "",
                                this.loginUser);
                        }

                    }
                    #endregion
                    //// Begin 2008/5/15 ȫ�������ļ��ɹ��󣬿�ʼ���ļ��ı��ݺ�ɾ��

                    for (int i = 0; i < fi.Length; i++)
                    {
                        blnZip = false;
                        FileInfo sourcefile = new FileInfo(fi[i]);
                        FileChk = null;
                        if (sourcefile.Extension.ToLower() == ".zip")
                        {
                            blnZip = true;
                            //edit by ssh 2014-11-12  M5012�����ж��ļ����������ļ�.ok�ļ�
                            if (TaskId == "M5012")
                            {
                                FileChk = fi[i] + ".ok";
                            }
                            else
                            {
                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                            }
                        }
                        else
                        {
                            goto ExitChk;
                        }
                        message = null;
                        //����zip�ļ�
                        if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, fi[i], out message) == true)
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + message + "\t" + "1" + CRLF;
                            if (TaskChkChk == true)
                            {
                                //����chk�ļ�
                                if (blnZip == true)
                                {
                                    if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, FileChk, out message) == false)
                                    {
                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + FileChk + message + "\t" + "0" + CRLF;
                                        blnExec = false;
                                        goto ExitNext;
                                    }
                                }
                            }
                        }
                        else
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + message + "\t" + "0" + CRLF;
                            blnExec = false;
                            goto ExitNext;
                        }
                    ExitChk: message = null;
                    }
                    ///// End 2008/5/15 ȫ�������ļ��ɹ��󣬿�ʼ���ļ��ı��ݺ�ɾ��

                }
                else
                {
                    //by xieming add 2011-09-09  �������ʧ�ܣ��򱾵�·���»����ļ��������û���⣬��ɼ����������
                    //                           ����������⣬���°�����FTP���£���������Ḳ�ǡ�
                    string[] CurIsExistsfile = new FileControl().GetFileList(TaskDestinationPath, TaskFileType, true);

                    if (CurIsExistsfile != null)
                    {
                        blnWS = true;
                    }
                    else
                    {

                        //by xieming add 2011-09-09
                        //û���ļ����ٵ���ws
                        if (message == null)
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺������û�����ص��ļ���" + "\t" + "1" + CRLF;
                        }
                        else
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺������û�����ص��ļ���" + "\t" + "1" + CRLF;
                        }
                        blnWS = false;
                    }
                }
            ExitNext: message = null;

                #endregion  ����
                //        }

                //}

                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺���������ļ�����" + (blnExec == false ? "ʧ��." : "�ɹ�.") + "\t";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);


            }
            catch (Exception ex)
            {
                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�쳣:" + ex.Message + "\t" + "0";
                FileControl fw = new FileControl();
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //add by xsq 20110929 �ͷ�����
                // UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                //end by xsq 20110929 �ͷ�����
            }
        }

        /// <summary>
        /// һ����6λ���ת��Ϊ2����������
        /// ����ֵ������ɵĵ�ţ����µĵ�ţ��õ�����ϵͳ��������
        /// </summary>

        /// <returns></returns>
        public string ChgStoreId6bit(string OldStoreID)
        {
            if (OldStoreID.Substring(0, 1) == "1" || OldStoreID.Substring(0, 1) == "3" || OldStoreID.Substring(0, 1) == "5")
            {

                if (OldStoreID.Substring(0, 1) == "1" || OldStoreID.Substring(0, 1) == "5")
                {
                    return "01" + OldStoreID.Substring(0, 4);
                }
                else
                {
                    return "02" + OldStoreID.Substring(0, 4);
                }
            }
            else
            {
                return OldStoreID;
            }

        }

        /// <summary>
        /// �����ļ� (���췽�������ݣ�
        /// </summary>
        /// <param name="taskid">�����</param>

        private void OnFileTransfer_bak(string taskid)
        {
            string messglog = null;
            string message = null;
            bool blnZip = false;
            string taskIdName = TaskDate + '\t' + TaskId + '\t' + TaskName;
            FileControl FileOperator = new FileControl();
            string TransferType = TaskWay;
            string[] FileBackList = null;
            //
            this.blnWS = true;

            try
            {
                // �ļ��ϴ���ʽ 
                string FileChk = null;

                switch (TransferType)
                {
                    case "R": //Remoting ���ζ��Ʋ����˹���
                        break;
                    case "F": //FTP
                        {
                            FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                            #region �ϴ�
                            if (TaskMode == "S")  // �ϴ�
                            {
                                List<string> dLFiles = new List<string>();
                                string[] files = FileOperator.GetFileList(TaskSourcePath, TaskFileType, true);
                                StringBuilder strfile = new StringBuilder();

                                if (files != null)
                                {
                                    ArrayList Listfi = new ArrayList();
                                    Listfi.AddRange(files);
                                    Listfi.Sort();
                                    Listfi.Reverse();
                                    strfile = new StringBuilder();
                                    for (int i = 0; i < Listfi.Count; i++)
                                    {
                                        strfile.Append(Listfi[i]);
                                        strfile.Append("\n");

                                    }
                                    strfile.Remove(strfile.ToString().LastIndexOf('\n'), 1);


                                    string[] fi = strfile.ToString().Split('\n');
                                    FileBackList = fi;
                                    for (int i = 0; i < fi.Length; i++)
                                    {
                                        bool blnSend = true;
                                        blnZip = false;
                                        // 1--�ϴ��ļ���Ŀ�ķ�������
                                        FileInfo sourcefile = new FileInfo(fi[i]);
                                        if (TaskChkChk == true)
                                        {

                                            //��������.chk�ļ����򲻴��ļ���
                                            FileChk = null;
                                            if (sourcefile.Extension.ToLower() == ".zip")
                                            {
                                                blnZip = true;
                                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                                {
                                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ���ϴ��ļ���Ե�chk�ļ������ڡ����β��ϴ����ļ���" + '\t' + "0" + CRLF;
                                                    blnSend = false;
                                                    blnExec = false;
                                                    goto ExitNext;
                                                }
                                            }
                                            else if (sourcefile.Extension.ToLower() == ".chk")  //���chk�Ƿ���������Ӧ��zip�ļ�
                                            {
                                                goto ExitChk;
                                            }

                                        }
                                        // 2--����ϴ����ļ���ȷ�� 
                                        if (sourcefile.Extension.ToLower() == ".zip")
                                        {
                                            FileOperator.CheckDirectory(@"C:\TEMP");
                                            if (FilesZip.UnZip(fi[i], @"C:\TEMP") == false)
                                            {
                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " ��ѹ�ļ��쳣�����β��ϴ����ļ���" + "\t" + "0" + CRLF;
                                                blnSend = false;
                                                blnExec = false;
                                                goto ExitNext;
                                            }
                                        }
                                        //3--�ϴ��ļ�
                                        messglog = null;
                                        if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, out messglog) == false)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 " + fi[i] + " �ϴ��ļ�ʧ�ܡ�ʧ��ԭ��" + messglog + "\t" + "0" + CRLF;
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;
                                        }

                                        // 4--�Ƚϱ����ļ���Ŀ�Ķ��ļ���С
                                        long fileSize = ftpUpDown.GetFileSize(TaskDestinationPath, fi[i].Replace(TaskSourcePath, ""));

                                        if (fileSize == sourcefile.Length)
                                        {
                                            if (TaskChkChk == true)
                                            {

                                                //�ϴ���Ե�chk�ļ�
                                                if (blnZip == true)
                                                {
                                                    if (ftpUpDown.Upload(FileChk, TaskDestinationPath, TaskSourcePath, out messglog) == false)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + FileChk + " �ϴ��ļ�ʧ�ܡ�ʧ��ԭ��" + messglog + "\t" + "0" + CRLF;
                                                        blnSend = false;
                                                        blnExec = false;
                                                        goto ExitNext;
                                                    }
                                                    long fileChkSize = ftpUpDown.GetFileSize(TaskDestinationPath, FileChk.Replace(TaskSourcePath, ""));
                                                    if (fileChkSize != 0)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + FileChk + " �ϴ��ļ�ʧ�ܡ�ʧ��ԭ���ϴ��ļ��뱾���ļ���С��һ��." + "\t" + "0" + CRLF;
                                                        blnSend = false;
                                                        blnExec = false;
                                                        goto ExitNext;
                                                    }

                                                }

                                            }


                                            dLFiles.Add(fi[i]);
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ϴ��ļ���Դ�ļ���С��һ�¡������ϴ�ʧ�ܡ�" + "\t" + "0" + CRLF;
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;

                                        }
                                        dLFiles.ToArray();
                                        if (blnSend)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ϴ��ļ��ɹ���" + "\t" + "1" + CRLF;
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �ϴ��ļ�ʧ�ܡ�" + "\t" + "1" + CRLF;
                                            blnExec = false;
                                        }
                                    ExitChk: messglog = null;
                                    }

                                    //��������ɾ��
                                    ////??????????????????????????????????????????????????????????????
                                    for (int i = 0; i < fi.Length; i++)
                                    {
                                        blnZip = false;
                                        // 1--�ϴ��ļ���Ŀ�ķ�������
                                        FileInfo sourcefile = new FileInfo(fi[i]);

                                        //��������.chk�ļ����򲻴��ļ���
                                        if (TaskChkChk == true)
                                        {
                                            FileChk = null;
                                            if (sourcefile.Extension.ToLower() == ".zip")
                                            {
                                                blnZip = true;
                                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                            }

                                            else if (sourcefile.Extension.ToLower() == ".chk")
                                            {
                                                goto ExitChk;
                                            }
                                        }

                                        // 4-zip�����ļ�
                                        string backFile = TaskBackupPath + fi[i].Replace(TaskSourcePath, @"\");
                                        FileInfo thefile = new FileInfo(backFile);
                                        FileOperator.CheckDirectory(thefile.DirectoryName);

                                        if (File.Exists(backFile))
                                        {
                                            //������Ŀ¼������ͬ���ļ������±��ݵ��ļ�������������ɾ��ԭ�����ļ�
                                            // File.Delete(backFile);
                                            string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                                            backFile = backFile.Replace(thefile.Extension, strhms) + thefile.Extension;

                                        }
                                        //�ѱ��ݺ�ɾ���ϲ�����Ϊ�ļ����ƶ�
                                        File.Copy(fi[i], backFile, true);
                                        // 4-chk�����ļ�
                                        if (TaskChkChk == true)
                                        {
                                            if (blnZip == true)
                                            {
                                                string bkChkFile = TaskBackupPath + FileChk.Replace(TaskSourcePath, @"\");
                                                FileInfo Chkfile = new FileInfo(bkChkFile);
                                                FileOperator.CheckDirectory(Chkfile.DirectoryName);

                                                if (File.Exists(bkChkFile))
                                                {
                                                    //File.Delete(bkChkFile);
                                                    //������Ŀ¼������ͬ���ļ������±��ݵ��ļ�������������ɾ��ԭ�����ļ�
                                                    string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                                                    bkChkFile = bkChkFile.Replace(Chkfile.Extension, strhms) + Chkfile.Extension;

                                                }
                                                //����ZIP
                                                File.Copy(FileChk, bkChkFile, true);
                                            }
                                        }
                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ��ɹ���" + "\t" + "1" + CRLF;


                                        //------------------------------------------------------

                                        //  5--zipɾ���ļ� 
                                        if (File.Exists(fi[i]))
                                        {
                                            File.Delete(fi[i]);
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ɾ���ļ��ɹ���" + "\t" + "1" + CRLF;
                                        }

                                        //  5--chkɾ���ļ� 
                                        if (blnZip == true)
                                        {
                                            if (TaskChkChk == true)
                                            {

                                                if (File.Exists(FileChk))
                                                {
                                                    File.Delete(FileChk);
                                                }
                                            }
                                        }
                                    ExitChk: FileChk = null;
                                    }
                                /////???????????????????????????????????????????????????

                                    ////ʧ��ʱ���ļ���ȫ����
                                ExitNext: messglog = null;
                                    ////ʧ��ʱ���ļ���ȫ����

                                }

                                else
                                {
                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺 ������û���ϴ����ļ���" + "\t" + "1" + CRLF;
                                }
                                break;

                            }
                            #endregion  �ϴ�
                            #region ����
                            else if (TaskMode == "D") //����
                            {
                                List<string> dLFiles = new List<string>();
                                message = null;
                                string[] fi = ftpUpDown.GetFileList(TaskSourcePath, true, TaskFileType, out message);



                                if (fi != null)
                                {
                                    for (int i = 0; i < fi.Length; i++)
                                    {
                                        bool blndown = true;
                                        blnZip = false;
                                        FileInfo sourcefile = new FileInfo(fi[i]);
                                        FileChk = null;
                                        if (TaskChkChk == true)
                                        {

                                            if (sourcefile.Extension.ToLower() == ".zip")
                                            {
                                                blnZip = true;
                                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                                //���޲�����.chk�ļ����򲻴��ļ���

                                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                                {
                                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ���chk�ļ������ڡ����β����ش��ļ���" + "\t" + "0" + CRLF;
                                                    blndown = false;
                                                    blnExec = false;
                                                    goto ExitNext;
                                                }
                                            }
                                            else if (sourcefile.Extension.ToLower() == ".chk")  //���chk�Ƿ���������Ӧ��zip�ļ�
                                            {
                                                goto ExitChk;
                                            }
                                        }
                                        long filesize = 0;

                                        // 1--��FTP�������ļ�
                                        FileInfo fileInf = new FileInfo(TaskDestinationPath + fi[i]);
                                        FileOperator.CheckDirectory(fileInf.DirectoryName);
                                        if (fileInf.Exists)
                                        {
                                            File.Delete(TaskDestinationPath + fi[i]);    //������ �����ش����ļ����δ���
                                        }

                                        string downlog = null;
                                        //A-����zip�ļ�
                                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, fi[i], out  filesize, out downlog) == true)
                                        {
                                            //���»�ȡ�����ļ�
                                            FileInfo f = new FileInfo(TaskDestinationPath + fi[i]);

                                            // 2--��֤���ص��ļ���С��FTP�ϵ��ļ���С�Ƚ��������ݺ�ɾ��
                                            if (filesize == f.Length)
                                            {
                                                //// 3--������ص��ļ���ȷ��  ������
                                                if (fileInf.Extension.ToLower() == ".zip")
                                                {
                                                    //add by jdb 20081021
                                                    FileOperator.CheckDirectory(@"C:\TEMP");
                                                    //end by jdb 20081021
                                                    if (FilesZip.UnZip(TaskDestinationPath + fi[i], @"C:\TEMP") == false)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " ��ѹ�ļ��쳣�����ص��ļ���������" + "\t" + "0" + CRLF;
                                                        blndown = false;
                                                        goto ExitNext;
                                                    }
                                                }

                                                ////B-���ض�Ӧ��chk.�ļ�
                                                if (blnZip == true)
                                                {
                                                    if (TaskChkChk == true)
                                                    {
                                                        filesize = 0;
                                                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, FileChk, out  filesize, out downlog) == true)
                                                        {
                                                            //���»�ȡ�����ļ�
                                                            FileInfo fk = new FileInfo(TaskDestinationPath + FileChk);
                                                            if (filesize != fk.Length)
                                                            {
                                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ���Դ�ļ���С��һ�¡������ļ��Ĳ�����3��" + "\t" + "0" + CRLF;
                                                                blndown = false;
                                                                goto ExitNext;
                                                            }

                                                        }
                                                    }
                                                }



                                                dLFiles.Add(fi[i]);
                                            }
                                            else
                                            {
                                                blndown = false;
                                                blnExec = false;
                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ���Դ�ļ���С��һ�¡������ļ��Ĳ�����4��" + "\t" + "0" + CRLF;
                                                goto ExitNext;
                                            }
                                        }
                                        else
                                        {
                                            blndown = false;
                                            blnExec = false;
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ�ʧ�ܡ�ʧ��ԭ��" + downlog + "\t" + "0" + CRLF;
                                            goto ExitNext;
                                        }
                                        dLFiles.ToArray();
                                        if (blndown)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ��ɹ���" + "\t" + "1" + CRLF;
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + " �����ļ�ʧ�ܡ�" + "\t" + "0" + CRLF;
                                            blnExec = false;
                                        }
                                    ExitChk: message = null;

                                    }

                                    //// Begin 2008/5/15 ȫ�������ļ��ɹ��󣬿�ʼ���ļ��ı��ݺ�ɾ��

                                    for (int i = 0; i < fi.Length; i++)
                                    {
                                        blnZip = false;
                                        FileInfo sourcefile = new FileInfo(fi[i]);
                                        FileChk = null;
                                        if (sourcefile.Extension.ToLower() == ".zip")
                                        {
                                            blnZip = true;
                                            FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                        }
                                        else
                                        {
                                            goto ExitChk;
                                        }
                                        message = null;
                                        //����zip�ļ�
                                        if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, fi[i], out message) == true)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + message + "\t" + "1" + CRLF;
                                            if (TaskChkChk == true)
                                            {
                                                //����chk�ļ�
                                                if (blnZip == true)
                                                {
                                                    if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, FileChk, out message) == false)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + FileChk + message + "\t" + "0" + CRLF;
                                                        blnExec = false;
                                                        goto ExitNext;
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺" + fi[i] + message + "\t" + "0" + CRLF;
                                            blnExec = false;
                                            goto ExitNext;
                                        }
                                    ExitChk: message = null;
                                    }
                                    ///// End 2008/5/15 ȫ�������ļ��ɹ��󣬿�ʼ���ļ��ı��ݺ�ɾ��

                                }
                                else
                                {


                                    //by xieming add 2011-09-09  �������ʧ�ܣ��򱾵�·���»����ļ��������û���⣬��ɼ����������
                                    //                           ����������⣬���°�����FTP���£���������Ḳ�ǡ�
                                    string[] CurIsExistsfile = new FileControl().GetFileList(TaskDestinationPath, TaskFileType, true);

                                    if (CurIsExistsfile != null)
                                    {
                                        blnWS = true;
                                    }
                                    else
                                    {

                                        //by xieming add 2011-09-09
                                        //û���ļ����ٵ���ws
                                        if (message == null)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺������û�����ص��ļ���" + "\t" + "1" + CRLF;
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺������û�����ص��ļ���" + "\t" + "1" + CRLF;
                                        }
                                        blnWS = false;
                                    }
                                }
                            ExitNext: break;
                            }
                            #endregion  ����
                            break;
                        }
                }

                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺���������ļ�����" + (blnExec == false ? "ʧ��." : "�ɹ�.") + "\t";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
            }

            catch (Exception ex)
            {
                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "ִ�в��裺�쳣:" + ex.Message + "\t" + "0";
                FileControl fw = new FileControl();
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
            }
            finally
            {
                //ʧ��ʱ���ļ��ϴ���ȫ����
                if (blnExec == false && TaskMode == "S")
                {
                    for (int i = 0; i < FileBackList.Length; i++)
                    {
                        FileInfo sourcefile = new FileInfo(FileBackList[i]);
                        string strhms = "-ERROR-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                        // 4-zip�����ļ�
                        string fileName = sourcefile.Name.Replace(sourcefile.Extension, "") + strhms + sourcefile.Extension;
                        string backFile = TaskBackupPath + FileBackList[i].Replace(TaskSourcePath, @"\").Replace(sourcefile.Name, fileName);

                        FileInfo thefile = new FileInfo(backFile);
                        FileOperator.CheckDirectory(thefile.DirectoryName);

                        //�ѱ��ݺ�ɾ���ϲ�����Ϊ�ļ����ƶ�
                        File.Copy(FileBackList[i], backFile, true);
                    }
                }
                //ʧ��ʱ���ļ���ȫ����
            }

        }

        /// <summary>
        /// ����ʧ��ʱ�����賿ǰ��ʧ��������Ϣ�ռ����洢�����ؼ�����ʱ����������ö��ŷ��͹���
        /// </summary>
        //add by zhouyu 2014-1-16
        private void SetSendMessage(DataTable dtTaskListA, DataTable dtTaskListB, string strSysType, string taskid, string strResult)
        {
            FileControl fw = new FileControl();//add by zhouyu 2014-1-20
            //add by zhouyu 2014-1-16;begin
            if (dtTaskListA.Rows[0]["TSM_SUB_VALUE"].ToString().Contains(taskid))
            {
                int flagA = UI.PROXY<int>.CallService("M07F0904_FileAccept", "InsertMessageErr", taskid, strResult);
                //���ű����б�
                DataTable dtMessageErr = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetMessageErr");

                foreach (DataRow drMessageErr in dtMessageErr.Rows)
                {
                    //����־��Ϊ1
                    int flag = UI.PROXY<int>.CallService("M07F0904_FileAccept", "UpdateMessageErr", drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_LOG_DATE"].ToString());
                    SendErrMsg objSendErrMsgA = new SendErrMsg();
                    //"���", "ϵͳ���", "�����", "�Ƿ�ɹ�(0 ʧ�� 1 �ɹ� 2 ����ִ����)", "��Ϣ���"
                    string strTestA = objSendErrMsgA.ReSendMessage("990000", strSysType, drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_RESULT"].ToString(), drMessageErr["MER_RESULT"].ToString());
                    fw.WriteFileLog("���ͱ�����Ϣ��" + strSysType + "ϵͳ" + taskid + "���� " + strResult + strTestA + "", TaskLogPath, LogFileName);//������LOG 20120921                     

                }
            }
            else if (dtTaskListB.Rows[0]["TSM_SUB_VALUE"].ToString().Contains(taskid))
            //����־��Ϊ0����������
            {
                int flag = UI.PROXY<int>.CallService("M07F0904_FileAccept", "InsertMessageErr", taskid, strResult);
            }
            else
            //add by zhouyu 2014-1-16;end
            {
                SendErrMsg objSendErrMsg = new SendErrMsg();
                //"���", "ϵͳ���", "�����", "�Ƿ�ɹ�(0 ʧ�� 1 �ɹ� 2 ����ִ����)", "��Ϣ���"
                string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult);
                fw.WriteFileLog("���ͱ�����Ϣ��" + strSysType + "ϵͳ" + taskid + "���� " + strResult + strTest + "", TaskLogPath, LogFileName);//������LOG 20140120
            }

        }

    }


}
