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
        #region "var 变量"
        private const string CRLF = "\r\n";
        string TaskId = null;
        string TaskName = null;
        string TaskMode = null;  //S—上传，D—下载
        bool TaskDeleteFolder = false; //删除资料夹
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
        bool blnExec = true;   //判断任务的全局成功与失败
        bool blnWS = true;    //判断是否调用WS
        bool TaskChkChk = true; //判断是否检查CHK文件

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
        /// 获取FTP上的目录文件
        /// </summary>
        /// <returns></returns>
        public string[] GetFtpFileList()
        {
            GetFileParameter();

            FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
            string errrorMsg = null;
            if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：通讯失败。" + '\t' + "0";
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
        /// 获取本地上的目录文件
        /// </summary>
        /// <returns></returns>
        public string[] GetLocalFileList()
        {
            GetFileParameter();
            FileControl FileOperator = new FileControl();
            string[] files = null;
            if (TaskMode == "S")  // 上传
            {
                files = FileOperator.GetFileList(TaskSourcePath, TaskFileType, true);
            }
            else if (TaskMode == "D")  //下载
            {
                files = FileOperator.GetFileList(TaskDestinationPath, TaskFileType, true);
            }

            return files;
        }

        //手动调用执行
        public bool ExecManualTask()
        {
            return this.ExecManualTask("");
        }

        //手动调用执行
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
            //    int intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "开始处理：手动处理本次任务--" + taskid + "("+TaskName+")");
            strStartLog = TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "开始处理：手动处理本次任务。" + '\t' + CRLF;
            strWs = null;
            SelectExecute();

            //任务执行结束
            strlog = null;

            if (blnExec == false)
            {
                //        intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "06", "结束处理：本次任务处理失败--" + taskid + "(" + TaskName + ")");
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理失败" + '\t' + "0";
            }
            else
            {
                //        intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "结束处理：本次任务处理成功--" + taskid + "(" + TaskName + ")");
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理成功" + '\t' + "1";

            }
            FileControl fw = new FileControl();
            fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

            return blnExec;
        }

        /// <summary>
        /// 带DataSet 的记录传入
        /// </summary>
        /// <returns></returns>
        public bool ExecManualTaskDs(DataSet ds)
        {
            return this.ExecManualTaskDs(ds, "");
        }

        /// <summary>
        /// 带DataSet 的记录传入
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
            strStartLog = TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "开始处理：手动处理本次任务。" + '\t' + CRLF;
            strWs = null;
            SelectExecute(ds);

            //任务执行结束
            strlog = null;

            if (blnExec == false)
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理失败" + '\t' + "0";
            }
            else
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理成功" + '\t' + "1";

            }
            FileControl fw = new FileControl();
            fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

            return blnExec;

        }

        /// <summary>
        /// 带DataTable 的记录传入
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
            strStartLog = TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "开始处理：手动处理本次任务。" + '\t' + CRLF;
            strWs = null;
            SelectExecuteDt(dt, arlList);

            //任务执行结束
            strlog = null;

            if (blnExec == false)
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理失败" + '\t' + "0";
            }
            else
            {
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理成功" + '\t' + "1";

            }
            FileControl fw = new FileControl();
            fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

            return blnExec;

        }


      

        /// <summary>
        /// 传输先后选择
        /// </summary>
        public void SelectExecute()
        {
            // Update 2008/6/3 begin 检查ftp通讯是否正常通讯
            if (TaskFile == true)
            {
                if (taskid != "M8008" && taskid != "M8009")
                {
                    FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                    string errrorMsg = null;
                    if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：通讯失败。" + '\t' + "0";
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
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：通讯失败。" + '\t' + "0";
                        FileControl fw = new FileControl();
                        fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                        blnExec = false;
                        blnfinished = false;
                        return;
                    }
                   
                
                }
            }
            // Update 2008/6/3 end

            //调用ws
            if (TaskWS == true)
            {
                if (TaskFile == true)
                {
                    //先调ws
                    if (TaskOrder == "1")
                    {
                        strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：先调用WebService,后文件传输。" + '\t' + CRLF;
                        if (ExecuteWS() == true)
                        {
                            OnFileTransfer(TaskId);
                        }
                    }
                    //先传文件
                    else if (TaskOrder == "2")
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：先文件传输,后调用WebService。" + '\t' + CRLF;
                        OnFileTransfer(TaskId);
                        if (blnExec == true)
                        {
                            if (blnWS == true)
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：文件传输处理成功，开始调用WebService方法。" + '\t' + CRLF;
                                ExecuteWS();
                            }
                            else
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：不调用WebService方法。" + '\t' + CRLF;
                            }
                        }
                        else
                        {
                            strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：文件传输处理失败，不调用WebService方法。" + '\t' + CRLF;
                            blnExec = false;
                        }
                    }
                }
                else
                {
                    strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：开始调用WebService方法。" + '\t' + CRLF;
                    ExecuteWS();
                }
            }
            else if (TaskFile == true)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：开始文件传输。" + '\t' + CRLF;
                OnFileTransfer(TaskId);

            }
        }


        /// <summary>
        /// 传输先后选择
        /// </summary>
        public void SelectExecute(DataSet ds)
        {
            // Update 2008/6/3 begin 检查ftp通讯是否正常通讯
            if (TaskFile == true)
            {
                FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                string errrorMsg = null;
                if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
                {
                    strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：通讯失败。" + '\t' + "0";
                    FileControl fw = new FileControl();
                    fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                    blnExec = false;
                    blnfinished = false;
                    return;
                }
            }
            // Update 2008/6/3 end

            //调用ws
            if (TaskWS == true)
            {
                if (TaskFile == true)
                {
                    //先调ws
                    if (TaskOrder == "1")
                    {
                        strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：先调用WebService,后文件传输。" + '\t' + CRLF;

                        if (ExecuteWS(ds) == true)
                        {
                            OnFileTransfer(TaskId);
                        }

                    }
                    //先传文件
                    else if (TaskOrder == "2")
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：先文件传输,后调用WebService。" + '\t' + CRLF;
                        OnFileTransfer(TaskId);
                        if (blnExec == true)
                        {
                            if (blnWS == true)
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：文件传输处理成功，开始调用WebService方法。" + '\t' + CRLF;
                                ExecuteWS(ds);
                            }
                            else
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：不调用WebService方法。" + '\t' + CRLF;
                            }
                        }
                        else
                        {
                            strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：文件传输处理失败，不调用WebService方法。" + '\t' + CRLF;
                            blnExec = false;
                        }
                    }
                }
                else
                {
                    strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：开始调用WebService方法。" + '\t' + CRLF;
                    ExecuteWS(ds);
                }
            }
            else if (TaskFile == true)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：开始文件传输。" + '\t' + CRLF;
                OnFileTransfer(TaskId);

            }
        }

        /// <summary>
        /// 传输先后选择
        /// </summary>
        public void SelectExecuteDt(DataTable dt, ArrayList arlList)
        {
            // Update 2008/6/3 begin 检查ftp通讯是否正常通讯
            if (TaskFile == true)
            {
                FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                string errrorMsg = null;
                if (ftpUpDown.chkFTPOpen(out errrorMsg) == false)
                {
                    strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：通讯失败。" + '\t' + "0";
                    FileControl fw = new FileControl();
                    fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                    blnExec = false;
                    blnfinished = false;
                    return;
                }
            }
            // Update 2008/6/3 end

            //调用ws
            if (TaskWS == true)
            {
                if (TaskFile == true)
                {
                    //先调ws
                    if (TaskOrder == "1")
                    {
                        strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：先调用WebService,后文件传输。" + '\t' + CRLF;

                        if (ExecuteWSDt(dt, arlList) == true)
                        {
                            OnFileTransfer(TaskId);
                        }

                    }
                    //先传文件
                    else if (TaskOrder == "2")
                    {
                        strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：先文件传输,后调用WebService。" + '\t' + CRLF;
                        OnFileTransfer(TaskId);
                        if (blnExec == true)
                        {
                            if (blnWS == true)
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：文件传输处理成功，开始调用WebService方法。" + '\t' + CRLF;
                                ExecuteWSDt(dt, arlList);
                            }
                            else
                            {
                                strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：不调用WebService方法。" + '\t' + CRLF;
                            }
                        }
                        else
                        {
                            strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：文件传输处理失败，不调用WebService方法。" + '\t' + CRLF;
                            blnExec = false;
                        }
                    }
                }
                else
                {
                    strWs = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：开始调用WebService方法。" + '\t' + CRLF;
                    ExecuteWSDt(dt, arlList);
                }
            }
            else if (TaskFile == true)
            {
                strlog = strStartLog + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：开始文件传输。" + '\t' + CRLF;
                OnFileTransfer(TaskId);

            }
        }

        /// <summary>
        /// 设定数据检查Timer参数
        /// </summary>
        internal void GetTimerStart()
        {
            //到达时间的时候执行事件；
            Timer t = new Timer(6 * 10000);
            t.Elapsed += new ElapsedEventHandler(TaskTimer_Elapsed);
            t.AutoReset = true;
            t.Enabled = true;
            t.Start();
        }

        /// <summary>
        /// timer事件
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>

        private void TaskTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            //add by xsq 20110929 任务执行检查
            FileControl fw = new FileControl();

            //end by xsq 20110929
            try
            {

                ////时间的比对          
                DateTime dtNow = System.DateTime.Now;
                if (checkNextTime(dtNow) == true)
                {

                    //写执行时间的日志
                    string slog = "任务号--" + TaskId + " 执行的时间：" + dtNow.Date.ToString() + '\t' + dtNow.ToShortTimeString();
                    //FileControl fw = new FileControl();
                    fw.WriteFile(slog, TaskLogPath, "自动执行-" + TaskId + "-" + dtNow.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + ".txt");

                    slog = string.Empty;

                    if (blnfinished == false)
                    {
                        blnfinished = true;
                        TaskDate = dtNow.ToShortDateString();
                        LogFileName = dtNow.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + "-" + TaskId + ".txt";

                        //bool blnReturn = UI.PROXY<bool>.CallService("BLCommon", "LockTask", taskid, "1", loginUser);
                        //if (blnReturn == false)
                        //{
                        //    fw.WriteFileLog("任务被锁。", TaskLogPath, LogFileName);
                        //    return;
                        //}

                        blnExec = true;
                        strlog = null;
                        strWs = null;
                        strStartLog = null;
                        //     int intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "开始处理：服务定时处理任务--" + taskid + "(" + TaskName + ")");
                        strStartLog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + dtNow.ToString() + '\t' + "开始处理：服务定时处理任务, 执行内容如下:" + '\t' + CRLF;
                        SelectExecute();

                        //任务执行结束
                        strlog = null;
                        if (blnExec == false)
                        {
                            //          intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "06", "结束处理：本次任务处理失败--" + taskid + "(" + TaskName + ")");
                            strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理失败" + '\t' + "0";
                        }
                        else
                        {
                            //         intReturn = UI.PROXY<int>.CallService("M90_Alert", "WriteLog", "03", "01", "结束处理：本次任务处理成功--" + taskid + "(" + TaskName + ")");
                            strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理成功" + '\t' + "1";

                        }

                        ////add by xsq 20110929 释放任务
                        //bool blnLock = UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                        ////end by xsq 20110929 释放任务

                        //slog = "任务号--" + TaskId + " 释放任务" + blnLock.ToString();
                        fw = new FileControl();
                        fw.WriteFile(slog, TaskLogPath, "自动执行-" + TaskId + "-" + dtNow.ToString("yyyyMMdd", DateTimeFormatInfo.InvariantInfo) + ".txt");


                        //strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理" + (blnExec == false ? "失败" : "成功") + "\t";
                        // FileControl fw = new FileControl();
                        fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

                        #region 成功后发出报警信息
                        //add by zhouyu 2014-1-14;begin
                        try
                        {
                            string strSysType = "";
                            string strResult = "0";
                            //add by zhouyu 2014-16;begin
                            //关键任务
                            DataTable dtTaskListA = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9101", "01");
                            //凌晨任务
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
                                //取得要监控报警的任务号及要监控报警的处理状态（0 失败 1成功） 
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
                                                //将标志置为1
                                                int flag = UI.PROXY<int>.CallService("M07F0904_FileAccept", "UpdateMessageErr", drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_LOG_DATE"].ToString());
                                                SendErrMsg objSendErrMsg = new SendErrMsg();
                                                //"店号", "系统编号", "任务号", "是否成功(0 失败 1 成功 2 正在执行中)", "消息编号"
                                                string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_RESULT"].ToString(), drMessageErr["MER_RESULT"].ToString());
                                                fw.WriteFileLog("发送报警信息：" + strSysType + "系统" + taskid + "任务 " + strResult + strTest + "", TaskLogPath, LogFileName);//测试用LOG 20120921                                           
                                            }
                                        }
                                        //add by zhouyu 2014-1-16;end
                                    }
                                }
                                //end
                            }
                            catch (Exception exCeption)
                            {
                                //fw.WriteFileLog("报警发生异常！" + exCeption + "", TaskLogPath, LogFileName);//测试用LOG 20120921

                                this.SetSendMessage(dtTaskListA, dtTaskListB, strSysType, taskid, strResult);//add by zhouyu 2014-1-16

                                //add by zhouyu 2014-1-16;end
                                //SendErrMsg objSendErrMsg = new SendErrMsg();
                                ////"店号", "系统编号", "任务号", "是否成功(0 失败 1 成功 2 正在执行中)", "消息编号" 
                                //string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult); 
                                //fw.WriteFileLog("发送报警信息：" + strSysType + "系统" + taskid + "任务 " + strResult + strTest + "", TaskLogPath, LogFileName);//测试用LOG 20120921
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
                    string slog = "任务号--" + TaskId + " 闲置的时间：" + dtNow.Date.ToString() + '\t' + dtNow.ToShortTimeString();
                    // FileOperate.WriteFile(slog, @"C:\SLOG", "闲置-"+TaskId + ".txt");     
                }
            }
            catch (Exception ex)
            {
                strlog = null;
                strlog = TaskDate + '\t' + TaskId + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "结束处理：本次任务处理失败(自动定时异常:" + ex.Message + ")" + '\t' + "0";
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);

                #region 失败后发出报警信息
                //add by zhouyu 2014-1-14;begin
                try
                {
                    string strSysType = "";
                    string strResult = "0";
                    //add by zhouyu 2014-1-16;begin
                    //关键任务
                    DataTable dtTaskListA = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9101", "01");
                    //凌晨任务
                    DataTable dtTaskListB = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaster", "9101", "02");
                    //add by zhouyu 2014-1-16;end
                    if (string.IsNullOrEmpty(strSysType))
                    {
                        strSysType = "MA";
                    }
                    try
                    {
                        //取得要监控报警的任务号及要监控报警的处理状态（0 失败 1成功） 
                        DataTable dtTaskList = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetSysMaser", "9100");

                        foreach (DataRow drTaskList in dtTaskList.Rows)
                        {
                            if (drTaskList["TSM_SUB_VALUE"].ToString().Contains(taskid))
                            {
                                if (drTaskList["TSM_SUB_VALUE2"].ToString().Contains(strResult))
                                {
                                    this.SetSendMessage(dtTaskListA, dtTaskListB, strSysType, taskid, strResult);
                                    //SendErrMsg objSendErrMsg = new SendErrMsg();
                                    // //"店号", "系统编号", "任务号", "是否成功(0 失败 1 成功 2 正在执行中)", "消息编号"
                                    // string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult);
                                }
                            }
                        }
                        //end
                    }
                    catch (Exception exCeption)
                    {
                        //fw.WriteFileLog("报警发生异常！" + exCeption + "", TaskLogPath, LogFileName);//测试用LOG 20120921

                        this.SetSendMessage(dtTaskListA, dtTaskListB, strSysType, taskid, strResult); //add by zhouyu 2014-1-16;

                        //SendErrMsg objSendErrMsg = new SendErrMsg();
                        ////"店号", "系统编号", "任务号", "是否成功(0 失败 1 成功 2 正在执行中)", "消息编号" 
                        //string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult);  
                        //fw.WriteFileLog("发送报警信息：" + strSysType + "系统" + taskid + "任务 " + strResult + strTest + "", TaskLogPath, LogFileName);//测试用LOG 20120921
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
        /// 获取任务的参数 从参数配置文件FilesTransfer.xml中读取任务的参数
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

                if (TaskFile == true)  //文件传输的参数获取
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
                            //Ftp备份路径
                            if (TaskBackupPath.Substring(TaskBackupPath.Length - 1) != "/")
                            { TaskBackupPath = TaskBackupPath + "/"; }
                        }
                        else
                        {
                            if (TaskSourcePath.Substring(TaskSourcePath.Length - 1) != @"\")
                            {
                                TaskSourcePath = TaskSourcePath + @"\";
                            }
                            //本地备份路径
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

                    if (TaskFrequencyType == "D")   //每天执行参数值
                    {

                        if (TaskDayCyType == "1") //执行一次的时间
                        {
                            TaskDayCyOneTime = (DateTime)drow["TaskDayCyOneTime"];
                        }
                        else if (TaskDayCyType == "2")  //反复执行的周期间隔时间
                        {
                            TaskDayCyRepeat = (int)drow["TaskDayCyRepeat"];
                            TaskDayCyUnit = drow["TaskDayCyUnit"].ToString();
                            TaskDayCyBeginTime = (DateTime)drow["TaskDayCyBeginTime"];
                            TaskDayCyEndTime = (DateTime)drow["TaskDayCyEndTime"];
                        }

                    }
                    else if (TaskFrequencyType == "W")  //每周执行参数值
                    {
                        TaskWK1 = (drow["TaskWK1"] == DBNull.Value ? false : (bool)drow["TaskWK1"]);
                        TaskWK2 = (drow["TaskWK2"] == DBNull.Value ? false : (bool)drow["TaskWK2"]);
                        TaskWK3 = (drow["TaskWK3"] == DBNull.Value ? false : (bool)drow["TaskWK3"]);
                        TaskWK4 = (drow["TaskWK4"] == DBNull.Value ? false : (bool)drow["TaskWK4"]);
                        TaskWK5 = (drow["TaskWK5"] == DBNull.Value ? false : (bool)drow["TaskWK5"]);
                        TaskWK6 = (drow["TaskWK6"] == DBNull.Value ? false : (bool)drow["TaskWK6"]);
                        TaskWK7 = (drow["TaskWK7"] == DBNull.Value ? false : (bool)drow["TaskWK7"]);

                        if (TaskDayCyType == "1") //执行一次的时间
                        {
                            TaskDayCyOneTime = (DateTime)drow["TaskDayCyOneTime"];
                        }
                        else if (TaskDayCyType == "2")  //反复执行的周期间隔时间
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
        /// 执行的时间的比较
        /// </summary>
        /// <param name="dtNow"></param>
        /// <returns></returns>
        private bool checkNextTime(DateTime dtNow)
        {
            bool flag = false;
            if (TaskTimeType == "1")     //执行一次的时间
            {
                if (dtNow.Date.CompareTo(TaskOneDate.Date) == 0 & dtNow.ToShortTimeString().CompareTo(TaskOneTime.ToShortTimeString()) == 0)
                {
                    NowDate = TaskOneDate;
                    NowTime = TaskOneTime;
                    flag = true;
                }
            }
            else if (TaskTimeType == "2")　　//反复执行时间
            {

                if (TaskFrequencyType == "D")   //每天执行参数值
                {
                    NowDate = dtNow.Date;
                    if (TaskDayCyType == "1") //执行一次的时间
                    {
                        nextDate = NowDate.AddDays(1);
                        if (dtNow.ToShortTimeString().CompareTo(TaskDayCyOneTime.ToShortTimeString()) == 0)
                        {
                            NowTime = TaskDayCyOneTime;
                            flag = true;
                        }
                    }
                    else if (TaskDayCyType == "2")  //反复执行的周期间隔时间
                    {
                        DateTime startDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyBeginTime.ToShortTimeString());
                        DateTime endDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyEndTime.ToShortTimeString());
                        DateTime Nowtime = Convert.ToDateTime(dtNow.Year + "-" + dtNow.Month + "-" + dtNow.Day + " " + dtNow.ToShortTimeString());

                        if (startDate > endDate) 　　//跨天时间
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
                else if (TaskFrequencyType == "W")  //每周执行参数值
                {
                    bool blnWK = false;

                    DateTime startDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyBeginTime.ToShortTimeString());
                    DateTime endDate = Convert.ToDateTime(preDate.Year + "-" + preDate.Month + "-" + preDate.Day + " " + TaskDayCyEndTime.ToShortTimeString());
                    DateTime Nowtime = Convert.ToDateTime(dtNow.Year + "-" + dtNow.Month + "-" + dtNow.Day + " " + dtNow.ToShortTimeString());
                    if (TaskDayCyType == "2")
                    {
                        if (startDate > endDate) 　　//跨天时间
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
                        if (TaskDayCyType == "1") //执行一次的时间
                        {
                            nextDate = NowDate.AddDays(1);
                            if (dtNow.ToShortTimeString().CompareTo(TaskDayCyOneTime.ToShortTimeString()) == 0)
                            {
                                NowTime = TaskDayCyOneTime;
                                flag = true;
                            }
                        }
                        else if (TaskDayCyType == "2")  //反复执行的周期间隔时间
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
        /// ws调用
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
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。WebService方法处理成功." + '\t';// +CRLF;
                }
                else
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。WebService方法处理失败." + '\t' + "0"; //+ CRLF;
                    blnExec = false;
                }

                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);

                return flag;
            }
            catch (Exception ex)
            {
                strWs = strWs + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。调用WebService方法处理异常:" + ex.Message + '\t' + "0" + CRLF;
                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //add by xsq 20110929 释放任务
                //UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                //end by xsq 20110929 释放任务
                return false;
            }

        }


        /// <summary>
        /// ws调用
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
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。WebService方法处理成功." + '\t';// +CRLF;
                }
                else
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。WebService方法处理失败." + '\t' + "0"; //+ CRLF;
                    blnExec = false;
                }

                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                return flag;
            }
            catch (Exception ex)
            {
                strWs = strWs + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。调用WebService方法处理异常:" + ex.Message + '\t' + "0" + CRLF;
                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //释放LOCK TASK
                blnExec = false;
                //UI.PROXY<object>.CallService("BLCommon", "ReleaseLockTask", taskid, this.loginUser);

                return false;
            }

        }

        /// <summary>
        /// ws调用
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
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。WebService方法处理成功." + '\t';// +CRLF;
                }
                else
                {
                    strWs = strWs + TaskDate + '\t' + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。WebService方法处理失败." + '\t' + "0"; //+ CRLF;
                    blnExec = false;
                }

                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                return flag;
            }
            catch (Exception ex)
            {
                strWs = strWs + taskid + '\t' + TaskName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：类：" + TaskWSClass + ",方法：" + TaskWSMethod + " 。调用WebService方法处理异常:" + ex.Message + '\t' + "0" + CRLF;
                FileControl fw = new FileControl();
                fw.WriteFileLog(strWs, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //释放LOCK TASK
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
                case "R": //Remoting 本次定制不做此功能
                    break;
                case "F": //FTP
                    {
                        if (TaskMode == "S")  // 上传
                        {
                            OnFileTransfer_Send();
                        }
                        else if (TaskMode == "D")  //下载
                        {
                            OnFileTransfer_Down();
                        }
                        break;
                    }
            }
        }

        //上传
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

            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：开始上传文件 " + '\t' + CRLF;
            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

            try
            {
                // 文件上传方式 
                string FileChk = null;


               FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);

               SFTPOperation SFTPHelper = new SFTPOperation(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword);
              

                #region 上传

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
                    #region  文件上传
                    for (int i = 0; i < fi.Length; i++)
                    {

                        bool blnSend = true;
                        blnZip = false;
                        // 1--上传文件到目的服务器端
                        FileInfo sourcefile = new FileInfo(fi[i]);
                        if (TaskChkChk == true)
                        {

                            //若不存在.chk文件，则不传文件；
                            FileChk = null;
                            if (sourcefile.Extension.ToLower() == ".zip")
                            {
                                blnZip = true;
                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                {
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 与上传文件配对的chk文件不存在。本次不上传此文件。" + '\t' + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    blnSend = false;
                                    blnExec = false;
                                    goto ExitNext;
                                }
                            }
                            else if (sourcefile.Extension.ToLower() == ".chk")  //检查chk是否存在有相对应的zip文件
                            {
                                goto ExitChk;
                            }

                        }
                        // 2--检查上传的文件正确性 
                        if (sourcefile.Extension.ToLower() == ".zip")
                        {
                            //edit by ssh 2013-04-17  判断文件是否正被另外的进程使用
                            if (FileOperator.IsFileInUse(fi[i]) == true)
                            {
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 此文件正被占用。" + '\t' + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                System.Threading.Thread.Sleep(20000);
                            }
                            //edit by ssh 2013-04-16 若此次循环的文件已被（例：另一进程）移除，则继续下次循环
                            if (!File.Exists(fi[i]))
                            {
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " 此文件已被删除…………。" + "\t" + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                continue;
                            }
                            //end 
                            FileOperator.CheckDirectory(@"C:\TEMP");
                            if (FilesZip.UnZip(fi[i], @"C:\TEMP") == false)
                            {
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " 解压文件异常。本次不上传此文件。" + "\t" + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                blnSend = false;
                                blnExec = false;
                                goto ExitNext;
                            }
                        }
                        //3--上传文件
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

                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " 文件上传，重试第一次" + "\t" + "0" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                System.Threading.Thread.Sleep(2000);

                                if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, mytaskid, out messglog) == false)
                                {
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " 文件上传，重试第二次" + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                    System.Threading.Thread.Sleep(2000);

                                    if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, mytaskid, out messglog) == false)
                                    {
                                        strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " 上传文件失败。失败原因：" + messglog + "\t" + "0" + CRLF;
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
                            // 4--比较本地文件与目的端文件大小
                            long fileSize = ftpUpDown.GetFileSize(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"));

                            if (fileSize == sourcefile.Length)
                            {
                                if (TaskChkChk == true)
                                {

                                    //上传配对的chk文件
                                    if (blnZip == true)
                                    {
                                        if (ftpUpDown.Upload(FileChk, TaskDestinationPath, TaskSourcePath, out messglog) == false)
                                        {
                                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + FileChk + " 上传文件失败。失败原因：" + messglog + "\t" + "0" + CRLF;
                                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;
                                        }
                                        long fileChkSize = ftpUpDown.GetFileSize(TaskDestinationPath, FileChk.Replace(TaskSourcePath, ""));
                                        if (fileChkSize != 0)
                                        {
                                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + FileChk + " 上传文件失败。失败原因：上传文件与本地文件大小不一致." + "\t" + "0" + CRLF;
                                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;
                                        }

                                    }

                                }

                                //文件重命名20111220

                                FileInfo fileinfo = new FileInfo(fi[i]);
                                if (ftpUpDown.FtpFileRename(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"), fileinfo.Name) == false)
                                {
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " FTP文件重命名，重试第一次" + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                    System.Threading.Thread.Sleep(2000);
                                    if (ftpUpDown.FtpFileRename(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"), fileinfo.Name) == false)
                                    {
                                        strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " FTP文件重命名，重试第二次" + "\t" + "0" + CRLF;
                                        FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);

                                        System.Threading.Thread.Sleep(2000);

                                        if (ftpUpDown.FtpFileRename(TaskDestinationPath, fi[i].Replace(TaskSourcePath, "").Replace(".zip", ".zi"), fileinfo.Name) == false)
                                        {
                                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " FTP文件重命名失败。" + "\t" + "0" + CRLF;
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
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 上传文件与源文件大小不一致。本次上传失败。" + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    blnSend = false;
                                    blnExec = false;
                                    goto ExitNext;
                                }
                                else
                                {
                                    //strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 上传文件与源文件大小不一致。本次上传失败。" + "\t" + "0" + CRLF;
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + Path.GetDirectoryName(fi[i]) + " 本地路径中的源文件大小为：" + sourcefile.Length + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + TaskDestinationPath + " FTP路径对应子目录中的上传文件大小为：" + fileSize + "\t" + "0" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                    continue;
                                }

                            }
                    }
                        dLFiles.ToArray();
                        if (blnSend)
                        {
                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 上传文件成功。" + "总共记录数：" + fi.Length + "还剩:" + (fi.Length - (i + 1)) + "\t" + "1" + CRLF;
                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                        }
                        else
                        {
                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 上传文件失败。" + "\t" + "1" + CRLF;
                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                            blnExec = false;
                        }
                    ExitChk: messglog = null;
                    }
                    #endregion
                    #region 文件备份与删除
                    //处理备份与删除
                    ////??????????????????????????????????????????????????????????????
                    for (int i = 0; i < fi.Length; i++)
                    {
                        blnZip = false;
                        // 1--上传文件到目的服务器端
                        FileInfo sourcefile = new FileInfo(fi[i]);

                        //若不存在.chk文件，则不传文件；
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

                        // 4-zip备份文件
                        string backFile = TaskBackupPath + fi[i].Replace(TaskSourcePath, @"\");
                        FileInfo thefile = new FileInfo(backFile);
                        FileOperator.CheckDirectory(thefile.DirectoryName);

                        if (File.Exists(backFile))
                        {
                            //若备份目录存在有同名文件，则新备份的文件重新命名。不删除原来的文件
                            // File.Delete(backFile);
                            string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                            backFile = backFile.Replace(thefile.Extension, strhms) + thefile.Extension;

                        }
                        //把备份和删除合并更改为文件的移动
                        if (File.Exists(fi[i]))   //edit by ssh 2013-04-16 判断是否有需要备份的文件
                        {
                            File.Copy(fi[i], backFile, true);

                            strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 备份文件成功。" + "\t" + "1" + CRLF;
                            FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                        }
                        // 4-chk备份文件
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
                                    //若备份目录存在有同名文件，则新备份的文件重新命名。不删除原来的文件
                                    string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                                    bkChkFile = bkChkFile.Replace(Chkfile.Extension, strhms) + Chkfile.Extension;

                                }
                                //备份ZIP
                                if (File.Exists(FileChk))   //edit by ssh 2013-04-16 判断是否有需要备份的文件
                                {
                                    File.Copy(FileChk, bkChkFile, true);
                                }
                            }
                        }
                        //edit by ssh 2013-04-16 移至上面备份方法处
                        //strlog =  taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 备份文件成功。" + "\t" + "1" + CRLF;
                        //FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);


                        //------------------------------------------------------
                        if (TaskId != "M4001")
                        {
                            //  5--zip删除文件 
                            if (File.Exists(fi[i]))
                            {
                                File.Delete(fi[i]);
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 删除文件成功。" + "\t" + "1" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                            }

                            //  5--chk删除文件 
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
                            //当执行任务M4001：对档给SC时备份删除失败不抛错，跳过继续执行
                            try
                            {
                                //  5--zip删除文件 
                                if (File.Exists(fi[i]))
                                {
                                    File.Delete(fi[i]);
                                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 删除文件成功。" + "\t" + "1" + CRLF;
                                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                }

                                //  5--chk删除文件 
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
                                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 文件删除失败，继续备份删除下一个文件。异常信息:" + ex.Message + "\t" + "1" + CRLF;
                                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                                continue;
                            }
                        }
                    //end
                    ExitChk: FileChk = null;
                    }
                    #endregion
                /////???????????????????????????????????????????????????

                    ////失败时做文件的全备份
                ExitNext: messglog = null;
                    ////失败时做文件的全备份

                }

                else
                {
                    strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： 任务中没有上传的文件。" + "\t" + "1" + CRLF;
                    FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                }

                #endregion  上传
                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：本次任务文件传输" + (blnExec == false ? "失败." : "成功.") + "\t";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
            }
            catch (Exception ex)
            {
                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：异常:" + ex.Message + "\t" + "0";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
                FileControl fw = new FileControl();
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //add by xsq 20110929 释放任务
                //UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                //end by xsq 20110929 释放任务
            }
            finally
            {
                //失败时做文件上传的全备份
                if (blnExec == false && TaskMode == "S")
                {
                    for (int i = 0; i < FileBackList.Length; i++)
                    {
                        FileInfo sourcefile = new FileInfo(FileBackList[i]);
                        string strhms = "-ERROR-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                        // 4-zip备份文件
                        string fileName = sourcefile.Name.Replace(sourcefile.Extension, "") + strhms + sourcefile.Extension;
                        string backFile = TaskBackupPath + FileBackList[i].Replace(TaskSourcePath, @"\").Replace(sourcefile.Name, fileName);

                        FileInfo thefile = new FileInfo(backFile);
                        FileOperator.CheckDirectory(thefile.DirectoryName);

                        //把备份和删除合并更改为文件的移动
                        File.Copy(FileBackList[i], backFile, true);
                    }
                }
                //失败时做文件的全备份
                strlog = taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "上传文件结束" + "\t";
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
                // 文件上传方式 
                string FileChk = null;

                //switch (TransferType)
                //{
                //case "R": //Remoting 本次定制不做此功能
                //    break;
                //case "F": //FTP
                //    {
                FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                #region 下载
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
                                //edit by ssh 2014-11-12  M5012任务判断文件完整与否的文件.ok文件
                                if (TaskId == "M5012")
                                {
                                    FileChk = fi[i] +".ok";
                                }
                                else
                                {
                                    FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                }
                                //若无不存在.chk文件，则不传文件；

                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                {
                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件的chk文件不存在。本次不下载此文件。" + "\t" + "0" + CRLF;
                                    blndown = false;
                                    blnExec = false;
                                    goto ExitNext;
                                }
                            }
                            else if (sourcefile.Extension.ToLower() == ".chk" || sourcefile.Extension.ToLower() == ".ok")  //检查chk是否存在有相对应的zip文件
                            {
                                goto ExitChk;
                            }
                        }
                        long filesize = 0;

                        // 1--从FTP上下载文件
                        FileInfo fileInf = new FileInfo(TaskDestinationPath + fi[i]);
                        FileOperator.CheckDirectory(fileInf.DirectoryName);
                        if (fileInf.Exists)
                        {
                            File.Delete(TaskDestinationPath + fi[i]);    //？？？ 若本地存在文件做何处理？
                        }

                        string downlog = null;
                        //A-下栽zip文件
                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, fi[i], out  filesize, out downlog) == true)
                        {
                            //重新获取下载文件
                            FileInfo f = new FileInfo(TaskDestinationPath + fi[i]);

                            // 2--验证下载的文件大小与FTP上的文件大小比较再做备份和删除
                          
                                if (TaskId == "M5012")
                                {
                                    filesize = f.Length;
                                }

                            if (filesize == f.Length )
                            {
                                //// 3--检查下载的文件正确性  ？？？
                                if (fileInf.Extension.ToLower() == ".zip")
                                {
                                    //add by jdb 20081021
                                    FileOperator.CheckDirectory(@"C:\TEMP");
                                    //end by jdb 20081021
                                    if (FilesZip.UnZip(TaskDestinationPath + fi[i], @"C:\TEMP") == false)
                                    {
                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 解压文件异常。下载的文件不完整。" + "\t" + "0" + CRLF;
                                        blndown = false;
                                        goto ExitNext;
                                    }
                                }

                                ////B-下载对应的chk.文件
                                if (blnZip == true)
                                {
                                    if (TaskChkChk == true)
                                    {
                                        filesize = 0;
                                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, FileChk, out  filesize, out downlog) == true)
                                        {
                                            //重新获取下载文件
                                            FileInfo fk = new FileInfo(TaskDestinationPath + FileChk);
                                            if (filesize != fk.Length)
                                            {
                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件与源文件大小不一致。下载文件的不完整1。" + "\t" + "0" + CRLF;
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
                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件与源文件大小不一致。下载文件的不完整2。" + "\t" + "0" + CRLF;
                                goto ExitNext;
                            }
                        }
                        else
                        {
                            blndown = false;
                            blnExec = false;
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件失败。失败原因：" + downlog + "\t" + "0" + CRLF;
                            goto ExitNext;
                        }
                        dLFiles.ToArray();
                        if (blndown)
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件成功。" + "\t" + "1" + CRLF;
                        }
                        else
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件失败。" + "\t" + "0" + CRLF;
                            blnExec = false;
                        }
                    ExitChk: message = null;

                    }

                    #region
                    if (TaskId == "I2090" || TaskId == "I2092")
                    {
                        for (int p = 0; p < fi.Length; p++)
                        {

                            // 8.1 组装Zip完整文件名
                            string strZipFileName = Path.GetFileName(fi[p]);

                            // 取不含".ZIP"的文件名
                            string strZipFileNameWithout = Path.GetFileNameWithoutExtension(fi[p]);

                            // 取ZIP文件的日期
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
                            //获取文件包的修改日期
                            FileInfo f = new FileInfo(TaskDestinationPath + fi[p]);
                            string StoreTransDate = f.LastWriteTime.ToString();

                            // 取ZIP文件的店铺代码 和现在的"营业时点"( 用于营收接收SC任务时写“营收排程表”)
                            string ZipStoreId = "";

                            if (TaskId == "I2092")  //2代店号不转
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

                                //1店铺号
                                //2日期
                                //3排程类型  01 订购 02 营收 订购处理时填01 ,营收接收文件时填02
                                //4便次组号/营收时点号  01表示0：00，02表示14：00
                                //5排程内/排程外  01 排程内日结生成  02 排程外 
                                //6店铺订购/营收上传时间 (集配信提供功能取得)   
                                //7店铺订购/营收下载标志 01 未下载  02 已下载 
                                //8鲜食订购标志  01 未订购 02 已订购
                                //9处理状态 00未处理  01 成功 02 失败
                                //10失败原因
                                //11操作人
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
                    //// Begin 2008/5/15 全部下载文件成功后，开始做文件的备份和删除

                    for (int i = 0; i < fi.Length; i++)
                    {
                        blnZip = false;
                        FileInfo sourcefile = new FileInfo(fi[i]);
                        FileChk = null;
                        if (sourcefile.Extension.ToLower() == ".zip")
                        {
                            blnZip = true;
                            //edit by ssh 2014-11-12  M5012任务判断文件完整与否的文件.ok文件
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
                        //备份zip文件
                        if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, fi[i], out message) == true)
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + message + "\t" + "1" + CRLF;
                            if (TaskChkChk == true)
                            {
                                //备份chk文件
                                if (blnZip == true)
                                {
                                    if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, FileChk, out message) == false)
                                    {
                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + FileChk + message + "\t" + "0" + CRLF;
                                        blnExec = false;
                                        goto ExitNext;
                                    }
                                }
                            }
                        }
                        else
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + message + "\t" + "0" + CRLF;
                            blnExec = false;
                            goto ExitNext;
                        }
                    ExitChk: message = null;
                    }
                    ///// End 2008/5/15 全部下载文件成功后，开始做文件的备份和删除

                }
                else
                {
                    //by xieming add 2011-09-09  如果导入失败，则本地路径下还有文件，如果包没问题，则可继续导入包、
                    //                           如果包有问题，则将新包放至FTP上下，再下载则会覆盖。
                    string[] CurIsExistsfile = new FileControl().GetFileList(TaskDestinationPath, TaskFileType, true);

                    if (CurIsExistsfile != null)
                    {
                        blnWS = true;
                    }
                    else
                    {

                        //by xieming add 2011-09-09
                        //没有文件则不再调用ws
                        if (message == null)
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：任务中没有下载的文件。" + "\t" + "1" + CRLF;
                        }
                        else
                        {
                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：任务中没有下载的文件。" + "\t" + "1" + CRLF;
                        }
                        blnWS = false;
                    }
                }
            ExitNext: message = null;

                #endregion  下载
                //        }

                //}

                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：本次任务文件传输" + (blnExec == false ? "失败." : "成功.") + "\t";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);


            }
            catch (Exception ex)
            {
                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：异常:" + ex.Message + "\t" + "0";
                FileControl fw = new FileControl();
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
                //add by xsq 20110929 释放任务
                // UI.PROXY<bool>.CallService("BLCommon", "ReleaseLockTask", TaskId, loginUser);
                //end by xsq 20110929 释放任务
            }
        }

        /// <summary>
        /// 一代的6位店号转换为2代的物理店号
        /// 返回值：传入旧的店号，或新的店号，得到二代系统的物理店号
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
        /// 传送文件 (改造方法，备份）
        /// </summary>
        /// <param name="taskid">任务号</param>

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
                // 文件上传方式 
                string FileChk = null;

                switch (TransferType)
                {
                    case "R": //Remoting 本次定制不做此功能
                        break;
                    case "F": //FTP
                        {
                            FileTransfer.FileFtpUpDown ftpUpDown = new FileFtpUpDown(TaskServerIP, TaskServerPort.ToString(), TaskUser, TaskPassword, TaskFtpPassive);
                            #region 上传
                            if (TaskMode == "S")  // 上传
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
                                        // 1--上传文件到目的服务器端
                                        FileInfo sourcefile = new FileInfo(fi[i]);
                                        if (TaskChkChk == true)
                                        {

                                            //若不存在.chk文件，则不传文件；
                                            FileChk = null;
                                            if (sourcefile.Extension.ToLower() == ".zip")
                                            {
                                                blnZip = true;
                                                FileChk = fi[i].Replace(sourcefile.Name, "") + sourcefile.Name.Replace(sourcefile.Extension, ".chk");
                                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                                {
                                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 与上传文件配对的chk文件不存在。本次不上传此文件。" + '\t' + "0" + CRLF;
                                                    blnSend = false;
                                                    blnExec = false;
                                                    goto ExitNext;
                                                }
                                            }
                                            else if (sourcefile.Extension.ToLower() == ".chk")  //检查chk是否存在有相对应的zip文件
                                            {
                                                goto ExitChk;
                                            }

                                        }
                                        // 2--检查上传的文件正确性 
                                        if (sourcefile.Extension.ToLower() == ".zip")
                                        {
                                            FileOperator.CheckDirectory(@"C:\TEMP");
                                            if (FilesZip.UnZip(fi[i], @"C:\TEMP") == false)
                                            {
                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " 解压文件异常。本次不上传此文件。" + "\t" + "0" + CRLF;
                                                blnSend = false;
                                                blnExec = false;
                                                goto ExitNext;
                                            }
                                        }
                                        //3--上传文件
                                        messglog = null;
                                        if (ftpUpDown.Upload(fi[i], TaskDestinationPath, TaskSourcePath, out messglog) == false)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： " + fi[i] + " 上传文件失败。失败原因：" + messglog + "\t" + "0" + CRLF;
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;
                                        }

                                        // 4--比较本地文件与目的端文件大小
                                        long fileSize = ftpUpDown.GetFileSize(TaskDestinationPath, fi[i].Replace(TaskSourcePath, ""));

                                        if (fileSize == sourcefile.Length)
                                        {
                                            if (TaskChkChk == true)
                                            {

                                                //上传配对的chk文件
                                                if (blnZip == true)
                                                {
                                                    if (ftpUpDown.Upload(FileChk, TaskDestinationPath, TaskSourcePath, out messglog) == false)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + FileChk + " 上传文件失败。失败原因：" + messglog + "\t" + "0" + CRLF;
                                                        blnSend = false;
                                                        blnExec = false;
                                                        goto ExitNext;
                                                    }
                                                    long fileChkSize = ftpUpDown.GetFileSize(TaskDestinationPath, FileChk.Replace(TaskSourcePath, ""));
                                                    if (fileChkSize != 0)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + FileChk + " 上传文件失败。失败原因：上传文件与本地文件大小不一致." + "\t" + "0" + CRLF;
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
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 上传文件与源文件大小不一致。本次上传失败。" + "\t" + "0" + CRLF;
                                            blnSend = false;
                                            blnExec = false;
                                            goto ExitNext;

                                        }
                                        dLFiles.ToArray();
                                        if (blnSend)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 上传文件成功。" + "\t" + "1" + CRLF;
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 上传文件失败。" + "\t" + "1" + CRLF;
                                            blnExec = false;
                                        }
                                    ExitChk: messglog = null;
                                    }

                                    //处理备份与删除
                                    ////??????????????????????????????????????????????????????????????
                                    for (int i = 0; i < fi.Length; i++)
                                    {
                                        blnZip = false;
                                        // 1--上传文件到目的服务器端
                                        FileInfo sourcefile = new FileInfo(fi[i]);

                                        //若不存在.chk文件，则不传文件；
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

                                        // 4-zip备份文件
                                        string backFile = TaskBackupPath + fi[i].Replace(TaskSourcePath, @"\");
                                        FileInfo thefile = new FileInfo(backFile);
                                        FileOperator.CheckDirectory(thefile.DirectoryName);

                                        if (File.Exists(backFile))
                                        {
                                            //若备份目录存在有同名文件，则新备份的文件重新命名。不删除原来的文件
                                            // File.Delete(backFile);
                                            string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                                            backFile = backFile.Replace(thefile.Extension, strhms) + thefile.Extension;

                                        }
                                        //把备份和删除合并更改为文件的移动
                                        File.Copy(fi[i], backFile, true);
                                        // 4-chk备份文件
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
                                                    //若备份目录存在有同名文件，则新备份的文件重新命名。不删除原来的文件
                                                    string strhms = "-bak-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                                                    bkChkFile = bkChkFile.Replace(Chkfile.Extension, strhms) + Chkfile.Extension;

                                                }
                                                //备份ZIP
                                                File.Copy(FileChk, bkChkFile, true);
                                            }
                                        }
                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 备份文件成功。" + "\t" + "1" + CRLF;


                                        //------------------------------------------------------

                                        //  5--zip删除文件 
                                        if (File.Exists(fi[i]))
                                        {
                                            File.Delete(fi[i]);
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 删除文件成功。" + "\t" + "1" + CRLF;
                                        }

                                        //  5--chk删除文件 
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

                                    ////失败时做文件的全备份
                                ExitNext: messglog = null;
                                    ////失败时做文件的全备份

                                }

                                else
                                {
                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤： 任务中没有上传的文件。" + "\t" + "1" + CRLF;
                                }
                                break;

                            }
                            #endregion  上传
                            #region 下载
                            else if (TaskMode == "D") //下载
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
                                                //若无不存在.chk文件，则不传文件；

                                                if (FileOperator.ExitFileChk(fi, FileChk) == false)
                                                {
                                                    strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件的chk文件不存在。本次不下载此文件。" + "\t" + "0" + CRLF;
                                                    blndown = false;
                                                    blnExec = false;
                                                    goto ExitNext;
                                                }
                                            }
                                            else if (sourcefile.Extension.ToLower() == ".chk")  //检查chk是否存在有相对应的zip文件
                                            {
                                                goto ExitChk;
                                            }
                                        }
                                        long filesize = 0;

                                        // 1--从FTP上下载文件
                                        FileInfo fileInf = new FileInfo(TaskDestinationPath + fi[i]);
                                        FileOperator.CheckDirectory(fileInf.DirectoryName);
                                        if (fileInf.Exists)
                                        {
                                            File.Delete(TaskDestinationPath + fi[i]);    //？？？ 若本地存在文件做何处理？
                                        }

                                        string downlog = null;
                                        //A-下栽zip文件
                                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, fi[i], out  filesize, out downlog) == true)
                                        {
                                            //重新获取下载文件
                                            FileInfo f = new FileInfo(TaskDestinationPath + fi[i]);

                                            // 2--验证下载的文件大小与FTP上的文件大小比较再做备份和删除
                                            if (filesize == f.Length)
                                            {
                                                //// 3--检查下载的文件正确性  ？？？
                                                if (fileInf.Extension.ToLower() == ".zip")
                                                {
                                                    //add by jdb 20081021
                                                    FileOperator.CheckDirectory(@"C:\TEMP");
                                                    //end by jdb 20081021
                                                    if (FilesZip.UnZip(TaskDestinationPath + fi[i], @"C:\TEMP") == false)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 解压文件异常。下载的文件不完整。" + "\t" + "0" + CRLF;
                                                        blndown = false;
                                                        goto ExitNext;
                                                    }
                                                }

                                                ////B-下载对应的chk.文件
                                                if (blnZip == true)
                                                {
                                                    if (TaskChkChk == true)
                                                    {
                                                        filesize = 0;
                                                        if (ftpUpDown.Download(TaskDestinationPath, TaskSourcePath, FileChk, out  filesize, out downlog) == true)
                                                        {
                                                            //重新获取下载文件
                                                            FileInfo fk = new FileInfo(TaskDestinationPath + FileChk);
                                                            if (filesize != fk.Length)
                                                            {
                                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件与源文件大小不一致。下载文件的不完整3。" + "\t" + "0" + CRLF;
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
                                                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件与源文件大小不一致。下载文件的不完整4。" + "\t" + "0" + CRLF;
                                                goto ExitNext;
                                            }
                                        }
                                        else
                                        {
                                            blndown = false;
                                            blnExec = false;
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件失败。失败原因：" + downlog + "\t" + "0" + CRLF;
                                            goto ExitNext;
                                        }
                                        dLFiles.ToArray();
                                        if (blndown)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件成功。" + "\t" + "1" + CRLF;
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + " 下载文件失败。" + "\t" + "0" + CRLF;
                                            blnExec = false;
                                        }
                                    ExitChk: message = null;

                                    }

                                    //// Begin 2008/5/15 全部下载文件成功后，开始做文件的备份和删除

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
                                        //备份zip文件
                                        if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, fi[i], out message) == true)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + message + "\t" + "1" + CRLF;
                                            if (TaskChkChk == true)
                                            {
                                                //备份chk文件
                                                if (blnZip == true)
                                                {
                                                    if (ftpUpDown.BackUpFtpFile(TaskSourcePath, TaskBackupPath, FileChk, out message) == false)
                                                    {
                                                        strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + FileChk + message + "\t" + "0" + CRLF;
                                                        blnExec = false;
                                                        goto ExitNext;
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：" + fi[i] + message + "\t" + "0" + CRLF;
                                            blnExec = false;
                                            goto ExitNext;
                                        }
                                    ExitChk: message = null;
                                    }
                                    ///// End 2008/5/15 全部下载文件成功后，开始做文件的备份和删除

                                }
                                else
                                {


                                    //by xieming add 2011-09-09  如果导入失败，则本地路径下还有文件，如果包没问题，则可继续导入包、
                                    //                           如果包有问题，则将新包放至FTP上下，再下载则会覆盖。
                                    string[] CurIsExistsfile = new FileControl().GetFileList(TaskDestinationPath, TaskFileType, true);

                                    if (CurIsExistsfile != null)
                                    {
                                        blnWS = true;
                                    }
                                    else
                                    {

                                        //by xieming add 2011-09-09
                                        //没有文件则不再调用ws
                                        if (message == null)
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：任务中没有下载的文件。" + "\t" + "1" + CRLF;
                                        }
                                        else
                                        {
                                            strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：任务中没有下载的文件。" + "\t" + "1" + CRLF;
                                        }
                                        blnWS = false;
                                    }
                                }
                            ExitNext: break;
                            }
                            #endregion  下载
                            break;
                        }
                }

                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：本次任务文件传输" + (blnExec == false ? "失败." : "成功.") + "\t";
                FileOperator.WriteFileLog(strlog, TaskLogPath, LogFileName);
            }

            catch (Exception ex)
            {
                strlog = strlog + taskIdName + '\t' + System.DateTime.Now.ToString() + '\t' + "执行步骤：异常:" + ex.Message + "\t" + "0";
                FileControl fw = new FileControl();
                fw.WriteFileLog(strlog, TaskLogPath, LogFileName);
                blnExec = false;
                blnfinished = false;
            }
            finally
            {
                //失败时做文件上传的全备份
                if (blnExec == false && TaskMode == "S")
                {
                    for (int i = 0; i < FileBackList.Length; i++)
                    {
                        FileInfo sourcefile = new FileInfo(FileBackList[i]);
                        string strhms = "-ERROR-" + System.DateTime.Now.ToString("yyyyMMddHHmmss");
                        // 4-zip备份文件
                        string fileName = sourcefile.Name.Replace(sourcefile.Extension, "") + strhms + sourcefile.Extension;
                        string backFile = TaskBackupPath + FileBackList[i].Replace(TaskSourcePath, @"\").Replace(sourcefile.Name, fileName);

                        FileInfo thefile = new FileInfo(backFile);
                        FileOperator.CheckDirectory(thefile.DirectoryName);

                        //把备份和删除合并更改为文件的移动
                        File.Copy(FileBackList[i], backFile, true);
                    }
                }
                //失败时做文件的全备份
            }

        }

        /// <summary>
        /// 任务失败时，将凌晨前的失败任务信息收集并存储，当关键任务时触发集体调用短信发送功能
        /// </summary>
        //add by zhouyu 2014-1-16
        private void SetSendMessage(DataTable dtTaskListA, DataTable dtTaskListB, string strSysType, string taskid, string strResult)
        {
            FileControl fw = new FileControl();//add by zhouyu 2014-1-20
            //add by zhouyu 2014-1-16;begin
            if (dtTaskListA.Rows[0]["TSM_SUB_VALUE"].ToString().Contains(taskid))
            {
                int flagA = UI.PROXY<int>.CallService("M07F0904_FileAccept", "InsertMessageErr", taskid, strResult);
                //短信报错列表
                DataTable dtMessageErr = UI.PROXY<DataTable>.CallService("M07F0904_FileAccept", "GetMessageErr");

                foreach (DataRow drMessageErr in dtMessageErr.Rows)
                {
                    //将标志置为1
                    int flag = UI.PROXY<int>.CallService("M07F0904_FileAccept", "UpdateMessageErr", drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_LOG_DATE"].ToString());
                    SendErrMsg objSendErrMsgA = new SendErrMsg();
                    //"店号", "系统编号", "任务号", "是否成功(0 失败 1 成功 2 正在执行中)", "消息编号"
                    string strTestA = objSendErrMsgA.ReSendMessage("990000", strSysType, drMessageErr["MER_TASK_ID"].ToString(), drMessageErr["MER_RESULT"].ToString(), drMessageErr["MER_RESULT"].ToString());
                    fw.WriteFileLog("发送报警信息：" + strSysType + "系统" + taskid + "任务 " + strResult + strTestA + "", TaskLogPath, LogFileName);//测试用LOG 20120921                     

                }
            }
            else if (dtTaskListB.Rows[0]["TSM_SUB_VALUE"].ToString().Contains(taskid))
            //将标志置为0，插入数据
            {
                int flag = UI.PROXY<int>.CallService("M07F0904_FileAccept", "InsertMessageErr", taskid, strResult);
            }
            else
            //add by zhouyu 2014-1-16;end
            {
                SendErrMsg objSendErrMsg = new SendErrMsg();
                //"店号", "系统编号", "任务号", "是否成功(0 失败 1 成功 2 正在执行中)", "消息编号"
                string strTest = objSendErrMsg.ReSendMessage("990000", strSysType, taskid, strResult, strResult);
                fw.WriteFileLog("发送报警信息：" + strSysType + "系统" + taskid + "任务 " + strResult + strTest + "", TaskLogPath, LogFileName);//测试用LOG 20140120
            }

        }

    }


}
