using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text.RegularExpressions;
using System.Collections;
using System.Net;
using System.Net.Mail;
using System.Data;
using System.Net.Mime;

namespace AppFileBackup.sftp
{
    public class FileControl
    {
        private const int CHAR_COUNT = 128;
        private  StringBuilder ListFiles = new StringBuilder();
        private  ServiceController[] services;

        [DllImport("kernel32 ")]
        private static extern void GetWindowsDirectory(StringBuilder WinDir, int count);

        ///****************************************************************************************************
        /// 函 数 名：CheckDirectory
        /// 输入参数：文件夹地址[string](lstrPath)
        /// 返回值  ：无
        /// 功能描述：
        /// <summary>
        /// 检测路径是否存在，不存在时自动创建
        /// </summary>
        /// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        /// 创 建 人：                              创建日期：2007-12-05
        /// 修 改 人：                              修改日期：
        ///****************************************************************************************************
        public  void CheckDirectory(string lstrPath)
        {
            if (System.IO.Directory.Exists(lstrPath) == false)
            {
                System.IO.Directory.CreateDirectory(lstrPath);
            }
        }


        ///****************************************************************************************************
        /// 函 数 名：WriteFile
        /// 输入参数：string LogContent          内容
        ///           string LogPath             保存路径
        ///           string LogName             文件名
        /// 返回值  ：无
        /// 功能描述：
        /// <summary>
        /// 写磁盘文件
        /// </summary>
        /// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        /// 创 建 人：                          创建日期：2007-12-06
        /// 修 改 人：                              修改日期：
        ///****************************************************************************************************
        public  void WriteFile(string LogContent, string LogPath, string LogName)
        {
            //检测路径是否存在，不存在时自动创建
            this.CheckDirectory(LogPath);

            //确保路径以‘\’结尾，以便拼合路径
            if (LogPath.Substring(LogPath.Length - 1) != @"\")
            {
                LogPath = LogPath + @"\";
            }

            //加上文件名形成最终文件地址
            LogPath = LogPath + LogName;

            //定义写文件的流
            StreamWriter swLog = new StreamWriter(LogPath, true);
            //写
            swLog.WriteLine(LogContent);
            //关闭流
            swLog.Close();
        }

        public void WriteFileLog(string LogContent, string LogPath, string LogName)
        {
            //检测路径是否存在，不存在时自动创建
            this.CheckDirectory(LogPath);

            //确保路径以‘\’结尾，以便拼合路径
            if (LogPath.Substring(LogPath.Length - 1) != @"\")
            {
                LogPath = LogPath + @"\";
            }

            //加上文件名形成最终文件地址
            LogPath = LogPath + LogName;

            //定义写文件的流
            StreamWriter swLog = new StreamWriter(LogPath, true);
            //写
            try
            {
                swLog.WriteLine(LogContent);
            }
            catch
            {
            }
            finally
            {
                try
                {
                    //关闭流
                    swLog.Close();
                }
                catch
                {
                }
            }
           
           
        }


        ///   <summary> 
        ///   获取Windows目录 
        ///   </summary> 
        ///   <returns> </returns> 
        public  string GetWinDirectory()
        {
            StringBuilder sBuilder = new StringBuilder(CHAR_COUNT);
            GetWindowsDirectory(sBuilder, CHAR_COUNT);
            return sBuilder.ToString();
        }

        /// <summary>
        /// 获取当前文件夹下的文件
        /// </summary>
        /// <param name="SourcePath">文件路径</param>
        /// <param name="blnfullName">是否显示文件全路径</param>
        /// <returns></returns>
        public  string[] GetCurFileList(string SourcePath, string searchPattern, bool blnfullName)
        {
            try
            {
                DirectoryInfo dirInfo = new DirectoryInfo(SourcePath);
                ListFiles = new StringBuilder();
                foreach (FileSystemInfo fsi in dirInfo.GetFileSystemInfos())
                {
                    if (fsi is FileInfo)   //文件
                    {
                        if (blnfullName == true)
                        {
                            if (isMatch(fsi.Name, searchPattern))
                            {
                                ListFiles.Append(fsi.FullName);
                                ListFiles.Append("\n");
                            }
                        }
                        else
                        {
                            if (isMatch(fsi.Name, searchPattern))
                            {
                                ListFiles.Append(fsi.Name);
                                ListFiles.Append("\n");
                            }
                        }
                    }
                }
                ListFiles.Remove(ListFiles.ToString().LastIndexOf('\n'), 1);
                return  ListFiles.ToString().Split('\n');
            }
            catch 
            {
                return null;
            }
        }

        /// <summary>
        /// 获取当前文件夹下所以的文件（包括子资料夹)
        /// </summary>
        /// <param name="SourcePath">文件路径</param>
        /// <param name="blnfullName">是否显示文件全路径</param>
        /// <returns></returns>
        public  string[] GetFileList(string SourcePath, string searchPattern, bool blnfullName)
        {
            string[] filelist = null;
            try
            {
                ListFiles = new StringBuilder();
                DetailFileList(SourcePath, searchPattern, blnfullName);
                ListFiles.Remove(ListFiles.ToString().LastIndexOf('\n'), 1);
                filelist=ListFiles.ToString().Split('\n');

                return filelist;

            }
            catch
            {
                filelist = null;
                return filelist;
            }
        }

        private  void DetailFileList(string SourcePath, string searchPattern, bool blnfullName)
        {
            try
            {
                DirectoryInfo dirInfo = new DirectoryInfo(SourcePath);
                foreach (FileSystemInfo fsi in dirInfo.GetFileSystemInfos())
                {
                    if (fsi is FileInfo)   //文件
                    {
                        if (blnfullName == true)
                        {
                            if (isMatch(fsi.Name, searchPattern))
                            {
                                ListFiles.Append(fsi.FullName);
                                ListFiles.Append("\n");
                            }
                        }
                        else
                        {
                            if (isMatch(fsi.Name, searchPattern))
                            {
                                ListFiles.Append(fsi.Name);
                                ListFiles.Append("\n");
                            }
                        }
                       
                    }
                    else   //目录
                    {
                        DetailFileList(SourcePath + "\\" + fsi.Name, searchPattern, blnfullName);
                    }
                }
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry("GetFileList(LocalHost)", ex.Message + "GetFileList");
            }
        }

        /// <summary>
        /// 删除文件夹及文件
        /// </summary>
        /// <param name="aimPath">目录 </param>
        /// <param name="blndelFolder">是否删除资料夹</param>
        public  void DeleteDir(string filePath, bool blndelFolder)
        {
            // 检查目标目录是否以目录分割字符结束如果不是则添加之
            if (filePath[filePath.Length - 1] != Path.DirectorySeparatorChar)
            {
                filePath += Path.DirectorySeparatorChar;
            }
            string[] fileList = null;
            fileList = Directory.GetFileSystemEntries(filePath);

            // 遍历所有的文件和目录
            foreach (string file in fileList)
            {
                // 先当作目录处理如果存在这个目录就递归Delete该目录下面的文件
                if (Directory.Exists(file))
                {
                    DeleteDir(filePath + Path.GetFileName(file), blndelFolder);
                }
                // 否则直接Delete文件
                else
                {
                    File.Delete(filePath + Path.GetFileName(file));
                }
            }
            //删除文件夹
            if (blndelFolder == true)
            {
                System.IO.Directory.Delete(filePath, true);
            }
        }

        ///****************************************************************************************************
        /// 函 数 名：GetAppPath
        /// 输入参数：
        /// 返回值  ：string:	                    程序启动物理地址
        /// 功能描述：
        /// <summary>
        /// 取得程序启动物理地址
        /// </summary>
        /// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        /// 创 建 人：000100                        创建日期：2008-01-24
        /// 修 改 人：                              修改日期：
        ///****************************************************************************************************
        public  string GetAppPath()
        {
            string strCurPath = System.Windows.Forms.Application.StartupPath;
            if (strCurPath.Substring(strCurPath.Length - 1, 1) != @"\")
            {
                strCurPath += @"\";
            }
            return strCurPath;
        }
        /// <summary>
        /// 重启服务
        /// </summary>
        /// <param name="ServiceName">服务名称</param>
        public  void ReStartService(string ServiceName)
        {
            bool flag = false;
            string status = null;
            services = ServiceController.GetServices();
            for (int i = 0; i < services.Length; i++)
            {
                if (ServiceName == services[i].ServiceName.ToString())
                {
                    flag = true;
                    status=services[i].Status.ToString();
                    break;
                }
            }
            if (flag == true)
            {
                ServiceController controller = new ServiceController(ServiceName);
                if (status == "Running" || status == "Paused")
                {
                    controller.Stop();
                    controller.WaitForStatus(ServiceControllerStatus.Stopped);
                }
                    controller.Start();
                    controller.WaitForStatus(ServiceControllerStatus.Running );
            }
        }

        //检查是否路径是否以'\'结尾
        public  string chkPathBacklash(string Path)
        {
            //确保路径以‘\’结尾，以便拼合路径
            if (Path.Substring(Path.Length - 1) != @"\")
            {
                return Path = Path + @"\";
            }
            else
                return Path;
        }

        /// <summary>
        /// 检查是否路径是否以'/'结尾
        /// </summary>
        /// <param name="Path">待验证的路径</param>
        /// <returns>尾带'/'的路径</returns>
        public  string chkPathEndSolidus(string Path)
        {
            //确保路径以‘\’结尾，以便拼合路径
            if (Path.Substring(Path.Length - 1) != "/")
            {
                return Path = Path + "/";
            }
            else
                return Path;
        }

        /// <summary>
        /// 检查是否路径是否以'/'开头
        /// </summary>
        /// <param name="Path">待验证的路径</param>
        /// <returns>头'/'的路径</returns>
        public  string chkPathStartSolidus(string Path)
        {
            //确保路径以‘\’结尾，以便拼合路径
            if (Path.Substring(0, 1) == "/")
            {
                return Path;
            }
            else
                return Path = "/" + Path;
        }



        //检查chk文件是否生成
        /// <summary>
        /// 检查chk文件是否生成
        /// </summary>
        /// <param name="fi">查询的文件列表</param>
        /// <param name="filename">文件名</param>
        /// <returns>存在与否</returns>
        public  bool ExitFileChk(string[] fi, string filename)
        {
            bool blnFlag = false;
            try
            {
                for (int i = 0; i < fi.Length; i++)
                {           
                    if (fi[i].ToLower() == filename.ToLower())
                    {
                        blnFlag = true;
                        break;
                    }
                }
                return blnFlag;
            }
            catch
            {
                return false;
            }
        }
        //检查与zip文件是否生成
        /// <summary>
        /// 检查与zip文件是否生成
        /// </summary>
        /// <param name="afi">所有文件列表</param>
        /// <param name="dfi">已经存在的文件列表</param>
        /// <param name="filename">文件名</param>
        /// <returns>存在与否</returns>
        public  bool ExitFileZip(string[] afi, string[] dfi, string filename)
        {
            bool blnFlag = false;
            try
            {
                for (int i = 0; i < dfi.Length; i++)
                {
                    FileInfo f = new FileInfo(dfi[i]);

                    if (f.Name.ToLower() == filename + ".zip")
                    {
                        blnFlag = true;
                        break;
                    }
                }
                if (blnFlag == false)
                {
                    for (int i = 0; i < afi.Length; i++)
                    {
                        FileInfo f = new FileInfo(afi[i]);

                        if (f.Name.ToLower() == filename + ".zip")
                        {
                            blnFlag = false;
                            break;
                        }
                    }
                }
                return blnFlag;
            }
            catch
            {
                return false;
            }
        }

        //判断是否为数字
        public  bool IsNumber(String strNumber)
        {
            Regex objNotNumberPattern = new Regex("[^0-9.-]");
            Regex objTwoDotPattern = new Regex("[0-9]*[.][0-9]*[.][0-9]*");
            Regex objTwoMinusPattern = new Regex("[0-9]*[-][0-9]*[-][0-9]*");
            String strValidRealPattern = "^([-]|[.]|[-.]|[0-9])[0-9]*[.]*[0-9]+$";
            String strValidIntegerPattern = "^([-]|[0-9])[0-9]*$";
            Regex objNumberPattern = new Regex("(" + strValidRealPattern + ")|(" + strValidIntegerPattern + ")");

            return !objNotNumberPattern.IsMatch(strNumber) &&
            !objTwoDotPattern.IsMatch(strNumber) &&
            !objTwoMinusPattern.IsMatch(strNumber) &&
            objNumberPattern.IsMatch(strNumber);
        }
        
        public  bool IsNumeric(string value)
        {
            return Regex.IsMatch(value, @"^[+-]?\d*[.]?\d*$");
        }
        public  bool IsInt(string value)
        {
            return Regex.IsMatch(value, @"^[+-]?\d*$");
        }
        public  bool IsUnsign(string value)
        {
            return Regex.IsMatch(value, @"^\d*[.]?\d*$");
        }

        /// 判断目录名中字符是否合法
        /// </summary>
        /// <param name="DirectoryName">目录名称</param>
        public  bool IsValidPathChars(string DirectoryName)
        {
            char[] invalidPathChars = Path.GetInvalidPathChars();
            char[] DirChar = DirectoryName.ToCharArray();
            foreach (char C in DirChar)
            {
                if (Array.BinarySearch(invalidPathChars, C) >= 0)
                {
                    return false;
                }
            }
            return true;
        }


        /**/
        /// <summary>
        /// 判断文件名中字符是否合法
        /// </summary>
        /// <param name="FileName">文件名称</param>
        public  bool IsValidFileChars(string FileName)
        {
            char[] invalidFileChars = Path.GetInvalidFileNameChars();
            char[] NameChar = FileName.ToCharArray();
            foreach (char C in NameChar)
            {
                if (Array.BinarySearch(invalidFileChars, C) >= 0)
                {
                    return false;
                }
            }
            return true;
        }

        public  bool isMatch(string fileName, string Pattern)
        {
            bool blnMatch = false;
            //判断表达式中的特殊字符号
            if (Pattern.IndexOf("(") != -1 && Pattern.IndexOf(@"\(") == -1)
            {
                Pattern = Pattern.Replace("(", @"\(");
            }
            if (Pattern.IndexOf(")") != -1 && Pattern.IndexOf(@"\)") == -1)
            {
                Pattern = Pattern.Replace(")", @"\)");
            }
            if (Pattern.StartsWith("*"))
            {
                Pattern = Pattern.Substring(2, Pattern.Length);
            }
            if (Regex.IsMatch(fileName, @"" + Pattern))
            {
                blnMatch = true;
            }
            else
            {
                blnMatch = false;
            }
            return blnMatch;
        }

        #region 邮件发送
        public bool SendMail(string mailServerIP, string addressFrom, string addressTo, string mailTitle, string body, string userid, string pwd, string AttachmentFile)
        {
            bool blnflag = false;
            try
            {
                SmtpClient client = new SmtpClient(mailServerIP);   //设置邮件协议

                client.UseDefaultCredentials = false;//用户验证

                client.DeliveryMethod = SmtpDeliveryMethod.Network; //通过网络发送到Smtp服务器

                client.Credentials = new NetworkCredential(userid, pwd); //通过用户名和密码 认证


                MailMessage msg = new MailMessage(new MailAddress(addressFrom), new MailAddress(addressTo)); //发件人和收件人的邮箱地址

                msg.Subject = mailTitle;      //邮件主题

                msg.SubjectEncoding = Encoding.Default;   //主题编码

                msg.Body = body;         //邮件正文

                msg.BodyEncoding = Encoding.Default;      //正文编码

                msg.IsBodyHtml = false;    //设置为HTML格式      

                msg.Priority = MailPriority.High;   //优先级

                Attachment data = null;
                if (AttachmentFile != "")
                {

                    // msg.Attachments.Add(new Attachment(AttachmentFile));//增加附件

                    data = new Attachment(AttachmentFile, MediaTypeNames.Application.Octet);
                    msg.Attachments.Add(data);

                }
                try
                {
                    client.Send(msg);
                    blnflag = true;
                }
                catch (Exception ex)
                {
                    blnflag = false;
                }
                finally
                {

                }

                return blnflag;
            }
            catch (Exception ex)
            {
                blnflag = false;
                return blnflag;
            }
            finally
            {

            }
        }

        #endregion

        public bool IsFileInUse(string fileName)
        {
            bool inUse = true;

            if (File.Exists(fileName))
            {
                FileStream fs = null;
                try
                {
                    fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.None);                  
                   inUse = false;
                }
                catch
                {
                   // exception....

                }

                finally
                {
                    if (fs != null)
                        fs.Close();
                }
                return inUse;//by  true表示正在使用,false没有使用
            }
            else
            {
                return false;//文件不存在,肯定没有被使用
            }

        }

    }
}
