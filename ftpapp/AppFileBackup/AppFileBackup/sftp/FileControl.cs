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
        /// �� �� ����CheckDirectory
        /// ����������ļ��е�ַ[string](lstrPath)
        /// ����ֵ  ����
        /// ����������
        /// <summary>
        /// ���·���Ƿ���ڣ�������ʱ�Զ�����
        /// </summary>
        /// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        /// �� �� �ˣ�                              �������ڣ�2007-12-05
        /// �� �� �ˣ�                              �޸����ڣ�
        ///****************************************************************************************************
        public  void CheckDirectory(string lstrPath)
        {
            if (System.IO.Directory.Exists(lstrPath) == false)
            {
                System.IO.Directory.CreateDirectory(lstrPath);
            }
        }


        ///****************************************************************************************************
        /// �� �� ����WriteFile
        /// ���������string LogContent          ����
        ///           string LogPath             ����·��
        ///           string LogName             �ļ���
        /// ����ֵ  ����
        /// ����������
        /// <summary>
        /// д�����ļ�
        /// </summary>
        /// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        /// �� �� �ˣ�                          �������ڣ�2007-12-06
        /// �� �� �ˣ�                              �޸����ڣ�
        ///****************************************************************************************************
        public  void WriteFile(string LogContent, string LogPath, string LogName)
        {
            //���·���Ƿ���ڣ�������ʱ�Զ�����
            this.CheckDirectory(LogPath);

            //ȷ��·���ԡ�\����β���Ա�ƴ��·��
            if (LogPath.Substring(LogPath.Length - 1) != @"\")
            {
                LogPath = LogPath + @"\";
            }

            //�����ļ����γ������ļ���ַ
            LogPath = LogPath + LogName;

            //����д�ļ�����
            StreamWriter swLog = new StreamWriter(LogPath, true);
            //д
            swLog.WriteLine(LogContent);
            //�ر���
            swLog.Close();
        }

        public void WriteFileLog(string LogContent, string LogPath, string LogName)
        {
            //���·���Ƿ���ڣ�������ʱ�Զ�����
            this.CheckDirectory(LogPath);

            //ȷ��·���ԡ�\����β���Ա�ƴ��·��
            if (LogPath.Substring(LogPath.Length - 1) != @"\")
            {
                LogPath = LogPath + @"\";
            }

            //�����ļ����γ������ļ���ַ
            LogPath = LogPath + LogName;

            //����д�ļ�����
            StreamWriter swLog = new StreamWriter(LogPath, true);
            //д
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
                    //�ر���
                    swLog.Close();
                }
                catch
                {
                }
            }
           
           
        }


        ///   <summary> 
        ///   ��ȡWindowsĿ¼ 
        ///   </summary> 
        ///   <returns> </returns> 
        public  string GetWinDirectory()
        {
            StringBuilder sBuilder = new StringBuilder(CHAR_COUNT);
            GetWindowsDirectory(sBuilder, CHAR_COUNT);
            return sBuilder.ToString();
        }

        /// <summary>
        /// ��ȡ��ǰ�ļ����µ��ļ�
        /// </summary>
        /// <param name="SourcePath">�ļ�·��</param>
        /// <param name="blnfullName">�Ƿ���ʾ�ļ�ȫ·��</param>
        /// <returns></returns>
        public  string[] GetCurFileList(string SourcePath, string searchPattern, bool blnfullName)
        {
            try
            {
                DirectoryInfo dirInfo = new DirectoryInfo(SourcePath);
                ListFiles = new StringBuilder();
                foreach (FileSystemInfo fsi in dirInfo.GetFileSystemInfos())
                {
                    if (fsi is FileInfo)   //�ļ�
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
        /// ��ȡ��ǰ�ļ��������Ե��ļ������������ϼ�)
        /// </summary>
        /// <param name="SourcePath">�ļ�·��</param>
        /// <param name="blnfullName">�Ƿ���ʾ�ļ�ȫ·��</param>
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
                    if (fsi is FileInfo)   //�ļ�
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
                    else   //Ŀ¼
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
        /// ɾ���ļ��м��ļ�
        /// </summary>
        /// <param name="aimPath">Ŀ¼ </param>
        /// <param name="blndelFolder">�Ƿ�ɾ�����ϼ�</param>
        public  void DeleteDir(string filePath, bool blndelFolder)
        {
            // ���Ŀ��Ŀ¼�Ƿ���Ŀ¼�ָ��ַ�����������������֮
            if (filePath[filePath.Length - 1] != Path.DirectorySeparatorChar)
            {
                filePath += Path.DirectorySeparatorChar;
            }
            string[] fileList = null;
            fileList = Directory.GetFileSystemEntries(filePath);

            // �������е��ļ���Ŀ¼
            foreach (string file in fileList)
            {
                // �ȵ���Ŀ¼��������������Ŀ¼�͵ݹ�Delete��Ŀ¼������ļ�
                if (Directory.Exists(file))
                {
                    DeleteDir(filePath + Path.GetFileName(file), blndelFolder);
                }
                // ����ֱ��Delete�ļ�
                else
                {
                    File.Delete(filePath + Path.GetFileName(file));
                }
            }
            //ɾ���ļ���
            if (blndelFolder == true)
            {
                System.IO.Directory.Delete(filePath, true);
            }
        }

        ///****************************************************************************************************
        /// �� �� ����GetAppPath
        /// ���������
        /// ����ֵ  ��string:	                    �������������ַ
        /// ����������
        /// <summary>
        /// ȡ�ó������������ַ
        /// </summary>
        /// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        /// �� �� �ˣ�000100                        �������ڣ�2008-01-24
        /// �� �� �ˣ�                              �޸����ڣ�
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
        /// ��������
        /// </summary>
        /// <param name="ServiceName">��������</param>
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

        //����Ƿ�·���Ƿ���'\'��β
        public  string chkPathBacklash(string Path)
        {
            //ȷ��·���ԡ�\����β���Ա�ƴ��·��
            if (Path.Substring(Path.Length - 1) != @"\")
            {
                return Path = Path + @"\";
            }
            else
                return Path;
        }

        /// <summary>
        /// ����Ƿ�·���Ƿ���'/'��β
        /// </summary>
        /// <param name="Path">����֤��·��</param>
        /// <returns>β��'/'��·��</returns>
        public  string chkPathEndSolidus(string Path)
        {
            //ȷ��·���ԡ�\����β���Ա�ƴ��·��
            if (Path.Substring(Path.Length - 1) != "/")
            {
                return Path = Path + "/";
            }
            else
                return Path;
        }

        /// <summary>
        /// ����Ƿ�·���Ƿ���'/'��ͷ
        /// </summary>
        /// <param name="Path">����֤��·��</param>
        /// <returns>ͷ'/'��·��</returns>
        public  string chkPathStartSolidus(string Path)
        {
            //ȷ��·���ԡ�\����β���Ա�ƴ��·��
            if (Path.Substring(0, 1) == "/")
            {
                return Path;
            }
            else
                return Path = "/" + Path;
        }



        //���chk�ļ��Ƿ�����
        /// <summary>
        /// ���chk�ļ��Ƿ�����
        /// </summary>
        /// <param name="fi">��ѯ���ļ��б�</param>
        /// <param name="filename">�ļ���</param>
        /// <returns>�������</returns>
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
        //�����zip�ļ��Ƿ�����
        /// <summary>
        /// �����zip�ļ��Ƿ�����
        /// </summary>
        /// <param name="afi">�����ļ��б�</param>
        /// <param name="dfi">�Ѿ����ڵ��ļ��б�</param>
        /// <param name="filename">�ļ���</param>
        /// <returns>�������</returns>
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

        //�ж��Ƿ�Ϊ����
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

        /// �ж�Ŀ¼�����ַ��Ƿ�Ϸ�
        /// </summary>
        /// <param name="DirectoryName">Ŀ¼����</param>
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
        /// �ж��ļ������ַ��Ƿ�Ϸ�
        /// </summary>
        /// <param name="FileName">�ļ�����</param>
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
            //�жϱ��ʽ�е������ַ���
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

        #region �ʼ�����
        public bool SendMail(string mailServerIP, string addressFrom, string addressTo, string mailTitle, string body, string userid, string pwd, string AttachmentFile)
        {
            bool blnflag = false;
            try
            {
                SmtpClient client = new SmtpClient(mailServerIP);   //�����ʼ�Э��

                client.UseDefaultCredentials = false;//�û���֤

                client.DeliveryMethod = SmtpDeliveryMethod.Network; //ͨ�����緢�͵�Smtp������

                client.Credentials = new NetworkCredential(userid, pwd); //ͨ���û��������� ��֤


                MailMessage msg = new MailMessage(new MailAddress(addressFrom), new MailAddress(addressTo)); //�����˺��ռ��˵������ַ

                msg.Subject = mailTitle;      //�ʼ�����

                msg.SubjectEncoding = Encoding.Default;   //�������

                msg.Body = body;         //�ʼ�����

                msg.BodyEncoding = Encoding.Default;      //���ı���

                msg.IsBodyHtml = false;    //����ΪHTML��ʽ      

                msg.Priority = MailPriority.High;   //���ȼ�

                Attachment data = null;
                if (AttachmentFile != "")
                {

                    // msg.Attachments.Add(new Attachment(AttachmentFile));//���Ӹ���

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
                return inUse;//by  true��ʾ����ʹ��,falseû��ʹ��
            }
            else
            {
                return false;//�ļ�������,�϶�û�б�ʹ��
            }

        }

    }
}
