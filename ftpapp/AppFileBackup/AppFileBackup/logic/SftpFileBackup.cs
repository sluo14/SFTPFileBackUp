using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AppFileBackup.sftp;
using log4net;
using System.Reflection;

namespace AppFileBackup.logic
{
    public class SftpFileBackup
    {
        //创建日志记录组件实例
        ILog log = log4net.LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        ///****************************************************************************************************
        /// 函 数 名：doBackup
        /// 输入参数：无
        /// 返回值  ：无
        /// 功能描述：
        /// <summary>
        /// 进行文件备份，即从sftp指定目录上下载文件到本地并删除sftp上的相应文件
        /// </summary>
        /// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        /// 创 建 人：罗嗣扬                        创建日期：2017-08-24
        /// 修 改 人：罗嗣扬                        修改日期：2017-08-25
        ///****************************************************************************************************
        public void doBackup()
        {
            //访问配置文件并获取默认配置
            string sftp_ip = System.Configuration.ConfigurationManager.AppSettings["sftp_ip"];
            string sftp_port = System.Configuration.ConfigurationManager.AppSettings["sftp_port"];
            string sftp_username = System.Configuration.ConfigurationManager.AppSettings["sftp_username"];
            string sftp_password = System.Configuration.ConfigurationManager.AppSettings["sftp_password"];
            string sftp_path = System.Configuration.ConfigurationManager.AppSettings["sftp_path"];
            string local_path = System.Configuration.ConfigurationManager.AppSettings["local_path"];

            //创建一个sftp类对象
            SFTPHelper sftpObj = null;

            try
            {
                //给sftp类对象赋值
                sftpObj = new SFTPHelper(sftp_ip, sftp_port, sftp_username, sftp_password);

                //连接sftp服务器
                sftpObj.Connect();

                //获取指定sftp的文件列表
                Array fileList = sftpObj.GetFileList(sftp_path).ToArray();

                //创建一个代表不带路径的文件名的字符串
                string shortFileName = "";

                //遍历已经获取的sftp文件列表，逐个从sftp下载文件到本地并删除sftp上的相应文件
                int i = 0;
                for (i = 0; i < fileList.Length; i++)
                {
                    try
                    {
                        shortFileName = fileList.GetValue(i).ToString();
                        sftpObj.Get(sftp_path + shortFileName, local_path + shortFileName);
                        sftpObj.Delete(sftp_path + shortFileName);
                    }
                    catch (Exception ex)
                    {
                        continue;
                    }
                }

                log.Info("Ftp文件下载成功。下载文件数：" + i.ToString());
            }
            catch (Exception ex)
            {
                log.Error("Ftp文件下载出现异常。", ex);
            }
            finally
            {
                if (sftpObj != null && sftpObj.Connected)
                {
                    //断开sftp连接
                    sftpObj.Disconnect();
                }
            }
        }
    }
}
