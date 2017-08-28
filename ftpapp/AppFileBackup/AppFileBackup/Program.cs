using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Globalization;
using AppFileBackup.sftp;
using log4net;
using System.Reflection;
using AppFileBackup.logic;

[assembly: log4net.Config.XmlConfigurator(Watch = true)]
namespace AppFileBackup
{
    class Program
    {
        static void Main(string[] args)
        {
            //创建一个sftp文件备份逻辑的对象
            SftpFileBackup logic = new SftpFileBackup();

            //进行文件备份，即从sftp指定目录上下载文件到本地并删除sftp上的相应文件
            logic.doBackup();
        }
    }
}
