#Datahearts 测试脚本示例程序

工程目录构造:
/bin                                        --脚本shell
/conf                                       --shell执行时的配置文件
/lib                                        --Dataheart客户端jar文件
/logs                                       --打包后执行shell时log的生成log文件的路径
/src/main/com.datahearts.dbasetest          --案例包
/src/main/com.datahearts.dbsuport           --数据库导入程序包





编译,打包,解压,执行:
前提:已安装maven,jdk1.8以上并且lib中的Datahearts客户端Jar文件导入本地maven库中:
mvn install:install-file -Dfile=/lib/dh-java-client-1.0.1.jar -DgroupId=com.dataherarts.client -DartifactId=dh-java-client -Dversion=1.0.1 -Dpackaging=jar
*注意[-Dfile]路径要正确

1.执行[mvn clean package]打包成功后,会在/target下生成[dh-samples-0.0.1-SNAPSHOT-bin.tar.gz]的tar文件。
2.使用[tar -xzvf dh-samples-0.0.1-SNAPSHOT-bin.tar.gz]解压后,生成[dh-samples-0.0.1-SNAPSHOT]。
3.进入[dh-samples-0.0.1-SNAPSHOT/bin]中,执行shell文件名+空格+start启动脚本。例如:[./doOracleToDBase.sh start]
4.执行结果log文件会生成到[dh-samples-0.0.1-SNAPSHOT/logs]中。
5.执行过程中出了查看log文件,可以用[ps -ef |grep 类名]查看进程情况,执行完成后进程会自动结束。


脚本说明:
1.从Oracle数据库取得数据导入DBase

功能:根据配置文件中设定的参数(oracle数据库服务器IP[dbserver.ip],端口[dbserver.port],SID[dbserver.sid],用户名[db.user],密码[db.password])
进行Oracle数据库进行连接。执行,配置文件中的SQL,取得结果转换成JSON数据,存入DBase。DBase的服务器IP和Bucket名需要在配置文件中设定。


程序:GetFromOracleTest
shell脚本:doOracleToDBase.sh
配置文件:oracle-setting.xml



