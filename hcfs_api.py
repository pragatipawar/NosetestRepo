import subprocess
import hashlib


class HdfsCli(object):
    def __init__(self):
        pass

    def __execute(self, cmd):
        """

        :param cmd:
        :return:
        """
        cmd_list = cmd.split()
        proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_out, proc_err = proc.communicate()
        if proc.returncode != 0:
            return False
        return proc_out

    def hdfs_ls(self, dir_path="/", search_file="", option="", check_num_of_files=0):
        """
        :param dir_path:
        :param search_file:
        :param recursive:
        :return:
        """
        if option is not "":
            cmd = "hadoop fs -ls " + option + " " + dir_path
        else:
            cmd = "hadoop fs -ls " + dir_path
        output = self.__execute(cmd)
        print output
        if check_num_of_files > 0:
            if "Found " + str(check_num_of_files) + " items" in output:
                return True
            return False
        if search_file == "":
            return True
        if search_file in output:
            return True
        return False

    def hdfs_mkdir(self, mkdir_path):
        """
        :param mkdir_path:
        :return:
        """
        cmd = "hadoop fs -mkdir " + mkdir_path
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_moveFromLocal(self, local_src, dest):
        cmd = "hadoop fs -moveFromLocal " + local_src + " " + dest
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_put(self, src, dest, option=""):
        """

        :param src:
        :param dest:
        :param option:
        :return:
        """
        cmd = "hadoop fs -put " + option + " " + src + " " + dest
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_appendToFile(self, sources=[], dest=""):
        """
        :param source:
        :param dest:
        :return:
        """
        source_str = " ".join(sources)
        cmd = "hadoop fs -appendToFile " + source_str + " " + dest
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_rmdir(self, dir_path="", option="--ignore-fail-on-non-empty", enable=True):
        """

        :param dir_path:
        :return:
        """
        if enable:
            cmd = "hadoop fs -rmdir " + option + " " + dir_path
        else:
            cmd = "hadoop fs -rmdir " + dir_path
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_rm(self, dir_path="", option=""):
        """

        :param dir_path:
        :param option:
        :return:
        """
        cmd = "hadoop fs -rm " + option + " " + dir_path
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_chmod(self, permissions="", filepath="", recursive=False):
        """

        :param permissions:
        :return:
        """
        if recursive:
            cmd = "hadoop fs -chmod -R " + permissions + " " + filepath
        else:
            cmd = "hadoop fs -chmod " + permissions + " " + filepath
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_chgrp(self, recursive=False, group_name="", filepath=""):
        """

        :param recursive:
        :param group_name:
        :param filepath:
        :return:
        """
        if recursive:
            cmd = "hadoop fs -chgrp -R " + group_name + " " + filepath
        else:
            cmd = "hadoop fs -chgrp " + group_name + " " + filepath
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_chown(self, owner="", group="", recursive=False, path=""):
        """

        :param owner:
        :param path:
        :return:
        """
        if recursive:
            cmd = "hadoop fs -chown -R " + owner + ":[%s] " % group + path
        else:
            cmd = "hadoop fs -chown " + owner + ":[%s] " % group + path
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_copyFromLocal(self, source, dest, option=""):
        """

        :param source:
        :param dest:
        :param option:
        :return:
        """
        # cmd = "hadoop fs -copyFromLocal %s %s %s" % (option, source, dest)
	cmd = "hadoop fs -copyFromLocal " + option + " " + source + " " + dest
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_copyToLocal(self, source, dest, option=""):
        """

        :param source:
        :param dest:
        :param option: 
        :return:
        """
        # cmd = "hadoop fs -copyToLocal %s %s" % (source, dest)
	cmd = "hadoop fs -copyToLocal " + option + " " + source + " " + dest
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_get(self, source, dest, option=""):
        """

        :param source:
        :param dest:
        :param option:
        :return:
        """
        cmd = "hadoop fs -get -%s %s %s" % (option, source, dest)
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_touchz(self, path):
        """

        :param path:
        :return:
        """
        cmd = "hadoop fs -touchz %s" % (path)
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_mv(self, source, dest):
        """

        :param source:
        :param dest:
        :return:
        """
        cmd = "hadoop fs -mv %s %s" % (source, dest)
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_df(self, path):
        """

        :param path:
        :return:
        """
        cmd = "hadoop fs -df -h %s" % path
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_du(self, path):
        """

        :param path:
        :return:
        """
        cmd = "hadoop fs -du -h %s" % path
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_getmerge(self, src, localdst):
        """
        :param src:
        :param localdst:
        :return:
        """
        #cmd = "hadoop fs -getmerge %s %s"%(src, localdst)
        cmd = "hadoop fs -getmerge -nl " + src + " " + localdst	  
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_setfattr(self, attr_name="", value="", path=""):
        """
        :param attr_name:
        :param value:
        :param path:
        :return:
        """
        cmd = "hadoop fs -setfattr -n %s -v %s %s" % (attr_name, value, path)
        out = self.__execute(cmd)
        if out is False:
            return False
        return True

    def hdfs_getattr(self, path="", attr_val={}):
        """
        :param attr_name:
        :param path:
        :return:
        """
        attribute_dict={}
        cmd = "hadoop fs -getfattr -d %s" % path
        out = self.__execute(cmd)
        for line in out.splitlines():
            if "=" in line:
                items = line.split("=")
                attribute_dict[items[0].strip()]=items[1].strip('"')
        print attribute_dict
        if attribute_dict == attr_val:
            return True
        return False

    def md5sum(self, filename):
        """

        :param filename:
        :return:
        """
        md5sum = hashlib.md5(open(filename,'rb').read()).hexdigest()
        return md5sum

    def ismd5sum_equal(self, file1, file2):
        """

        :param file1:
        :param file2:
        :return:
        """
        sum1 = self.md5sum(file1)
        sum2 = self.md5sum(file2)
        print sum1
        print sum2
        if sum1.strip() == sum2.strip():
            return True
        return False
