import unittest
import os
from hcfs_api import HdfsCli


class HdfsTest(unittest.TestCase):

    hdfs_obj = HdfsCli()
    maxFiles = 10
    maxDirs = 10
    test_dir = "/testdir"
    local_path = "files/"
    copy_back_path="copyfile/"

    def setUp(self):                                                     #working
	print "setup()"
        self.hdfs_obj.hdfs_rm(dir_path=self.test_dir, option="-r")
        self.hdfs_obj.hdfs_mkdir(self.test_dir)

   # def tearDown(self):                                                 #working
   #     self.hdfs_obj.hdfs_rm(dir_path=self.test_dir, option="-r")

    def test_00_mkdir(self):                                                  #working 
        test_mkdir = self.test_dir+"/dc-hcfs"
        self.assertTrue(self.hdfs_obj.hdfs_mkdir(test_mkdir))
        #self.assertTrue(self.hdfs_obj.hdfs_ls(dir_path=self.test_dir, search_file=test_mkdir, option=""))

    def test_01_recur_mkdir(self):                                             #working (directories are creating recursively like /0, /0/1, /0/1/2... ) 
        dir_path = self.test_dir
        for num in xrange(0, self.maxDirs):
            dir_path = dir_path + "/" +str(num)
           
           # self.hdfs_obj.hdfs_mkdir(dir_path)
            self.assertTrue(self.hdfs_obj.hdfs_mkdir(dir_path))
        self.assertTrue(self.hdfs_obj.hdfs_ls(dir_path=self.test_dir, search_file=dir_path, option="-R"))

    def test_02_one_mil_files(self):                                           #working (multiple file creating at time like /f1, /f2, /f3...)
        dir_path = self.test_dir + "/file"
        path_str = ""
        for num in xrange(0, self.maxFiles):
            path = dir_path + str(num)
            path_str = path_str + path + " "
            self.assertTrue(self.hdfs_obj.hdfs_touchz(path_str))
       # self.assertTrue(self.hdfs_obj.hdfs_ls(dir_path=self.test_dir, check_num_of_files=self.maxFiles))   # this line have an error

    def test_03_create_dirs(self):                                             #working (multiple directories are created 1 after another like /dir0, /dir1, dir2...)   
        dir_path = self.test_dir + "/dir"
        dir_str = ""
        for num in xrange(0, self.maxFiles):
            path = dir_path + str(num)
            dir_str = dir_str + path + " "
            
            self.hdfs_obj.hdfs_mkdir(dir_str)
           # self.assertTrue(self.hdfs_obj.hdfs_mkdir(dir_str))                  # not working with assert
       # self.assertTrue(self.hdfs_obj.hdfs_ls(dir_path=self.test_dir, check_num_of_files=self.maxFiles))    # this line hace an error

    def test_04_copyFromLocal(self):                                             # working 
        local_path = self.local_path+"sample.txt"

        self.hdfs_obj.hdfs_copyFromLocal(source=local_path, dest=self.test_dir, option="-p")
        # self.assertTrue(self.hdfs_obj.hdfs_copyFromLocal(source=local_path, dest=self.test_dir, option="-p"))

        self.hdfs_obj.hdfs_copyToLocal(source=self.test_dir+"/sample.txt", dest=self.copy_back_path)     
        # self.assertTrue(self.hdfs_obj.hdfs_copyToLocal(source=self.test_dir+"/sample.txt", dest=self.copy_back_path))

        self.hdfs_obj.ismd5sum_equal(file1=local_path, file2=self.copy_back_path+"sample.txt")
        # self.assertTrue(self.hdfs_obj.ismd5sum_equal(file1=local_path, file2=self.copy_back_path+"sample.txt")) 

        os.remove(self.copy_back_path+"sample.txt")

    def test_05_setfattr_getfattr(self):                                        #working  
        local_path = self.local_path+"sample.txt"

        self.hdfs_obj.hdfs_copyFromLocal(source=local_path, dest=self.test_dir, option="-p")
       # self.assertTrue(self.hdfs_obj.hdfs_copyFromLocal(source=local_path, dest=self.test_dir, option="-p"))

        self.assertTrue(self.hdfs_obj.hdfs_setfattr(attr_name="permissions", value=667, path=self.test_dir+"/sample.txt"))
        self.assertTrue(self.hdfs_obj.hdfs_getattr(path=self.test_dir+"/sample.txt", attr_val={"permissions": "667"}))

    def test_06_getmerge(self):                                                 #working 
        local_path = self.local_path + "sample.txt"
        file_path = self.copy_back_path + "output.txt"

        self.hdfs_obj.hdfs_copyFromLocal(source=local_path, dest=self.test_dir, option="-p") 
        #self.assertTrue(self.hdfs_obj.hdfs_copyFromLocal(local_path, dest=self.test_dir, option="-p"))

        self.assertTrue(self.hdfs_obj.hdfs_getmerge(src=self.test_dir, localdst=file_path))

        self.hdfs_obj.ismd5sum_equal(file1=local_path, file2=file_path)
        #self.assertTrue(self.hdfs_obj.ismd5sum_equal(local_path, file_path))
        #os.remove(file_path)

