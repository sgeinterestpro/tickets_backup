package main

/*
const char *build_time(void) {
	static const char *psz_build_time = __DATE__ " " __TIME__ ;
	return psz_build_time;
}
*/
import "C"

import (
	"bytes"
	"flag"
	"fmt"
	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

const (
	PublicKey  = "@PublicKey@"
	PrivateKey = "@PrivateKey@"
	BucketName = "@BucketName@"
	FileHost   = "@FileHost@"
)

type uFile struct {
	uFileRequest *ufsdk.UFileRequest
	config       *ufsdk.Config
}

func newUFile() *uFile {
	config := new(ufsdk.Config)
	config.PublicKey = PublicKey
	config.PrivateKey = PrivateKey
	config.BucketName = BucketName
	config.FileHost = FileHost
	req, _ := ufsdk.NewFileRequest(config, nil)
	return &uFile{req, config}
}

func (u *uFile) getFiles(prefix string) []ufsdk.FileDataSet {
	//log.Println("正在获取文件列表...")
	list, err := u.uFileRequest.PrefixFileList(prefix, "", 0)
	if err != nil {
		log.Println("获取文件列表失败，错误信息为：", err.Error())
		return nil
	}
	//log.Printf("获取文件列表返回的信息是：\n%s\n", list)
	return list.DataSet
}

func (u *uFile) uploadFile(dbName, path string, tag string) (err error) {
	timeNow := time.Now().Format("20060102_150405")
	name := filepath.Base(path)
	ext := filepath.Ext(name)
	var key string
	if tag != "" {
		key = fmt.Sprintf("%s_tag/%s_%s%s", dbName, tag, timeNow, ext)
	} else {
		key = fmt.Sprintf("%s/db_%s%s", dbName, timeNow, ext)
	}
	log.Printf("正在上传数据库归档文件`%s`到`%s`...\n", path, key)
	err = u.uFileRequest.PutFile(path, key, "")
	if err != nil {
		log.Println("数据库归档上传失败!!，错误信息为：", err.Error())
		//把 HTTP 详细的 HTTP response dump 出来
		log.Println(u.uFileRequest.DumpResponse(true))
		return
	}
	log.Println("数据库归档上传成功!!")

	// 删除一年前的备份
	timeAgo := int(time.Now().AddDate(-2, 0, 0).Unix())
	fileList := u.getFiles(dbName + "/")
	for _, file := range fileList {
		if file.ModifyTime < timeAgo {
			log.Printf("删除一年前的数据库归档文件`%s`!!\n", file.FileName)
			_ = u.uFileRequest.DeleteFile(file.FileName)
		}
	}
	return
}

func archiveDb(dbName, tmpPath string) (archPath string, err error) {
	archPath = filepath.Join(tmpPath, dbName+"_db.dump")
	log.Printf("正在将数据库`%s`归档到文件`%s`...\n", dbName, archPath)
	command := exec.Command("mongodump", "-d", dbName, "--archive="+archPath)
	stdErrBuffer := bytes.NewBuffer(nil)
	command.Stderr = stdErrBuffer
	if err = command.Run(); err != nil {
		log.Println("数据库归档失败!!，错误信息为：", err.Error())
		log.Println(stdErrBuffer.String())
		return
	}
	log.Println("数据库归档成功!!")
	return
}

func getTempPath() (tmpPath string) {
	tmpPath, err := ioutil.TempDir("", "backup_tmp")
	if err != nil {
		log.Panicln(err.Error())
	}
	return
}

var uf = newUFile()
var tagPtr = flag.String("tag", "", "upload with tag")

func BuildTime() string {
	return C.GoString(C.build_time())
}

//goland:noinspection GoUnhandledErrorResult
func main() {
	flag.CommandLine.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Version: %s\n\n", BuildTime())
		fmt.Fprintf(flag.CommandLine.Output(), "BucketName: %s\n\n", uf.config.BucketName)
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options] db1 [db2 [db3 [...]]]\n\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "Options:\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.CommandLine.Usage()
		os.Exit(0)
	}
	// 申请一个临时文件夹
	tmpPath := getTempPath()
	defer os.RemoveAll(tmpPath)
	// 备份参数指定的所有数据库
	for idx, args := range flag.Args() {
		fmt.Println("开始备份第`"+strconv.Itoa(idx+1)+"`个数据库:", args)
		// 打包数据库到临时文件夹
		archPath, err := archiveDb(args, tmpPath)
		if err == nil {
			// 上传数据库归档文件到云端
			_ = uf.uploadFile(args, archPath, *tagPtr)
		}
	}

}
