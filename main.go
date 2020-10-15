package main

import (
	"bytes"
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

type ufs struct {
	ufr    *ufsdk.UFileRequest
	config *ufsdk.Config
}

func newUfs() *ufs {
	config := new(ufsdk.Config)
	config.PublicKey = PublicKey
	config.PrivateKey = PrivateKey
	config.BucketName = BucketName
	config.FileHost = FileHost
	req, _ := ufsdk.NewFileRequest(config, nil)
	return &ufs{req, config}
}

func (u *ufs) getFiles(prefix string) []ufsdk.FileDataSet {
	//log.Println("正在获取文件列表...")
	list, err := u.ufr.PrefixFileList(prefix, "", 0)
	if err != nil {
		log.Println("获取文件列表失败，错误信息为：", err.Error())
		return nil
	}
	//log.Printf("获取文件列表返回的信息是：\n%s\n", list)
	return list.DataSet
}

func (u *ufs) uploadFile(dbName, path string) (err error) {
	timePath := time.Now().Format("20060102_150405")
	name := filepath.Base(path)
	ext := filepath.Ext(name)
	key := fmt.Sprintf("%s/%s_%s%s", dbName, name[:len(name)-len(ext)], timePath, ext)
	log.Printf("正在上传数据库归档文件`%s`到`%s`...\n", path, key)
	err = u.ufr.PutFile(path, key, "")
	if err != nil {
		log.Println("数据库归档上传失败!!，错误信息为：", err.Error())
		//把 HTTP 详细的 HTTP response dump 出来
		log.Println(u.ufr.DumpResponse(true))
		return
	}
	log.Println("数据库归档上传成功!!")

	// 删除一年前的备份
	timeAgo := int(time.Now().AddDate(-1, 0, 0).Unix())
	fileList := u.getFiles(dbName + "/")
	for _, file := range fileList {
		if file.ModifyTime < timeAgo {
			log.Printf("删除一年前的数据库归档文件`%s`!!\n", file.FileName)
			_ = u.ufr.DeleteFile(file.FileName)
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

//goland:noinspection GoUnhandledErrorResult
func main() {
	// 申请一个临时文件夹
	tmpPath := getTempPath()
	defer os.RemoveAll(tmpPath)
	ufs := newUfs()
	if len(os.Args) == 1 {
		log.Println("该命令需要带参数使用，参数为需要备份的`mongodb`数据库名，支持多库备份")
		log.Println("BucketName is:", ufs.config.BucketName)
	}
	for idx, args := range os.Args {
		switch idx {
		case 0:
			continue
		default:
			fmt.Println("开始备份第`"+strconv.Itoa(idx)+"`个数据库:", args)
			// 打包数据库到临时文件夹
			archPath, err := archiveDb(args, tmpPath)
			if err == nil {
				// 上传数据库归档文件到云端
				_ = ufs.uploadFile(args, archPath)
			}
		}
	}

}
