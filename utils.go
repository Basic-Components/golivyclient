package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

//HTTPJSONQuery 构造http请求
func HTTPJSONQuery(URL string, Method string, jsonData ...interface{}) ([]byte, error) {
	switch len(jsonData) {
	case 0:
		{
			req, err := http.NewRequest(Method, URL, nil)
			if err != nil {
				return nil, err
			}

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			res, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode == 404 {
				return nil, fmt.Errorf("未找到资源,url:%s", URL)
			}
			if resp.StatusCode >= 300 {
				return nil, fmt.Errorf("请求失败,code:%d;msg:%s", resp.StatusCode, res)
			}

			return res, nil
		}
	case 1:
		{
			kvalue, err := json.Marshal(jsonData[0])
			if err != nil {
				return nil, err
			}
			req, err := http.NewRequest(Method, URL, bytes.NewBuffer(kvalue))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json;charset=utf-8")
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}

			defer resp.Body.Close()
			res, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode == 404 {
				return nil, fmt.Errorf("未找到资源,url:%s", URL)
			}
			if resp.StatusCode >= 300 {
				return nil, fmt.Errorf("请求失败,code:%d;msg:%s", resp.StatusCode, res)
			}
			return res, nil
		}
	default:
		{
			return nil, errors.New("请求的jsonData参数过多")
		}
	}
}

// MD5 md5字符串
func MD5(data []byte) string {
	h := md5.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

//PutEvent 更新事件
type PutEvent struct {
	message string
}

//Message 获取PutEvent的message
func (s *PutEvent) Message() string {
	return s.message
}

//DeleteEvent 停止事件
type DeleteEvent struct {
}

//ErrorEvent 出错事件
type ErrorEvent struct {
	message string
}

//Message 获取ErrorEvent的message
func (s *ErrorEvent) Message() string {
	return s.message
}

//Event 可以用于发出的消息接口
type Event interface {
	Message() string
}
