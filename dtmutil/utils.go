/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"

	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
)

// GetGinApp init and return gin
func GetGinApp() *gin.Engine {
	// 设置为 release 模式, 默认为 debug 模式
	gin.SetMode(gin.ReleaseMode)
	app := gin.New()
	// 中间件捕获 panic
	app.Use(gin.Recovery())
	// 中间件 打印 body
	app.Use(func(c *gin.Context) {
		body := ""
		if c.Request.Body != nil {
			// 获取 body 数据 (注意，这个是只能读取一次的，所以需要下面的操作)
			rb, err := c.GetRawData()
			dtmimp.E2P(err)
			if len(rb) > 0 {
				body = string(rb)
				// 将 body 信息重新赋给 c.Request.Body
				c.Request.Body = ioutil.NopCloser(bytes.NewBuffer(rb))
			}
		}
		logger.Debugf("begin %s %s body: %s", c.Request.Method, c.Request.URL, body)
		c.Next()
	})
	// 注册一个支持任何 method 的 ping 接口
	app.Any("/api/ping", func(c *gin.Context) { c.JSON(200, map[string]interface{}{"msg": "pong"}) })
	return app
}

// WrapHandler2 wrap a function te bo the handler of gin request
// 完全可以自己写一个类似的包装函数去处理，比如那个result其实可以去掉
func WrapHandler2(fn func(*gin.Context) interface{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		began := time.Now()
		var err error
		// 返回视图函数执行结果
		r := func() interface{} {
			defer dtmimp.P2E(&err)
			return fn(c)
		}()

		// 如果没有报错的话，那就是返回 http.StatusOK
		status := http.StatusOK

		// in dtm test/busi, there are some functions, which will return a resty response
		// pass resty response as gin's response
		// 这里可以去掉，个人感觉是没啥必要
		if resp, ok := r.(*resty.Response); ok {
			// 获取返回的结果
			b := resp.Body()
			// 获取返回的状态码
			status = resp.StatusCode()
			r = nil
			err = json.Unmarshal(b, &r)
		}

		// error maybe returned in r, assign it to err
		// 如果返回的是错误的话, 将错误赋给 err
		if ne, ok := r.(error); ok && err == nil {
			err = ne
		}

		// if err != nil || r == nil. then set the status and dtm_result
		// dtm_result is for compatible with version lower than v1.10
		// when >= v1.10, result test should base on status, not dtm_result.
		result := map[string]interface{}{}
		if err != nil {
			// 如果返回错误是 dtmcli.ErrFailure的话，直接返回 http.StatusConflict 进行回滚
			if errors.Is(err, dtmcli.ErrFailure) {
				status = http.StatusConflict
				// 这里应该是兼容以前的吧
				result["dtm_result"] = dtmcli.ResultFailure
			} else if errors.Is(err, dtmcli.ErrOngoing) {
				// 如果返回的错误是 dtmcli.ErrOngoing，直接返回 http.StatusTooEarly 稍后重试
				status = http.StatusTooEarly
				result["dtm_result"] = dtmcli.ResultOngoing
			} else if err != nil {
				// 否则的话，返回 http.StatusInternalServerError 进行重试
				// 自己写的时候，应该是可以把大于等于 http.StatusInternalServerError 的直接回滚
				status = http.StatusInternalServerError
			}
			result["message"] = err.Error()
			r = result
		} else if r == nil {
			result["dtm_result"] = dtmcli.ResultSuccess
			r = result
		}

		// 这些打印日志完全可以去掉
		b, _ := json.Marshal(r)
		cont := string(b)
		if status == http.StatusOK || status == http.StatusTooEarly {
			logger.Infof("%2dms %d %s %s %s", time.Since(began).Milliseconds(), status, c.Request.Method, c.Request.RequestURI, cont)
		} else {
			fmt.Println("time.Since(began).Milliseconds() = ", time.Since(began).Milliseconds())
			fmt.Println("status = ", status)
			fmt.Println("c.Request.Method = ", c.Request.Method)
			fmt.Println("c.Request.RequestURI = ", c.Request.RequestURI)
			fmt.Println("cont = ", cont)
			logger.Errorf("%2dms %d %s %s %s", time.Since(began).Milliseconds(), status, c.Request.Method, c.Request.RequestURI, cont)
		}
		// 返回，主要是看状态码
		c.JSON(status, r)
	}
}

// MustGetwd must version of os.Getwd
func MustGetwd() string {
	wd, err := os.Getwd()
	dtmimp.E2P(err)
	return wd
}

// GetSQLDir 获取调用该函数的caller源代码的目录，主要用于测试时，查找相关文件
func GetSQLDir() string {
	wd := MustGetwd()
	if filepath.Base(wd) == "test" {
		wd = filepath.Dir(wd)
	}
	return wd + "/sqls"
}

// RecoverPanic execs recovery operation
func RecoverPanic(err *error) {
	if x := recover(); x != nil {
		e := dtmimp.AsError(x)
		if err != nil {
			*err = e
		}
	}
}

// GetNextTime gets next time from second
func GetNextTime(second int64) *time.Time {
	next := time.Now().Add(time.Duration(second) * time.Second)
	return &next
}

// RunSQLScript 1
func RunSQLScript(conf dtmcli.DBConf, script string, skipDrop bool) {
	con, err := dtmimp.StandaloneDB(conf)
	logger.FatalIfError(err)
	defer func() { _ = con.Close() }()
	content, err := ioutil.ReadFile(script)
	logger.FatalIfError(err)
	sqls := strings.Split(string(content), ";")
	for _, sql := range sqls {
		s := strings.TrimSpace(sql)
		if s == "" || (skipDrop && strings.Contains(s, "drop")) {
			continue
		}
		_, err = dtmimp.DBExec(con, s)
		logger.FatalIfError(err)
		logger.Infof("sql scripts finished: %s", s)
	}
}
