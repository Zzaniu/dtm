/*
Author ：zaniu(zzaniu@126.com)
Time   ：2022/3/31 10:04
Desc   :

    ......................我佛慈悲......................

                           _oo0oo_
                          o8888888o
                          88" . "88
                          (| -_- |)
                          0\  =  /0
                        ___/`---'\___
                      .' \\|     |// '.
                     / \\|||  :  |||// \
                    / _||||| -卍-|||||- \
                   |   | \\\  -  /// |   |
                   | \_|  ''\---/''  |_/ |
                   \  .-\__  '-'  ___/-. /
                 ___'. .'  /--.--\  `. .'___
              ."" '<  `.___\_<|>_/___.' >' "".
             | | :  `- \`.;`\ _ /`;.`/ - ` : | |
             \  \ `_.   \_ __\ /__ _/   .-` /  /
         =====`-.____`.___ \_____/___.-`___.-'=====
                           `=---='

    ..................佛祖保佑, 永无BUG...................

*/

package zrpcdriver

import (
	"context"
	"fmt"
	"github.com/Zzaniu/zrpc/middleware/register"
	retcd "github.com/Zzaniu/zrpc/middleware/register/etcd"
	"github.com/Zzaniu/zrpc/middleware/resolver/etcd"
	"github.com/dtm-labs/dtmdriver"
	"github.com/go-basic/uuid"
	"google.golang.org/grpc/resolver"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DriverName = "dtm-driver-zrpc"
	SchemaName = "discovery"
)

var builders sync.Map

type (
	zrpcDriver struct{}
)

func mustNewRegisterEtcd(endpoint string) *retcd.RegisterEtcd {
	discovery, err := retcd.NewRegisterEtcd(
		retcd.WithTTL(5),
		retcd.WithRegisterServiceUri(endpoint),
		retcd.WithCancelCtx(context.WithCancel(context.Background())),
	)
	if err != nil {
		panic(err)
	}
	return discovery
}

func mustNewBuilder(endpoint string) resolver.Builder {
	return etcd.NewResolverBuilderEtcd(mustNewRegisterEtcd(endpoint))
}

// Build 返回一个 resolver.Resolver
func (*zrpcDriver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	endpoint := target.URL.Host
	builder, ok := builders.Load(endpoint)
	if !ok {
		builder = mustNewBuilder(endpoint)
		builders.Store(endpoint, builder)
	}
	return builder.(resolver.Builder).Build(target, cc, opts)
}

func (*zrpcDriver) Scheme() string {
	return SchemaName
}

// GetName dtm 获取驱动名, sync.map 那边加载驱动的时候需要用到
func (k *zrpcDriver) GetName() string {
	return DriverName
}

// RegisterGrpcResolver 向 grpc 注册 resolver
func (k *zrpcDriver) RegisterGrpcResolver() {
	resolver.Register(&zrpcDriver{})
}

// RegisterGrpcService 怎么注册 dtm 这个服务
func (k *zrpcDriver) RegisterGrpcService(target string, endpoint string) error {
	if target == "" {
		return nil
	}

	u, err := url.Parse(target)
	if err != nil {
		return err
	}

	// u, err := url.Parse 之后
	// u.Scheme =  discovery
	// u.Host =  172.18.2.249:20000,172.18.2.249:20002,172.18.2.249:20004
	// u.Path = /Dev/inventory.rpc/Inventory/Reduce
	path := strings.TrimPrefix(u.Path, "/")

	registry := mustNewRegisterEtcd(u.Host)

	registerInstance := &register.ServiceInstance{
		Name:     path,
		Key:      fmt.Sprintf("%s/%d/%s", path, time.Now().Unix(), uuid.New()),
		Endpoint: endpoint,
	}

	switch u.Scheme {
	case SchemaName:
		return registry.Register(context.Background(), registerInstance)
	default:
		return fmt.Errorf("unknown scheme: %s", u.Scheme)
	}
}

// ParseServerMethod 解析服务名和方法名
func (k *zrpcDriver) ParseServerMethod(uri string) (server string, method string, err error) {
	if !strings.Contains(uri, "//") {
		return "", "", nil
	}
	u, err := url.Parse(uri)

	if err != nil {
		return "", "", nil
	}
	n1 := strings.IndexByte(u.Path[1:], '/') + 1
	n2 := strings.IndexByte(u.Path[n1+1:], '/') + 1
	index := n1 + n2
	return u.Scheme + "://" + u.Host + u.Path[:index], u.Path[index:], nil
}

// 这里相当于注册这个驱动
func init() {
	dtmdriver.Register(&zrpcDriver{})
}
