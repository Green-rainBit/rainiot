package nacos

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"rainiot/pkg/openconfig"
	"strconv"
	"syscall"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/rest"
)

func InitNacosConfig(config openconfig.NacosConfig, onChange func(namespace, group, dataId, data string)) error {
	if config.Model == "local" || len(config.IpAddress) == 0 {
		return nil
	}
	nacosConfigs := []constant.ServerConfig{}
	for _, ipAddress := range config.IpAddress {
		nacosConfigs = append(nacosConfigs, *constant.NewServerConfig(ipAddress, config.Port))
	}
	configClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig: constant.NewClientConfig(
				constant.WithNamespaceId(config.NamespaceId), //当namespace是public时，此处填空字符串。
				constant.WithTimeoutMs(5000),
				constant.WithNotLoadCacheAtStart(true),
				constant.WithLogDir("/tmp/nacos/log"),
				constant.WithCacheDir("/tmp/nacos/cache"),
				constant.WithLogLevel("debug"),
				constant.WithUsername(config.Username),
				constant.WithPassword(config.Password),
				constant.WithAccessKey(config.AccessKey),
				constant.WithSecretKey(config.SecretKey),
				constant.WithRegionId(config.RegionId),
			),
			ServerConfigs: nacosConfigs,
		},
	)
	if err != nil {
		return err
	}
	err = configClient.ListenConfig(vo.ConfigParam{
		DataId:   config.DataId,
		Group:    config.Group,
		OnChange: onChange,
	})
	if err != nil {
		return err
	}

	return nil
}

func InitNacosRegisterInstance(config openconfig.NacosConfig, c rest.RestConf) error {
	if config.Model == "local" || len(config.IpAddress) == 0 {
		return nil
	}
	nacosConfigs := []constant.ServerConfig{}
	for _, ipAddress := range config.IpAddress {
		nacosConfigs = append(nacosConfigs, *constant.NewServerConfig(ipAddress, config.Port))
	}
	namingClient, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig: constant.NewClientConfig(
				constant.WithNamespaceId(config.NamespaceId), //当namespace是public时，此处填空字符串。
				constant.WithTimeoutMs(5000),
				constant.WithNotLoadCacheAtStart(true),
				constant.WithLogDir("/tmp/nacos/log"),
				constant.WithCacheDir("/tmp/nacos/cache"),
				constant.WithLogLevel("debug"),
				constant.WithUsername(config.Username),
				constant.WithPassword(config.Password),
				constant.WithAccessKey(config.AccessKey),
				constant.WithSecretKey(config.SecretKey),
				constant.WithRegionId(config.RegionId),
			),
			ServerConfigs: nacosConfigs,
		},
	)
	if err != nil {
		return err
	}
	serviceName, ip, portStr := getRegistryParameters(c)
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid SERVICE_PORT: %w", err)
	}
	_, err = namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: config.DataId,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"idc": "shanghai"},
	})
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	go handleShutdown(namingClient, serviceName, ip, port)
	return nil
}

func getRegistryParameters(c rest.RestConf) (serviceName, ip, portStr string) {
	serviceName = os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = c.Name
	}

	ip = os.Getenv("SERVICE_IP")
	if ip == "" {
		ip = getLocalIP()
		if ip == "" {
			ip = "127.0.0.1"
			log.Println("[WARN] Failed to detect local IP, fallback to 127.0.0.1")
		} else {
			log.Printf("[INFO] Auto-detected local IP: %s", ip)
		}
	} else {
		log.Printf("[INFO] Using SERVICE_IP from env: %s", ip)
	}

	portStr = os.Getenv("SERVICE_PORT")
	if portStr == "" {
		portStr = strconv.Itoa(c.Port)
	}
	return
}

func getLocalIP() string {
	// 方法1：通过一个外部地址获取本机出口 IP
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		ip := localAddr.IP.String()
		if ip != "" && ip != "::1" && !net.IP.IsLoopback(localAddr.IP) {
			return ip
		}
	}

	// 方法2：遍历网卡
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			// 过滤 docker 等虚拟网卡可选择增加判断，这里简化，取第一个有效 IPv4
			return ipnet.IP.String()
		}
	}
	return ""
}

func handleShutdown(namingClient naming_client.INamingClient, serviceName, ip string, port uint64) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	log.Println("\n[INFO] Shutdown signal received, deregistering...")
	_, err := namingClient.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: serviceName,
		Ephemeral:   true,
	})
	if err != nil {
		log.Printf("[ERROR] Deregister failed: %v\n", err)
	} else {
		log.Println("[INFO] Deregistered successfully.")
	}
	os.Exit(0)
}
